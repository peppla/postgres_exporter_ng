// Copyright 2022 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gopkg.in/yaml.v3"
)

var (
	configReloadSuccess = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "postgres_exporter",
		Name:      "config_last_reload_successful",
		Help:      "Postgres exporter config loaded successfully.",
	})

	configReloadSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "postgres_exporter",
		Name:      "config_last_reload_success_timestamp_seconds",
		Help:      "Timestamp of the last successful configuration reload.",
	})
)

type Config struct {
	AuthModules map[string]AuthModule `yaml:"auth_modules"`
	// Limits is what one scrape is allowed to cost. See limits.go.
	Limits Limits `yaml:"limits"`
	// Monitoring configures built-in monitoring. See selection.go.
	Monitoring Monitoring `yaml:"monitoring"`
}

type AuthModule struct {
	Type     string   `yaml:"type"`
	UserPass UserPass `yaml:"userpass,omitempty"`
	// Add alternative auth modules here
	Options map[string]string `yaml:"options"`
}

type UserPass struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Handler struct {
	sync.RWMutex
	Config *Config
}

func (ch *Handler) GetConfig() *Config {
	ch.RLock()
	defer ch.RUnlock()
	return ch.Config
}

func (ch *Handler) ReloadConfig(f string, logger log.Logger) error {
	config := &Config{}
	var err error
	defer func() {
		if err != nil {
			configReloadSuccess.Set(0)
		} else {
			configReloadSuccess.Set(1)
			configReloadSeconds.SetToCurrentTime()
		}
	}()

	// Read the file once and parse it twice. The typed parse cannot see a key
	// written with no value, because yaml decodes a null into the same nil
	// that an absent key produces and does not call a custom unmarshaler for
	// one, so the keys where that matters are checked against the document
	// separately.
	content, err := os.ReadFile(f)
	if err != nil {
		return fmt.Errorf("error opening config file %q: %s", f, err)
	}

	if err = rejectNullKeys(content, limitKeyPaths()...); err != nil {
		return fmt.Errorf("error parsing config file %q: %s", f, err)
	}
	// A separate pass for the selection lists, which need their own message:
	// an empty list and an absent key mean opposite things there, and "give
	// it a value" does not say which one to write.
	if err = rejectNullSelectionLists(content); err != nil {
		return fmt.Errorf("error parsing config file %q: %s", f, err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)

	if err = decoder.Decode(config); err != nil {
		return fmt.Errorf("error parsing config file %q: %s", f, err)
	}

	if err = config.Limits.Budget().Validate(); err != nil {
		return fmt.Errorf("error parsing config file %q: limits: %s", f, err)
	}

	ch.Lock()
	ch.Config = config
	ch.Unlock()
	return nil
}

func (m AuthModule) ConfigureTarget(target string) (DSN, error) {
	dsn, err := dsnFromString(target)
	if err != nil {
		return DSN{}, err
	}

	// Set the credentials from the authentication module
	// TODO(@sysadmind): What should the order of precedence be?
	if m.Type == "userpass" {
		if m.UserPass.Username != "" {
			dsn.username = m.UserPass.Username
		}
		if m.UserPass.Password != "" {
			dsn.password = m.UserPass.Password
		}
	}

	for k, v := range m.Options {
		dsn.query.Set(k, v)
	}

	return dsn, nil
}
