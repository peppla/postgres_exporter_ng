package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeStateConfig puts a configuration file somewhere this test can load it
// from.
func writeStateConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "postgres_exporter.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing the fixture: %s", err)
	}
	return path
}

// loadState reads a configuration file and returns its state section.
func loadState(t *testing.T, body string) (State, error) {
	t.Helper()
	var h Handler
	err := h.ReloadConfig(writeStateConfig(t, body), nil)
	if err != nil {
		return State{}, err
	}
	return h.GetConfig().State, nil
}

// TestTheStateSectionIsReadAsWritten covers the three values an operator sets.
func TestTheStateSectionIsReadAsWritten(t *testing.T) {
	state, err := loadState(t, `
state:
  profiles: 7
  probe_connections: 11
  idle: 90m
`)
	if err != nil {
		t.Fatalf("loading: %s", err)
	}

	got := state.Slots()
	if got.Profiles != 7 {
		t.Errorf("profiles is %d, want 7", got.Profiles)
	}
	if got.Contexts != 11 {
		t.Errorf("probe_connections is %d, want 11", got.Contexts)
	}
	if got.Idle != 90*time.Minute {
		t.Errorf("idle is %s, want 90m", got.Idle)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("a complete section was rejected: %s", err)
	}
}

// TestNoStateSectionLeavesEverythingUnset is the behaviour this exporter has
// until the three values are chosen.
//
// Unset is not a default. There is nothing to fall back to, and zero is what
// the admission reads as no limit, so nothing is refused and nothing is
// released.
func TestNoStateSectionLeavesEverythingUnset(t *testing.T) {
	for _, body := range []string{
		"auth_modules: {}\n",
		"limits:\n  connections: 5\n",
	} {
		state, err := loadState(t, body)
		if err != nil {
			t.Fatalf("loading %q: %s", body, err)
		}
		got := state.Slots()
		if got.Profiles != 0 || got.Contexts != 0 || got.Idle != 0 {
			t.Errorf("a file with no state section gave %+v, want everything unset", got)
		}
		if err := got.Validate(); err != nil {
			t.Errorf("an unset section was rejected: %s", err)
		}
	}
}

// TestAZeroIsNotAnOmission is why the fields are pointers.
//
// Written as zero it means something the operator did not mean, so it is
// refused rather than read as no limit.
func TestAZeroIsNotAnOmission(t *testing.T) {
	state, err := loadState(t, "state:\n  profiles: 0\n  idle: 1h\n")
	if err != nil {
		t.Fatalf("loading: %s", err)
	}
	if state.Profiles == nil {
		t.Fatal("profiles: 0 was read as an omitted key, so a zero cannot be told from nothing")
	}
	if *state.Profiles != 0 {
		t.Errorf("profiles is %d, want the zero that was written", *state.Profiles)
	}

	// And knowing is what lets it be reported. Read as a limit a zero means
	// no limit, which is the opposite of what writing one looks like.
	if err := state.Validate(); err == nil {
		t.Error("profiles: 0 was accepted, and it reads as no limit at all")
	}
}

// TestAZeroIsReportedForEveryValue covers the other two.
func TestAZeroIsReportedForEveryValue(t *testing.T) {
	for _, body := range []string{
		"state:\n  profiles: 0\n  idle: 1h\n",
		"state:\n  probe_connections: 0\n  idle: 1h\n",
		"state:\n  profiles: 2\n  idle: 0s\n",
	} {
		state, err := loadState(t, body)
		if err != nil {
			t.Fatalf("loading %q: %s", body, err)
		}
		if err := state.Validate(); err == nil {
			t.Errorf("%q was accepted", body)
		}
	}
}

// TestAKeyWithNothingAfterItIsRefused covers the shape that looks like a
// setting and is not one.
func TestAKeyWithNothingAfterItIsRefused(t *testing.T) {
	for _, body := range []string{
		"state:\n",
		"state:\n  profiles:\n",
		"state:\n  probe_connections:\n",
		"state:\n  idle:\n",
	} {
		if _, err := loadState(t, body); err == nil {
			t.Errorf("%q was accepted, and it reads as a setting while being none", body)
		}
	}
}
