package versioncache

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
)

func clearVersionCache() {
	Clear()
}

func resetVersionHooks() {
	CurrentTime = time.Now
}

const testServer = "127.0.0.1:5432"

func TestGetVersionUsesCache(t *testing.T) {
	clearVersionCache()
	resetVersionHooks()
	t.Cleanup(func() {
		clearVersionCache()
		resetVersionHooks()
	})

	now := time.Unix(1_000, 0)
	CurrentTime = func() time.Time { return now }

	seed(testServer, versionInfo{
		semantic: semver.MustParse("14.22.0"),
		full:     "PostgreSQL 14.22",
		cachedAt: now,
	})

	version, versionString, err := GetVersion(context.Background(), (*sql.DB)(nil), testServer, func(_ context.Context, _ *sql.DB, _ string) (semver.Version, string, error) {
		t.Fatal("loadVersion should not be called for a fresh cache entry")
		return semver.Version{}, "", nil
	})
	require.NoError(t, err)
	require.Equal(t, semver.MustParse("14.22.0"), version)
	require.Equal(t, "PostgreSQL 14.22", versionString)
}

func TestGetVersionRefreshesExpiredCache(t *testing.T) {
	clearVersionCache()
	resetVersionHooks()
	t.Cleanup(func() {
		clearVersionCache()
		resetVersionHooks()
	})

	now := time.Unix(1_000, 0)
	CurrentTime = func() time.Time { return now }

	seed(testServer, versionInfo{
		semantic: semver.MustParse("14.22.0"),
		full:     "PostgreSQL 14.22",
		cachedAt: now.Add(-TTL - time.Second),
	})

	version, versionString, err := GetVersion(context.Background(), (*sql.DB)(nil), testServer, func(_ context.Context, _ *sql.DB, _ string) (semver.Version, string, error) {
		return semver.MustParse("15.0.0"), "PostgreSQL 15.0", nil
	})
	require.NoError(t, err)
	require.Equal(t, semver.MustParse("15.0.0"), version)
	require.Equal(t, "PostgreSQL 15.0", versionString)

	cached, ok := cached(testServer)
	require.True(t, ok)
	require.Equal(t, versionInfo{
		semantic: semver.MustParse("15.0.0"),
		full:     "PostgreSQL 15.0",
		cachedAt: now,
	}, cached)
}

func TestGetVersionLoadsOnceForConcurrentMisses(t *testing.T) {
	clearVersionCache()
	resetVersionHooks()
	t.Cleanup(func() {
		clearVersionCache()
		resetVersionHooks()
	})

	var calls atomic.Int32
	loadVersion := func(_ context.Context, _ *sql.DB, _ string) (semver.Version, string, error) {
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return semver.MustParse("15.0.0"), "PostgreSQL 15.0", nil
	}

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			version, versionString, err := GetVersion(context.Background(), (*sql.DB)(nil), testServer, loadVersion)
			require.NoError(t, err)
			require.Equal(t, semver.MustParse("15.0.0"), version)
			require.Equal(t, "PostgreSQL 15.0", versionString)
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), calls.Load())
}

// seed puts a version in the cache without a lookup.
func seed(server string, info versionInfo) {
	entryFor(server).store(info)
}

// cached reads an entry back, whether or not it is still fresh.
func cached(server string) (versionInfo, bool) {
	e := entryFor(server)
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.info, e.valid
}

// TestGetVersionStopsWaitingWhenTheContextExpires covers the wait that used
// to be unbounded.
//
// A single lock was held across the lookup, so a caller arriving while
// someone else was waiting on a slow server had no way to stop waiting. The
// scrape that queued there ran past its own deadline without having issued a
// query of its own.
func TestGetVersionStopsWaitingWhenTheContextExpires(t *testing.T) {
	Clear()
	defer Clear()

	const server = "slow:5432"
	holding := make(chan struct{})
	release := make(chan struct{})

	go func() {
		_, _, _ = GetVersion(context.Background(), nil, server,
			func(ctx context.Context, _ *sql.DB, _ string) (semver.Version, string, error) {
				close(holding)
				<-release
				return semver.MustParse("18.0.0"), "PostgreSQL 18.0", nil
			})
	}()
	<-holding
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err := GetVersion(ctx, nil, server,
		func(context.Context, *sql.DB, string) (semver.Version, string, error) {
			t.Error("the second caller ran a lookup of its own while one was in flight")
			return semver.Version{}, "", nil
		})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a caller whose context expired got a version anyway")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waiting returned %v, want a deadline error", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("the caller waited %s before giving up", elapsed)
	}
}

// TestGetVersionDoesNotSerialiseAcrossServers pins the other half.
//
// The version is a property of one server, so a slow lookup against one must
// not hold up a scrape of another. A single cache-wide lock made every server
// queue behind whichever was slowest.
func TestGetVersionDoesNotSerialiseAcrossServers(t *testing.T) {
	Clear()
	defer Clear()

	holding := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	go func() {
		_, _, _ = GetVersion(context.Background(), nil, "slow:5432",
			func(context.Context, *sql.DB, string) (semver.Version, string, error) {
				close(holding)
				<-release
				return semver.MustParse("18.0.0"), "PostgreSQL 18.0", nil
			})
	}()
	<-holding

	done := make(chan error, 1)
	go func() {
		_, _, err := GetVersion(context.Background(), nil, "other:5432",
			func(context.Context, *sql.DB, string) (semver.Version, string, error) {
				return semver.MustParse("17.0.0"), "PostgreSQL 17.0", nil
			})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the second server's lookup failed: %s", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a lookup for one server waited on a slow lookup for another")
	}
}
