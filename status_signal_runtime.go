package workersext

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/BananaLabs-OSS/Pulp/ext"
)

// statusSignalRuntime is an internal bounded worker/HTTP runtime. It is owned
// by effect.status.signal lifecycle setup and has no guest-visible generic
// workers imports. Each application instance gets an isolated pool and
// storage root; package bytes remain shared.
type statusSignalRuntime struct {
	pool        *workerPool
	storageRoot string
	refs        int
}

var statusSignalRuntimes = struct {
	mu       sync.Mutex
	runtimes map[workerApplicationKey]*statusSignalRuntime
}{runtimes: make(map[workerApplicationKey]*statusSignalRuntime)}

func statusSignalSetup(env ext.SetupEnv) error {
	scope := env.EffectiveScope()
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("workers status signals: setup scope: %w", err)
	}
	if strings.TrimSpace(env.StorageRoot) == "" {
		return errors.New("workers status signals: application storage root is required")
	}
	logger := env.Logger
	if logger == nil {
		logger = slog.Default()
	}
	key := applicationKey(scope)
	statusSignalRuntimes.mu.Lock()
	defer statusSignalRuntimes.mu.Unlock()
	if runtime := statusSignalRuntimes.runtimes[key]; runtime != nil {
		if runtime.storageRoot != env.StorageRoot {
			return errors.New("workers status signals: application storage root changed while runtime is active")
		}
		runtime.refs++
		return nil
	}
	maxConcurrency := readPositiveIntEnv("PULP_STATUS_SIGNAL_MAX_CONCURRENCY", defaultMaxConcurrency)
	maxQueued := readPositiveIntEnv("PULP_STATUS_SIGNAL_MAX_QUEUED", defaultMaxQueued)
	maxPerCell := readPositiveIntEnv("PULP_STATUS_SIGNAL_MAX_PER_CELL", defaultMaxPerCell)
	maxFetchBytes := readPositiveInt64Env("PULP_STATUS_SIGNAL_MAX_FETCH_BYTES", 1024*1024)
	statusSignalRuntimes.runtimes[key] = &statusSignalRuntime{
		pool:        newWorkerPool(logger, maxConcurrency, maxQueued, maxPerCell, maxFetchBytes),
		storageRoot: env.StorageRoot,
		refs:        1,
	}
	return nil
}

func statusSignalRuntimeForScope(scope ext.Scope) (*statusSignalRuntime, bool) {
	if err := scope.Validate(); err != nil {
		return nil, false
	}
	statusSignalRuntimes.mu.Lock()
	runtime := statusSignalRuntimes.runtimes[applicationKey(scope)]
	ok := runtime != nil && runtime.pool != nil && runtime.refs > 0
	statusSignalRuntimes.mu.Unlock()
	return runtime, ok
}

func statusSignalTeardown(scope ext.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("workers status signals: teardown scope: %w", err)
	}
	key := applicationKey(scope)
	statusSignalRuntimes.mu.Lock()
	runtime := statusSignalRuntimes.runtimes[key]
	if runtime == nil {
		statusSignalRuntimes.mu.Unlock()
		return nil
	}
	runtime.refs--
	if runtime.refs > 0 {
		statusSignalRuntimes.mu.Unlock()
		return nil
	}
	delete(statusSignalRuntimes.runtimes, key)
	statusSignalRuntimes.mu.Unlock()
	runtime.pool.teardown()
	return nil
}

func statusSignalRuntimeCount() int {
	statusSignalRuntimes.mu.Lock()
	defer statusSignalRuntimes.mu.Unlock()
	return len(statusSignalRuntimes.runtimes)
}
