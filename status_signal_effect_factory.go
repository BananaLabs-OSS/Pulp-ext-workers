package workersext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/abi"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/vmihailenco/msgpack/v5"
)

const statusSignalEffectResourceType = "workers-status-signal-effect"

// StatusSignalScopeConfig is host-owned configuration for one application
// instance's status ingest transport. The guest supplies only the bounded
// signal payload; it cannot select this endpoint, credential, or headers.
type StatusSignalScopeConfig struct {
	Endpoint    string
	BearerToken string
}

func (c StatusSignalScopeConfig) Validate() error {
	endpoint, err := url.Parse(c.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return errors.New("workers status signals: endpoint must be an absolute credential-free URL")
	}
	if endpoint.Scheme != "https" && endpoint.Scheme != "http" {
		return errors.New("workers status signals: endpoint scheme must be http or https")
	}
	if strings.TrimSpace(c.BearerToken) != c.BearerToken || c.BearerToken == "" {
		return errors.New("workers status signals: bearer token is required")
	}
	if len(c.BearerToken) > 4096 {
		return errors.New("workers status signals: bearer token exceeds 4096 bytes")
	}
	for _, r := range c.BearerToken {
		if r < 0x20 || r == 0x7f {
			return errors.New("workers status signals: bearer token contains a control character")
		}
	}
	return nil
}

// StatusSignalScopeConfigSource resolves a host-supplied config by the caller
// scope. It is intentionally a host-only dependency, never a WASM import.
type StatusSignalScopeConfigSource func(ext.Scope) (StatusSignalScopeConfig, error)

// ScopedStatusSignalEffectExecutorFactory holds one durable status executor
// per cell scope. Every executor resolves credentials from its application's
// host scope, so equal packages in different applications never share a token
// or receipt namespace.
type ScopedStatusSignalEffectExecutorFactory struct {
	mu           sync.Mutex
	config       StatusSignalScopeConfigSource
	storeFactory EffectStoreFactory
	executors    map[ext.ResourceKey]*EffectExecutor
}

// SetStoreFactory changes only subsequently constructed executors. Deployment
// calls this during initialization, before any application can submit work.
func (f *ScopedStatusSignalEffectExecutorFactory) SetStoreFactory(storeFactory EffectStoreFactory) error {
	if f == nil || storeFactory == nil {
		return errors.New("workers status signals: store factory is required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.executors) != 0 {
		return errors.New("workers status signals: store factory cannot change after use")
	}
	f.storeFactory = storeFactory
	return nil
}

// ConfigureStatusSignalEffectStore installs the production receipt backend.
// Local hosts that do not call it retain the file-backed store.
func ConfigureStatusSignalEffectStore(storeFactory EffectStoreFactory) error {
	return hostStatusSignalExecutors.SetStoreFactory(storeFactory)
}

func NewScopedStatusSignalEffectExecutorFactory(config StatusSignalScopeConfigSource) (*ScopedStatusSignalEffectExecutorFactory, error) {
	if config == nil {
		return nil, errors.New("workers status signals: config source is required")
	}
	return &ScopedStatusSignalEffectExecutorFactory{config: config, executors: make(map[ext.ResourceKey]*EffectExecutor)}, nil
}

func (f *ScopedStatusSignalEffectExecutorFactory) ForScope(scope ext.Scope) (*EffectExecutor, error) {
	if f == nil || f.config == nil {
		return nil, errors.New("workers status signals: factory is not configured")
	}
	key, err := scope.ResourceKey(statusSignalEffectResourceType, "executor")
	if err != nil {
		return nil, fmt.Errorf("workers status signals: scope: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if executor := f.executors[key]; executor != nil {
		return executor, nil
	}
	runtime, ok := statusSignalRuntimeForScope(scope)
	if !ok {
		return nil, errors.New("workers status signals: scoped runtime is unavailable")
	}
	config, err := f.config(scope)
	if err != nil {
		return nil, fmt.Errorf("workers status signals: host scope config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	var store EffectStore
	if f.storeFactory != nil {
		store, err = f.storeFactory(scope, statusSignalEffectResourceType)
	} else {
		store, err = newFileEffectStore(runtime.storageRoot, scope, statusSignalEffectResourceType)
	}
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("workers status signals: effect store is nil")
	}
	handler := EffectHandler(func(ctx context.Context, intent effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		return publishStatusSignalHTTP(ctx, runtime.pool, config, intent)
	})
	executor, err := newValidatedEffectExecutor(store, pooledEffectWorker{pool: runtime.pool}, handler, normalizeAndValidateStatusSignalEffect)
	if err != nil {
		return nil, fmt.Errorf("workers status signals: executor: %w", err)
	}
	f.executors[key] = executor
	return executor, nil
}

func (f *ScopedStatusSignalEffectExecutorFactory) TeardownScope(scope ext.Scope) error {
	if f == nil {
		return nil
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for key := range f.executors {
		if sameApplication(key.Scope(), scope) {
			delete(f.executors, key)
		}
	}
	return nil
}

func (f *ScopedStatusSignalEffectExecutorFactory) Count() int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.executors)
}

func normalizeAndValidateStatusSignalEffect(scope ext.Scope, intent *effect.Intent) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: scope: %v", ErrEffectInvalid, err)
	}
	if intent == nil {
		return fmt.Errorf("%w: intent is required", ErrEffectInvalid)
	}
	if err := intent.Validate(); err != nil {
		return fmt.Errorf("%w: intent: %v", ErrEffectInvalid, err)
	}
	if intent.Kind != effect.KindStatusSignalPublish {
		return fmt.Errorf("%w: unsupported status signal effect kind %q", ErrEffectInvalid, intent.Kind)
	}
	return nil
}

type statusSignalIngestRequest struct {
	Target    effect.StatusSignalTarget `json:"target"`
	Signal    effect.StatusSignalState  `json:"signal"`
	Detail    string                    `json:"detail"`
	ExpiresAt string                    `json:"expires_at"`
}

// publishStatusSignalHTTP uses effect.status.signal's internal scoped HTTP
// transport. It deliberately constructs the complete request from trusted
// config plus the typed payload instead of accepting arbitrary guest fields.
func publishStatusSignalHTTP(ctx context.Context, pool *workerPool, config StatusSignalScopeConfig, intent effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
	payload, err := effect.DecodePayload[effect.StatusSignalPublishPayload](intent)
	if err != nil {
		return nil, effectFailure("invalid_status_signal_intent", "status signal intent is invalid"), nil
	}
	body, err := json.Marshal(statusSignalIngestRequest{
		Target: payload.Target, Signal: payload.Signal, Detail: payload.Detail,
		ExpiresAt: time.Unix(payload.ExpiresAtUnix, 0).UTC().Format(time.RFC3339),
	})
	if err != nil {
		return nil, effectFailure("status_signal_encoding_failed", "status signal could not be encoded"), nil
	}
	if pool == nil {
		return nil, effectFailure("host_unavailable", "status signal delivery is unavailable"), nil
	}
	wire, err := pool.doHTTPFetch(ctx, taskRequest{
		Type: "http.fetch", Method: "POST", URL: config.Endpoint,
		Headers: map[string]string{"Authorization": "Bearer " + config.BearerToken, "Content-Type": "application/json"},
		Body:    body,
	})
	if err != nil {
		return nil, effectFailure("status_signal_provider_unavailable", "status signal delivery is unavailable"), nil
	}
	response, err := abi.DecodeHTTPResponse(wire)
	if err != nil || response.Status < 200 || response.Status >= 300 {
		return nil, effectFailure("status_signal_provider_rejected", "status signal endpoint rejected delivery"), nil
	}
	result, err := msgpack.Marshal(effect.StatusSignalPublishResult{
		Target: payload.Target, Signal: payload.Signal, ExpiresAtUnix: payload.ExpiresAtUnix,
	})
	if err != nil {
		return nil, effectFailure("status_signal_result_encoding_failed", "status signal result could not be encoded"), nil
	}
	return result, nil, nil
}

func effectFailure(code, message string) *effect.Failure {
	return &effect.Failure{Code: code, Message: message}
}

// statusSignalHostConfigs stores only host configuration keyed at application
// scope. It intentionally exposes no getter, so endpoint tokens do not cross
// into guest APIs or diagnostics.
var statusSignalHostConfigs = struct {
	mu      sync.RWMutex
	configs map[ext.Scope]StatusSignalScopeConfig
}{configs: make(map[ext.Scope]StatusSignalScopeConfig)}

// ConfigureStatusSignalScope installs status transport config for exactly one
// application host scope. Deployment wiring owns this call; cells never do.
func ConfigureStatusSignalScope(hostScope ext.Scope, config StatusSignalScopeConfig) error {
	if err := hostScope.Validate(); err != nil {
		return err
	}
	if hostScope.CellID() != "host" || hostScope.CellInstanceID() != "primary" {
		return errors.New("workers status signals: config must be owned by host/primary scope")
	}
	if err := config.Validate(); err != nil {
		return err
	}
	statusSignalHostConfigs.mu.Lock()
	statusSignalHostConfigs.configs[hostScope] = config
	statusSignalHostConfigs.mu.Unlock()
	return nil
}

func configuredStatusSignalScope(scope ext.Scope) (StatusSignalScopeConfig, error) {
	hostScope, err := ext.NewScope(scope.ApplicationID(), scope.ApplicationInstanceID(), "host", "primary")
	if err != nil {
		return StatusSignalScopeConfig{}, err
	}
	statusSignalHostConfigs.mu.RLock()
	config, ok := statusSignalHostConfigs.configs[hostScope]
	statusSignalHostConfigs.mu.RUnlock()
	if !ok {
		return StatusSignalScopeConfig{}, errors.New("status signal host config is unavailable")
	}
	return config, nil
}

var hostStatusSignalExecutors, _ = NewScopedStatusSignalEffectExecutorFactory(configuredStatusSignalScope)
