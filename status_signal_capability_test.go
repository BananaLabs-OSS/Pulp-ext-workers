package workersext

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

type statusSignalTestCell struct{ scope ext.Scope }

func (c statusSignalTestCell) Name() string     { return c.scope.CellID() }
func (c statusSignalTestCell) Scope() ext.Scope { return c.scope }

type statusSignalRoundTripper struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	status   int
}

func (r *statusSignalRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(request.Body)
	request.Body.Close()
	r.mu.Lock()
	r.requests = append(r.requests, request.Clone(request.Context()))
	r.bodies = append(r.bodies, body)
	r.mu.Unlock()
	return &http.Response{StatusCode: r.status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil)), Request: request}, nil
}

func (r *statusSignalRoundTripper) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

func statusSignalTestIntent(t *testing.T, id, key string) effect.Intent {
	t.Helper()
	intent, err := effect.NewIntent(id, effect.KindStatusSignalPublish, key, effect.StatusSignalPublishPayload{
		Target: effect.StatusSignalTargetPayments, Signal: effect.StatusSignalOK, Detail: "Stripe reachable", ExpiresAtUnix: 1_800_000_000,
	})
	if err != nil {
		t.Fatalf("new status intent: %v", err)
	}
	return intent
}

func statusSignalConfig(_ ext.Scope) (StatusSignalScopeConfig, error) {
	return StatusSignalScopeConfig{Endpoint: "https://status.example.test/api/v1/ingest", BearerToken: "host-only-token"}, nil
}

func waitStatusSignalReceipt(t *testing.T, executor *EffectExecutor, scope ext.Scope, key string) EffectReceipt {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		receipt, err := executor.Receipt(context.Background(), scope, key)
		if err != nil {
			t.Fatalf("receipt: %v", err)
		}
		if receipt.Status != effect.Pending {
			return receipt
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("status receipt did not settle")
	return EffectReceipt{}
}

func resetStatusSignalRuntimesForTest(t *testing.T) {
	t.Helper()
	statusSignalRuntimes.mu.Lock()
	runtimes := statusSignalRuntimes.runtimes
	statusSignalRuntimes.runtimes = make(map[workerApplicationKey]*statusSignalRuntime)
	statusSignalRuntimes.mu.Unlock()
	for _, runtime := range runtimes {
		if runtime != nil && runtime.pool != nil {
			runtime.pool.teardown()
		}
	}
	t.Cleanup(func() {
		statusSignalRuntimes.mu.Lock()
		runtimes := statusSignalRuntimes.runtimes
		statusSignalRuntimes.runtimes = make(map[workerApplicationKey]*statusSignalRuntime)
		statusSignalRuntimes.mu.Unlock()
		for _, runtime := range runtimes {
			if runtime != nil && runtime.pool != nil {
				runtime.pool.teardown()
			}
		}
	})
}

func TestStatusSignalCapabilityBindsActiveAndFailClosedStubABI(t *testing.T) {
	ctx := context.Background()
	runtime := wazero.NewRuntime(ctx)
	defer runtime.Close(ctx)
	scope := scopedTestScope(t, "sessions", "prod-a", "control", "primary")
	capability := newStatusSignalCapability(nil)
	active := runtime.NewHostModuleBuilder("status_signal_active")
	if err := capability.Register(active, statusSignalTestCell{scope: scope}); err != nil {
		t.Fatalf("register active: %v", err)
	}
	module, err := active.Instantiate(ctx)
	if err != nil {
		t.Fatalf("instantiate active: %v", err)
	}
	definition, ok := module.ExportedFunctionDefinitions()[statusSignalPublishExport]
	if !ok {
		t.Fatalf("active capability did not export %q", statusSignalPublishExport)
	}
	want := []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32}
	if !reflect.DeepEqual(definition.ParamTypes(), want) || !reflect.DeepEqual(definition.ResultTypes(), []api.ValueType{api.ValueTypeI32}) {
		t.Fatalf("active ABI = %#v -> %#v", definition.ParamTypes(), definition.ResultTypes())
	}
	stub := runtime.NewHostModuleBuilder("status_signal_stub")
	if err := capability.Stub(stub, statusSignalTestCell{scope: scope}); err != nil {
		t.Fatalf("register stub: %v", err)
	}
	if _, err := stub.Instantiate(ctx); err != nil {
		t.Fatalf("instantiate stub: %v", err)
	}
	if got := statusSignalPublishStub(ctx, nil, 0, 0, 0, 0); got != codeCapAbsent {
		t.Fatalf("stub code = %d, want %d", got, codeCapAbsent)
	}
}

func TestStatusSignalExecutorUsesScopedWorkerHTTPAndReplaysAcrossRestart(t *testing.T) {
	resetSharedWorkerPoolForTest(t)
	resetStatusSignalRuntimesForTest(t)
	t.Setenv("HTTP_FETCH_ALLOW", "127.0.0.0/8,::1/128")

	var received struct {
		sync.Mutex
		count         int
		authorization string
		contentType   string
		body          []byte
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		received.Lock()
		received.count++
		received.authorization = request.Header.Get("Authorization")
		received.contentType = request.Header.Get("Content-Type")
		received.body = append([]byte(nil), body...)
		received.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	storageRoot := t.TempDir()
	application := scopedTestScope(t, "sessions", "blue", "host", "primary")
	cell := scopedTestScope(t, "sessions", "blue", "control", "primary")
	factory, err := NewScopedStatusSignalEffectExecutorFactory(func(ext.Scope) (StatusSignalScopeConfig, error) {
		return StatusSignalScopeConfig{Endpoint: server.URL, BearerToken: "host-only-token"}, nil
	})
	if err != nil {
		t.Fatalf("new status factory: %v", err)
	}
	capability := newStatusSignalCapability(factory)
	if err := capability.Setup(ext.SetupEnv{Scope: application, StorageRoot: storageRoot, Logger: slog.Default()}); err != nil {
		t.Fatalf("status capability setup: %v", err)
	}
	if sharedWorkerPool() != nil {
		t.Fatal("status capability setup initialized the broad workers runtime")
	}
	runtime, ok := statusSignalRuntimeForScope(cell)
	if !ok {
		t.Fatal("status capability setup did not create its internal runtime")
	}
	executor, err := factory.ForScope(cell)
	if err != nil {
		t.Fatalf("factory scope: %v", err)
	}
	intent := statusSignalTestIntent(t, "status-1", "status-1")
	request, err := effect.MarshalIntent(intent)
	if err != nil {
		t.Fatalf("marshal intent: %v", err)
	}
	firstWire, code := executeStatusSignalWire(context.Background(), factory, cell, request)
	if code != statusSignalCodeOK {
		t.Fatalf("execute status signal code = %d", code)
	}
	pending, err := effect.UnmarshalReceipt(firstWire)
	if err != nil || pending.Status != effect.Pending || pending.ValidateFor(intent) != nil {
		t.Fatalf("first wire receipt = (%+v, %v)", pending, err)
	}
	settled := waitStatusSignalReceipt(t, executor, cell, intent.IdempotencyKey)
	if settled.Status != effect.Completed {
		t.Fatalf("settled receipt = %+v", settled)
	}
	received.Lock()
	requestCount := received.count
	authorization := received.authorization
	contentType := received.contentType
	body := append([]byte(nil), received.body...)
	received.Unlock()
	if requestCount != 1 {
		t.Fatalf("HTTP calls = %d, want 1", requestCount)
	}
	replayedWire, code := executeStatusSignalWire(context.Background(), factory, cell, request)
	if code != statusSignalCodeOK {
		t.Fatalf("replay code = %d", code)
	}
	replayedDirect, err := effect.UnmarshalReceipt(replayedWire)
	if err != nil || replayedDirect.Status != effect.Completed || replayedDirect.ValidateFor(intent) != nil {
		t.Fatalf("replayed direct receipt = (%+v, %v)", replayedDirect, err)
	}
	if authorization != "Bearer host-only-token" || contentType != "application/json" {
		t.Fatalf("host request headers authorization=%q content-type=%q", authorization, contentType)
	}
	if bytes.Contains(body, []byte("host-only-token")) || !bytes.Contains(body, []byte(`"target":"payments"`)) {
		t.Fatalf("status body = %q", body)
	}
	firstPool := runtime.pool
	if err := capability.TeardownScope(context.Background(), application); err != nil {
		t.Fatalf("capability teardown: %v", err)
	}
	if statusSignalRuntimeCount() != 0 || sharedWorkerPool() != nil {
		t.Fatal("status teardown leaked its runtime or initialized workers")
	}
	if err := capability.Setup(ext.SetupEnv{Scope: application, StorageRoot: storageRoot, Logger: slog.Default()}); err != nil {
		t.Fatalf("status capability restart: %v", err)
	}
	restartedRuntime, ok := statusSignalRuntimeForScope(cell)
	if !ok || restartedRuntime.pool == firstPool {
		t.Fatal("status capability restart reused its stopped pool")
	}
	resumed, err := factory.ForScope(cell)
	if err != nil {
		t.Fatalf("resume factory: %v", err)
	}
	replayed, err := resumed.Submit(context.Background(), cell, intent)
	if err != nil || replayed.Status != effect.Completed {
		t.Fatalf("replay = (%+v, %v)", replayed, err)
	}
	received.Lock()
	requestCount = received.count
	received.Unlock()
	if requestCount != 1 {
		t.Fatalf("restart replay performed HTTP again: %d", requestCount)
	}
}

func TestStatusSignalExecutorRejectsInvalidAndBlockedEgressWithoutNetwork(t *testing.T) {
	resetSharedWorkerPoolForTest(t)
	resetStatusSignalRuntimesForTest(t)
	t.Setenv("HTTP_FETCH_ALLOW", "")

	var requests int
	var requestsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestsMu.Lock()
		requests++
		requestsMu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	storageRoot := t.TempDir()
	application := scopedTestScope(t, "sessions", "blocked", "host", "primary")
	cell := scopedTestScope(t, "sessions", "blocked", "control", "primary")
	factory, err := NewScopedStatusSignalEffectExecutorFactory(func(ext.Scope) (StatusSignalScopeConfig, error) {
		return StatusSignalScopeConfig{Endpoint: server.URL, BearerToken: "host-only-token"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	capability := newStatusSignalCapability(factory)
	if err := capability.Setup(ext.SetupEnv{Scope: application, StorageRoot: storageRoot, Logger: slog.Default()}); err != nil {
		t.Fatal(err)
	}
	executor, err := factory.ForScope(cell)
	if err != nil {
		t.Fatal(err)
	}

	invalid := statusSignalTestIntent(t, "invalid-status", "invalid-status")
	invalid.Kind = effect.KindNotificationEmailSend
	if _, err := executor.Submit(context.Background(), cell, invalid); err == nil {
		t.Fatal("status executor accepted an invalid effect kind")
	}
	requestsMu.Lock()
	invalidRequests := requests
	requestsMu.Unlock()
	if invalidRequests != 0 {
		t.Fatalf("invalid intent performed %d network requests", invalidRequests)
	}

	intent := statusSignalTestIntent(t, "blocked-status", "blocked-status")
	pending, err := executor.Submit(context.Background(), cell, intent)
	if err != nil || pending.Status != effect.Pending {
		t.Fatalf("blocked submit = (%+v, %v)", pending, err)
	}
	settled := waitStatusSignalReceipt(t, executor, cell, intent.IdempotencyKey)
	if settled.Status != effect.Failed || settled.Failure == nil || settled.Failure.Code != "status_signal_provider_unavailable" {
		t.Fatalf("blocked receipt = %+v", settled)
	}
	requestsMu.Lock()
	blockedRequests := requests
	requestsMu.Unlock()
	if blockedRequests != 0 {
		t.Fatalf("SSRF-blocked endpoint received %d requests", blockedRequests)
	}
	if err := capability.TeardownScope(context.Background(), application); err != nil {
		t.Fatal(err)
	}
	if statusSignalRuntimeCount() != 0 || sharedWorkerPool() != nil {
		t.Fatal("blocked status teardown leaked its runtime or initialized broad workers")
	}
}

func TestStatusSignalExecutorIsNarrowAndScopeIsolated(t *testing.T) {
	resetSharedWorkerPoolForTest(t)
	resetStatusSignalRuntimesForTest(t)
	root := t.TempDir()
	appA := scopedTestScope(t, "sessions", "blue", "host", "primary")
	appB := scopedTestScope(t, "evolution", "blue", "host", "primary")
	cellA := scopedTestScope(t, "sessions", "blue", "control", "primary")
	cellB := scopedTestScope(t, "evolution", "blue", "control", "primary")
	for _, app := range []ext.Scope{appA, appB} {
		if err := statusSignalSetup(ext.SetupEnv{Scope: app, StorageRoot: root, Logger: slog.Default()}); err != nil {
			t.Fatalf("setup %s: %v", app.ApplicationID(), err)
		}
	}
	runtimeA, ok := statusSignalRuntimeForScope(cellA)
	if !ok {
		t.Fatal("application A status runtime unavailable")
	}
	runtimeB, ok := statusSignalRuntimeForScope(cellB)
	if !ok {
		t.Fatal("application B status runtime unavailable")
	}
	if runtimeA.pool == runtimeB.pool {
		t.Fatal("different applications shared a status worker pool")
	}
	runtimeA.pool.client.Transport = &statusSignalRoundTripper{status: http.StatusAccepted}
	runtimeB.pool.client.Transport = &statusSignalRoundTripper{status: http.StatusAccepted}
	factory, err := NewScopedStatusSignalEffectExecutorFactory(statusSignalConfig)
	if err != nil {
		t.Fatal(err)
	}
	executorA, err := factory.ForScope(cellA)
	if err != nil {
		t.Fatal(err)
	}
	email, err := effect.NewIntent("mail-1", effect.KindNotificationEmailSend, "shared", map[string]string{"template": "ready"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executorA.Submit(context.Background(), cellA, email); err == nil {
		t.Fatal("status executor accepted notification email")
	}
	notification := newEffectExecutor(t, NewMemoryEffectStore(), &fakeEffectWorker{}, func(context.Context, effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		return nil, nil, nil
	})
	if _, err := notification.Submit(context.Background(), cellA, statusSignalTestIntent(t, "status-other", "shared")); err == nil {
		t.Fatal("notification executor accepted status signal")
	}
	// Equal local idempotency keys are isolated by the complete caller scope.
	executorB, err := factory.ForScope(cellB)
	if err != nil {
		t.Fatal(err)
	}
	intent := statusSignalTestIntent(t, "status-a", "shared")
	if _, err := executorA.Submit(context.Background(), cellA, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := executorB.Submit(context.Background(), cellB, intent); err != nil {
		t.Fatalf("cross-scope local key was not isolated: %v", err)
	}
	if _, err := executorA.Receipt(context.Background(), cellB, "shared"); err == nil {
		t.Fatal("cross-scope receipt was visible")
	}
}

func TestStatusSignalCapabilitySetupOnlyIsRefCountedAndCrossAppIsolated(t *testing.T) {
	resetSharedWorkerPoolForTest(t)
	resetStatusSignalRuntimesForTest(t)
	root := t.TempDir()
	appA := scopedTestScope(t, "sessions", "prod", "host", "primary")
	appB := scopedTestScope(t, "evolution", "prod", "host", "primary")
	cellA := scopedTestScope(t, "sessions", "prod", "effects", "primary")
	cellB := scopedTestScope(t, "evolution", "prod", "effects", "primary")
	capability := newStatusSignalCapability(hostStatusSignalExecutors)
	if capability.Setup == nil || capability.TeardownScope == nil {
		t.Fatal("status capability must own scoped setup and teardown")
	}
	if err := capability.Setup(ext.SetupEnv{Scope: appA, StorageRoot: root, Logger: slog.Default()}); err != nil {
		t.Fatal(err)
	}
	if err := capability.Setup(ext.SetupEnv{Scope: appA, StorageRoot: root, Logger: slog.Default()}); err != nil {
		t.Fatal(err)
	}
	if err := capability.Setup(ext.SetupEnv{Scope: appB, StorageRoot: root, Logger: slog.Default()}); err != nil {
		t.Fatal(err)
	}
	if sharedWorkerPool() != nil {
		t.Fatal("setup-only status capability requires broad workers setup")
	}
	runtimeA, okA := statusSignalRuntimeForScope(cellA)
	runtimeB, okB := statusSignalRuntimeForScope(cellB)
	if !okA || !okB || runtimeA.pool == runtimeB.pool || statusSignalRuntimeCount() != 2 {
		t.Fatalf("status runtimes are not isolated: A=%p B=%p count=%d", runtimeA, runtimeB, statusSignalRuntimeCount())
	}
	if err := capability.TeardownScope(context.Background(), appA); err != nil {
		t.Fatal(err)
	}
	stillA, ok := statusSignalRuntimeForScope(cellA)
	if !ok || stillA.pool != runtimeA.pool || statusSignalRuntimeCount() != 2 {
		t.Fatal("first application A teardown ignored setup reference count")
	}
	if err := capability.TeardownScope(context.Background(), appA); err != nil {
		t.Fatal(err)
	}
	if _, ok := statusSignalRuntimeForScope(cellA); ok {
		t.Fatal("final application A teardown retained its runtime")
	}
	stillB, ok := statusSignalRuntimeForScope(cellB)
	if !ok || stillB.pool != runtimeB.pool || statusSignalRuntimeCount() != 1 {
		t.Fatal("application A teardown disturbed application B")
	}
	if err := capability.Setup(ext.SetupEnv{Scope: appA, StorageRoot: root, Logger: slog.Default()}); err != nil {
		t.Fatal(err)
	}
	restartedA, ok := statusSignalRuntimeForScope(cellA)
	if !ok || restartedA.pool == runtimeA.pool || restartedA.pool == runtimeB.pool {
		t.Fatal("application A restart did not create a fresh isolated pool")
	}
}

func TestConfiguredStatusSignalScopeUsesHostScopeOnly(t *testing.T) {
	host := scopedTestScope(t, "sessions", "green", "host", "primary")
	caller := scopedTestScope(t, "sessions", "green", "control", "primary")
	if err := ConfigureStatusSignalScope(host, StatusSignalScopeConfig{Endpoint: "https://status.example.test/ingest", BearerToken: "token"}); err != nil {
		t.Fatalf("configure host scope: %v", err)
	}
	config, err := configuredStatusSignalScope(caller)
	if err != nil || config.Endpoint != "https://status.example.test/ingest" {
		t.Fatalf("resolve caller config = (%#v, %v)", config, err)
	}
	if err := ConfigureStatusSignalScope(caller, config); err == nil {
		t.Fatal("cell scope was accepted as host config owner")
	}
}
