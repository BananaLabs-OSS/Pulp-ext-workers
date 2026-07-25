package workersext

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Pulp/abi"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/BananaLabs-OSS/Pulp/ssrfguard"
)

// newTestPool builds a pool with the given egress allowlist, bypassing the
// env-derived guard. Passing "" yields a default deny-all-private guard (no
// seed hosts) so the SSRF block paths can be exercised against a loopback
// httptest server.
func newTestPool(t *testing.T, allow string) *workerPool {
	t.Helper()
	p := newWorkerPool(slog.Default(), defaultMaxConcurrency, defaultMaxQueued, defaultMaxPerCell, defaultMaxFetchBytes)
	t.Cleanup(p.teardown)

	guard := ssrfguard.NewEgressGuard(allow, nil)
	dialer := &net.Dialer{Control: guard.DialControl}
	p.guard = guard
	p.client.Transport = &http.Transport{
		DialContext: guard.DialContext(dialer.DialContext),
	}
	p.client.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		return guard.CheckScheme(req)
	}
	return p
}

func testScope(cellID string) ext.Scope { return ext.LegacyScope(cellID) }

func scopedTestScope(t *testing.T, app, appInstance, cell, cellInstance string) ext.Scope {
	t.Helper()
	scope, err := ext.NewScope(app, appInstance, cell, cellInstance)
	if err != nil {
		t.Fatalf("new scope: %v", err)
	}
	return scope
}

// fetch runs a single http.fetch task to completion and returns its payload
// + status by polling result for the given cell.
func (p *workerPool) fetchSync(t *testing.T, cellID, url string) ([]byte, uint32) {
	t.Helper()
	scope := testScope(cellID)
	id, code := p.submit(scope, taskRequest{Type: "http.fetch", URL: url})
	if code != codeOK {
		t.Fatalf("submit returned code %d, want codeOK", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, status := p.result(scope, id)
		if status != statusPending {
			return data, status
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("task did not complete within deadline")
	return nil, statusUnknown
}

// ---------------------------------------------------------------------
// SSRF egress guard
// ---------------------------------------------------------------------

// TestSSRF_BlocksPrivate confirms a default (no-allowlist) pool refuses to
// reach a loopback httptest server — the metadata/localhost/RFC-1918 SSRF
// class. doHTTPFetch surfaces the block as statusError.
func TestSSRF_BlocksPrivate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := newTestPool(t, "") // deny all private
	data, status := p.fetchSync(t, "cellA", srv.URL)
	if status != statusError {
		t.Fatalf("expected SSRF guard to block loopback target (statusError), got status=%d body=%q", status, string(data))
	}
	if !strings.Contains(string(data), "blocked") {
		t.Fatalf("expected blocked-target error, got %q", string(data))
	}
}

// TestSSRF_BlocksScheme confirms non-http(s) schemes are rejected before any
// dial.
func TestSSRF_BlocksScheme(t *testing.T) {
	p := newTestPool(t, "")
	for _, u := range []string{"file:///etc/passwd", "gopher://127.0.0.1:70/", "ftp://example.com/x"} {
		_, err := p.doHTTPFetch(context.Background(), taskRequest{Type: "http.fetch", URL: u})
		if err == nil {
			t.Errorf("expected scheme %q to be rejected, got nil error", u)
		}
	}
}

// TestSSRF_AllowlistPermitsLoopback confirms an explicit CIDR allowlist lets
// a genuinely-needed internal target through.
func TestSSRF_AllowlistPermitsLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p := newTestPool(t, "127.0.0.0/8,::1/128")
	data, status := p.fetchSync(t, "cellA", srv.URL)
	if status != statusComplete {
		t.Fatalf("allowlisted loopback should succeed, got status=%d body=%q", status, string(data))
	}
	resp, err := abi.DecodeHTTPResponse(data)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != http.StatusNoContent {
		t.Errorf("status = %d, want 204", resp.Status)
	}
}

// TestSSRF_RedirectToBadSchemeBlocked confirms a redirect to a non-http(s)
// scheme is refused mid-chain by CheckRedirect.
func TestSSRF_RedirectToBadSchemeBlocked(t *testing.T) {
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "file:///etc/passwd", http.StatusFound)
	}))
	defer redir.Close()

	p := newTestPool(t, "127.0.0.0/8,::1/128") // permit the redirector
	data, status := p.fetchSync(t, "cellA", redir.URL)
	if status != statusError {
		t.Fatalf("expected redirect to file:// to be blocked, got status=%d body=%q", status, string(data))
	}
	if !strings.Contains(string(data), "scheme") {
		t.Fatalf("expected scheme error, got %q", string(data))
	}
}

// ---------------------------------------------------------------------
// Body cap (OOM guard)
// ---------------------------------------------------------------------

// TestBodyCap_ErrorsPastLimit confirms a response body larger than the cap
// surfaces an explicit error rather than buffering unbounded host memory.
func TestBodyCap_ErrorsPastLimit(t *testing.T) {
	const cap = 1024
	blob := strings.Repeat("x", cap*4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(blob))
	}))
	defer srv.Close()

	p := newTestPool(t, "127.0.0.0/8,::1/128")
	p.maxFetchBytes = cap

	data, status := p.fetchSync(t, "cellA", srv.URL)
	if status != statusError {
		t.Fatalf("expected over-cap body to error, got status=%d", status)
	}
	if !strings.Contains(string(data), "exceeds") {
		t.Fatalf("expected size-exceeded error, got %q", string(data))
	}
}

// TestBodyCap_AllowsUnderLimit confirms a body at/under the cap succeeds.
func TestBodyCap_AllowsUnderLimit(t *testing.T) {
	body := "hello world"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	p := newTestPool(t, "127.0.0.0/8,::1/128")
	p.maxFetchBytes = 1024

	data, status := p.fetchSync(t, "cellA", srv.URL)
	if status != statusComplete {
		t.Fatalf("under-cap body should succeed, got status=%d body=%q", status, string(data))
	}
	resp, err := abi.DecodeHTTPResponse(data)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if string(resp.Body) != body {
		t.Errorf("body = %q, want %q", string(resp.Body), body)
	}
}

// ---------------------------------------------------------------------
// Cross-cell result/cancel scoping
// ---------------------------------------------------------------------

// TestCrossCell_ResultDenied confirms a sibling cell cannot poll (or steal /
// delete) another cell's task result via an enumerable global ID. Cell B
// polling cell A's id sees statusUnknown, and cell A's own poll still
// succeeds afterward.
func TestCrossCell_ResultDenied(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("CELL-A-SECRET"))
	}))
	defer srv.Close()

	p := newTestPool(t, "127.0.0.0/8,::1/128")

	id, code := p.submit(testScope("cellA"), taskRequest{Type: "http.fetch", URL: srv.URL})
	if code != codeOK {
		t.Fatalf("submit returned code %d", code)
	}

	// Wait for cellA's task to complete.
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		_, done := p.results[id]
		p.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cellA task did not complete")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Cell B tries to steal cellA's result by enumerating the ID.
	dataB, statusB := p.result(testScope("cellB"), id)
	if statusB != statusUnknown {
		t.Fatalf("cross-cell poll should be denied (statusUnknown), got status=%d body=%q", statusB, string(dataB))
	}
	if dataB != nil {
		t.Fatalf("cross-cell poll leaked data: %q", string(dataB))
	}

	// Cell A's legitimate poll still works — B did not consume/delete it.
	dataA, statusA := p.result(testScope("cellA"), id)
	if statusA != statusComplete {
		t.Fatalf("owner poll should succeed, got status=%d", statusA)
	}
	resp, err := abi.DecodeHTTPResponse(dataA)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(resp.Body) != "CELL-A-SECRET" {
		t.Errorf("owner got wrong body: %q", string(resp.Body))
	}
}

// TestCrossCell_CancelDenied confirms a sibling cell cannot cancel another
// cell's in-flight task.
func TestCrossCell_CancelDenied(t *testing.T) {
	// A server that blocks until the test releases it, keeping the task
	// in-flight long enough to attempt a cross-cell cancel.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	p := newTestPool(t, "127.0.0.0/8,::1/128")

	id, code := p.submit(testScope("cellA"), taskRequest{Type: "http.fetch", URL: srv.URL})
	if code != codeOK {
		t.Fatalf("submit returned code %d", code)
	}

	// Wait until the task is actually in-flight.
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		_, inflight := p.inflight[id]
		p.mu.Unlock()
		if inflight {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task never became in-flight")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Cell B's cancel must be refused (return 1 = not found / not owned).
	if rc := p.cancel(testScope("cellB"), id); rc != 1 {
		t.Fatalf("cross-cell cancel should be denied (rc=1), got %d", rc)
	}

	// The task is still in-flight (B did not cancel it).
	p.mu.Lock()
	_, stillInflight := p.inflight[id]
	p.mu.Unlock()
	if !stillInflight {
		t.Fatal("cross-cell cancel wrongly cancelled the task")
	}

	// Owner cancel succeeds.
	if rc := p.cancel(testScope("cellA"), id); rc != 0 {
		t.Fatalf("owner cancel should succeed (rc=0), got %d", rc)
	}
}

// TestScopedApplications_IsolateQuotaResultAndIdempotency verifies the core
// multi-application invariant: the same statically-linked workers extension
// may share one pool, but every mutable work record is owned by its complete
// application/instance/cell placement. In particular, two apps may both have
// an "api" cell without sharing a quota, task result, or idempotency ledger.
func TestScopedApplications_IsolateQuotaResultAndIdempotency(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	defer close(release)

	p := newTestPool(t, "127.0.0.0/8,::1/128")
	p.scopes.maxPerCell = 1

	evolutionAPI := scopedTestScope(t, "evolution", "blue", "api", "one")
	sessionsAPI := scopedTestScope(t, "sessions", "blue", "api", "one")
	req := taskRequest{Type: "http.fetch", URL: srv.URL, IdempotencyKey: "delivery-42"}

	evolutionID, code := p.submit(evolutionAPI, req)
	if code != codeOK {
		t.Fatalf("Evolution submit code = %d, want %d", code, codeOK)
	}
	// A matching retry is exactly-once within its own scope, even when that
	// scope's concurrency quota is currently full.
	retryID, code := p.submit(evolutionAPI, req)
	if code != codeOK || retryID != evolutionID {
		t.Fatalf("same-scope idempotent retry = (%d, %d), want (%d, %d)", retryID, code, evolutionID, codeOK)
	}
	if _, code := p.submit(evolutionAPI, taskRequest{Type: "http.fetch", URL: srv.URL + "/different", IdempotencyKey: "delivery-42"}); code != codeIdempotencyConflict {
		t.Fatalf("same-scope conflicting key code = %d, want %d", code, codeIdempotencyConflict)
	}

	// The Sessions app has the same cell name and same local idempotency key,
	// but must receive its own task and its own per-cell-instance capacity.
	sessionsID, code := p.submit(sessionsAPI, req)
	if code != codeOK || sessionsID == evolutionID {
		t.Fatalf("cross-app submit = (%d, %d), want distinct successful task", sessionsID, code)
	}
	if _, code := p.submit(sessionsAPI, taskRequest{Type: "http.fetch", URL: srv.URL, IdempotencyKey: "another"}); code != codeCellFull {
		t.Fatalf("same scoped cell quota code = %d, want %d", code, codeCellFull)
	}

	// Enumerable task IDs reveal neither in-flight state nor result data to a
	// different application placement.
	if data, status := p.result(sessionsAPI, evolutionID); status != statusUnknown || data != nil {
		t.Fatalf("cross-app result = (%q, %d), want (nil, %d)", data, status, statusUnknown)
	}
	if rc := p.cancel(sessionsAPI, evolutionID); rc != 1 {
		t.Fatalf("cross-app cancel = %d, want 1", rc)
	}
}

// TestLegacyScopeCompatibility confirms existing hosts that only expose a
// cell name keep their prior ownership boundary and do not need a new ABI.
func TestLegacyScopeCompatibility(t *testing.T) {
	p := newTestPool(t, "")
	legacy := testScope("sessions-gene")
	if legacy != ext.ScopeOf(legacyCell{name: "sessions-gene"}) {
		t.Fatal("legacy cells must retain their stable default scope")
	}
	if _, code := p.submit(legacy, taskRequest{Type: "unknown"}); code != codeOK {
		t.Fatalf("legacy submit code = %d, want %d", code, codeOK)
	}
}

func resetSharedWorkerPoolForTest(t *testing.T) {
	t.Helper()
	workersHost.mu.Lock()
	old := workersHost.pool
	workersHost.pool = nil
	workersHost.owners = make(map[ext.Scope]struct{})
	workersHost.runtimes = make(map[workerApplicationKey]workerApplicationRuntime)
	workersHost.mu.Unlock()
	if old != nil {
		old.teardown()
	}
	t.Cleanup(func() {
		workersHost.mu.Lock()
		current := workersHost.pool
		workersHost.pool = nil
		workersHost.owners = make(map[ext.Scope]struct{})
		workersHost.runtimes = make(map[workerApplicationKey]workerApplicationRuntime)
		workersHost.mu.Unlock()
		if current != nil {
			current.teardown()
		}
	})
}

func TestWorkersLifecycle_TwoApplicationSetupsSharePoolAndTeardownIsScoped(t *testing.T) {
	resetSharedWorkerPoolForTest(t)
	appA := scopedTestScope(t, "evolution", "blue", "host", "primary")
	appB := scopedTestScope(t, "sessions", "blue", "host", "primary")
	if err := workersSetup(ext.SetupEnv{Scope: appA, Logger: slog.Default()}); err != nil {
		t.Fatalf("setup app A: %v", err)
	}
	first := sharedWorkerPool()
	if first == nil {
		t.Fatal("app A did not create shared worker pool")
	}
	if err := workersSetup(ext.SetupEnv{Scope: appB, Logger: slog.Default()}); err != nil {
		t.Fatalf("setup app B: %v", err)
	}
	if got := sharedWorkerPool(); got != first {
		t.Fatal("app B setup replaced the active shared worker pool")
	}

	// Application A teardown must leave the shared implementation and app B's
	// state alive until B releases the final owner reference.
	if err := workersTeardownScope(context.Background(), appA); err != nil {
		t.Fatalf("teardown app A: %v", err)
	}
	if got := sharedWorkerPool(); got != first {
		t.Fatal("application A teardown stopped/replaced app B's shared pool")
	}
	select {
	case <-first.cleanupDone:
		t.Fatal("application A teardown stopped app B's shared pool cleanup loop")
	default:
	}

	cellA := scopedTestScope(t, "evolution", "blue", "notifications", "one")
	cellB := scopedTestScope(t, "sessions", "blue", "notifications", "one")
	first.registerScope(cellA)
	first.registerScope(cellB)
	first.mu.Lock()
	first.results[901] = &taskResult{data: []byte("a"), status: statusComplete, completed: time.Now(), scope: cellA}
	first.results[902] = &taskResult{data: []byte("b"), status: statusComplete, completed: time.Now(), scope: cellB}
	first.mu.Unlock()
	if err := workersTeardownCell(context.Background(), cellA.RoutingID()); err != nil {
		t.Fatalf("teardown scoped cell A: %v", err)
	}
	if _, status := first.result(cellA, 901); status != statusUnknown {
		t.Fatalf("cell A result status after teardown = %d, want unknown", status)
	}
	if data, status := first.result(cellB, 902); status != statusComplete || string(data) != "b" {
		t.Fatalf("cell B result after A teardown = (%q, %d), want (b, %d)", data, status, statusComplete)
	}
	if err := workersTeardownScope(context.Background(), appB); err != nil {
		t.Fatalf("teardown app B: %v", err)
	}
	if sharedWorkerPool() != nil {
		t.Fatal("last application teardown retained worker pool")
	}
	select {
	case <-first.cleanupDone:
	default:
		t.Fatal("last application teardown did not stop worker pool cleanup loop")
	}
}

func TestWorkersLifecycle_RepeatedTwoAppSetupTeardownIsRaceSafe(t *testing.T) {
	resetSharedWorkerPoolForTest(t)
	appA := scopedTestScope(t, "evolution", "green", "host", "primary")
	appB := scopedTestScope(t, "sessions", "green", "host", "primary")
	var wg sync.WaitGroup
	for index := 0; index < 24; index++ {
		scope := appA
		if index%2 == 1 {
			scope = appB
		}
		wg.Add(1)
		go func(scope ext.Scope) {
			defer wg.Done()
			for round := 0; round < 8; round++ {
				if err := workersSetup(ext.SetupEnv{Scope: scope, Logger: slog.Default()}); err != nil {
					t.Errorf("setup: %v", err)
				}
				if err := workersTeardownScope(context.Background(), scope); err != nil {
					t.Errorf("teardown: %v", err)
				}
			}
		}(scope)
	}
	wg.Wait()
	if err := workersSetup(ext.SetupEnv{Scope: appA, Logger: slog.Default()}); err != nil {
		t.Fatalf("final setup: %v", err)
	}
	if sharedWorkerPool() == nil {
		t.Fatal("repeated app lifecycle could not restore shared pool")
	}
}

func TestScopedIdempotency_ReplaysConsumedResult(t *testing.T) {
	p := newTestPool(t, "")
	scope := scopedTestScope(t, "sessions", "blue", "commerce", "one")
	req := taskRequest{Type: "unknown", IdempotencyKey: "effect-7"}
	id, code := p.submit(scope, req)
	if code != codeOK {
		t.Fatalf("submit code = %d, want %d", code, codeOK)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, status := p.result(scope, id)
		if status == statusError {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task did not complete")
		}
		time.Sleep(time.Millisecond)
	}
	if !p.consume(scope, id) {
		t.Fatal("owner could not consume completed result")
	}

	retryID, code := p.submit(scope, req)
	if code != codeOK || retryID != id {
		t.Fatalf("consumed idempotent retry = (%d, %d), want (%d, %d)", retryID, code, id, codeOK)
	}
	if _, status := p.result(scope, retryID); status != statusError {
		t.Fatalf("replayed result status = %d, want %d", status, statusError)
	}
}

type legacyCell struct{ name string }

func (c legacyCell) Name() string { return c.name }
