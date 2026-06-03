package workersext

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Pulp/abi"
)

// newTestPool builds a pool with the given egress allowlist, bypassing the
// env-derived guard. Passing "" yields a default deny-all-private guard so
// the SSRF block paths can be exercised against a loopback httptest server.
func newTestPool(t *testing.T, allow string) *workerPool {
	t.Helper()
	p := newWorkerPool(slog.Default(), defaultMaxConcurrency, defaultMaxQueued, defaultMaxPerCell, defaultMaxFetchBytes)
	t.Cleanup(p.teardown)

	guard := newEgressGuard(allow)
	dialer := &net.Dialer{Control: guard.dialControl}
	p.guard = guard
	p.client.Transport = &http.Transport{
		DialContext: guard.dialContext(dialer.DialContext),
	}
	p.client.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		return guard.checkScheme(req)
	}
	return p
}

// fetch runs a single http.fetch task to completion and returns its payload
// + status by polling result for the given cell.
func (p *workerPool) fetchSync(t *testing.T, cellID, url string) ([]byte, uint32) {
	t.Helper()
	id, code := p.submit(cellID, taskRequest{Type: "http.fetch", URL: url})
	if code != codeOK {
		t.Fatalf("submit returned code %d, want codeOK", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, status := p.result(cellID, id)
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

func TestIPBlocked_Ranges(t *testing.T) {
	blocked := []string{
		"127.0.0.1",       // loopback
		"::1",             // loopback v6
		"169.254.169.254", // cloud metadata (link-local)
		"10.1.2.3",        // RFC-1918
		"172.16.0.1",      // RFC-1918
		"192.168.1.1",     // RFC-1918
		"fc00::1",         // ULA
		"0.0.0.0",         // unspecified
	}
	for _, s := range blocked {
		if !ipBlocked(net.ParseIP(s)) {
			t.Errorf("ipBlocked(%s) = false, want true", s)
		}
	}
	public := []string{"1.1.1.1", "8.8.8.8", "93.184.216.34", "2606:4700:4700::1111"}
	for _, s := range public {
		if ipBlocked(net.ParseIP(s)) {
			t.Errorf("ipBlocked(%s) = true, want false (public)", s)
		}
	}
}

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

	id, code := p.submit("cellA", taskRequest{Type: "http.fetch", URL: srv.URL})
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
	dataB, statusB := p.result("cellB", id)
	if statusB != statusUnknown {
		t.Fatalf("cross-cell poll should be denied (statusUnknown), got status=%d body=%q", statusB, string(dataB))
	}
	if dataB != nil {
		t.Fatalf("cross-cell poll leaked data: %q", string(dataB))
	}

	// Cell A's legitimate poll still works — B did not consume/delete it.
	dataA, statusA := p.result("cellA", id)
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

	id, code := p.submit("cellA", taskRequest{Type: "http.fetch", URL: srv.URL})
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
	if rc := p.cancel("cellB", id); rc != 1 {
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
	if rc := p.cancel("cellA", id); rc != 0 {
		t.Fatalf("owner cancel should succeed (rc=0), got %d", rc)
	}
}
