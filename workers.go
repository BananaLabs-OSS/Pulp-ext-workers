// Package workersext is Pulp's host-side parallel task runner extension.
// Cells submit work via host imports, the host runs each task in its own
// goroutine, and the cell polls for results. Fire-and-forget is also
// supported for tasks where the response doesn't matter.
//
// Currently supports one task type: http.fetch (outbound HTTP via net/http).
// Adding new types is a matter of adding a case to the switch in runTask.
package workersext

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BananaLabs-OSS/Pulp/abi"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/BananaLabs-OSS/Pulp/ssrfguard"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

// ---------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------

const (
	defaultFetchTimeout    = 30 * time.Second
	maxFetchTimeout        = 300 * time.Second // upper bound when no timeout_ms is set
	resultTTL              = 5 * time.Minute
	teardownGrace          = 5 * time.Second
	defaultMaxConcurrency  = 32
	defaultMaxQueued       = 1024
	defaultMaxPerCell      = 8
)

// maxFetchBytes caps the http.fetch response body buffered in host memory.
// Without a cap a cell-controlled URL can point at an endpoint streaming
// gigabytes (or a Content-Length-less attacker server) and OOM the whole
// Pulp host — multiplied by maxConcurrency simultaneous fetches. The cap is
// sized to admit the largest legitimate transfer this fetcher carries:
// Evolution's world-archive/backup download (Evolution/pulp-cell/poller.go
// notes "game worlds cap at ~10GB"), routed through workers http.fetch since
// the mutex-deadlock refactor. 16 GiB leaves headroom over that ~10 GB
// ceiling while still bounding a truly-unbounded / Content-Length-less
// hostile body so it cannot OOM the host. Override via
// PULP_WORKERS_MAX_FETCH_BYTES. A body past the cap surfaces an explicit
// error rather than silently truncating.
const defaultMaxFetchBytes int64 = 16 * 1024 * 1024 * 1024 // 16 GiB

// Result status codes returned by workers_result.
const (
	statusPending  = 0
	statusComplete = 1
	statusError    = 2
	statusPanic    = 3
	statusUnknown  = 4
)

// Host error codes for workers_submit / workers_submit_fire.
const (
	codeOK          = 0
	codeEmptyReq    = 1
	codeMemRead     = 2
	codeDecode      = 3
	codeFireFailed  = 4
	codeQueueFull   = 15
	codeCellFull    = 16
	codeSaturated   = 17
	codeCapAbsent   = 99
)

// ErrWorkerSaturated is returned when the concurrency semaphore is full and
// the submit call would block the WASM host import indefinitely.
var ErrWorkerSaturated = errors.New("worker pool saturated: all concurrency slots in use")

// ---------------------------------------------------------------------
// Module-level state
// ---------------------------------------------------------------------

var pool *workerPool

// ---------------------------------------------------------------------
// init — register the workers capability
// ---------------------------------------------------------------------

func init() {
	ext.Register(ext.Capability{
		Name:         "workers",
		Register:     workersRegister,
		Stub:         workersStub,
		Setup:        workersSetup,
		Teardown:     workersTeardown,
		TeardownCell: workersTeardownCell,
	})
}

// ---------------------------------------------------------------------
// Task request (decoded from cell msgpack)
// ---------------------------------------------------------------------

type taskRequest struct {
	Type      string            `msgpack:"type"`
	Method    string            `msgpack:"method"`
	URL       string            `msgpack:"url"`
	Headers   map[string]string `msgpack:"headers"`
	Body      []byte            `msgpack:"body"`
	TimeoutMs uint32            `msgpack:"timeout_ms"`
}

// ---------------------------------------------------------------------
// Task result stored after completion
// ---------------------------------------------------------------------

type taskResult struct {
	data      []byte // payload: msgpack abi.HTTPResponse on complete; raw error string on error/panic
	status    uint32 // statusComplete, statusError, or statusPanic
	completed time.Time
	cellID    string // owning cell (for per-cell teardown)
}

// ---------------------------------------------------------------------
// In-flight task
// ---------------------------------------------------------------------

type inflightTask struct {
	cancel context.CancelFunc
	done   chan struct{}
	cellID string // owning cell (for per-cell teardown)
}

// ---------------------------------------------------------------------
// Per-cell task tracker
// ---------------------------------------------------------------------

// cellTracker enforces a per-cell ceiling on concurrent tasks so that
// one misbehaving cell cannot consume all global worker slots.
type cellTracker struct {
	mu         sync.Mutex
	inflight   map[string]int // cellID -> count of active tasks (submit + submitFire)
	maxPerCell int
}

func newCellTracker(maxPerCell int) *cellTracker {
	return &cellTracker{
		inflight:   make(map[string]int),
		maxPerCell: maxPerCell,
	}
}

// acquire increments the cell's counter if under the limit.
// Returns true if the slot was acquired, false if the cell is full.
func (ct *cellTracker) acquire(cellID string) bool {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if ct.inflight[cellID] >= ct.maxPerCell {
		return false
	}
	ct.inflight[cellID]++
	return true
}

// release decrements the cell's counter by one. Safe to call even if
// the counter is already zero (clamps to zero).
func (ct *cellTracker) release(cellID string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if ct.inflight[cellID] > 0 {
		ct.inflight[cellID]--
	}
	if ct.inflight[cellID] == 0 {
		delete(ct.inflight, cellID)
	}
}

// dropCell zeroes the cell's counter. Used during per-cell teardown
// after all in-flight tasks have been cancelled.
func (ct *cellTracker) dropCell(cellID string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	delete(ct.inflight, cellID)
}

// countForCell returns the current inflight count for the cell.
func (ct *cellTracker) countForCell(cellID string) int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return ct.inflight[cellID]
}

// =====================================================================
// SSRF egress guard
// =====================================================================
//
// http.fetch performs outbound HTTP with a cell-supplied URL, and those
// URLs are USER-influenced (Evolution forwards customer-supplied datapack /
// world-restore URLs through cells). Without a guard a hostile or buggy
// cell could reach the cloud-metadata endpoint (169.254.169.254),
// localhost, RFC-1918 ranges, or other internal services on the VPS —
// classic SSRF. This mirrors the guard the sibling Pulp-ext-http ships;
// this extension re-implements outbound HTTP from scratch so it must carry
// its own copy.
//
// The guard does three things:
//  1. Scheme allowlist — only http/https (rejects file://, gopher://, …).
//  2. IP block — at DIAL time it validates the RESOLVED IP against a
//     deny-list of loopback / link-local / private / ULA / unspecified
//     ranges. Validating the resolved IP (not the hostname string) defeats
//     DNS-rebinding: even if a name resolves public at check time and
//     private at connect time, the dialer sees the real connect IP.
//  3. Redirect re-validation — http.Client.CheckRedirect re-runs the scheme
//     check on every hop, and the dialer re-runs the IP check for each hop's
//     connection, so a redirect to an internal target is refused mid-chain.
//
// A genuinely-needed internal host can be allowlisted via the
// HTTP_FETCH_ALLOW env var (comma-separated host[:port] or CIDR entries),
// kept consistent with Pulp-ext-http; default is deny-all-private.

// The SSRF egress guard is provided by the shared ssrfguard package.
// See github.com/BananaLabs-OSS/Pulp/ssrfguard for full documentation.
// ext-workers uses a deny-all-private default (no seed hosts).

// ---------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------

type workerPool struct {
	logger       *slog.Logger
	client       *http.Client
	guard        *ssrfguard.EgressGuard
	maxFetchBytes int64
	nextID       atomic.Uint32

	sem            chan struct{}
	maxConcurrency int
	maxQueued      int

	cells *cellTracker

	mu       sync.Mutex
	inflight map[uint32]*inflightTask
	results  map[uint32]*taskResult

	// Background cleanup
	cleanupDone chan struct{}
	cleanupStop context.CancelFunc
}

func newWorkerPool(logger *slog.Logger, maxConcurrency, maxQueued, maxPerCell int, maxFetchBytes int64) *workerPool {
	ctx, cancel := context.WithCancel(context.Background())
	// Reuse one transport with a real keep-alive pool. Default
	// http.Client builds a fresh transport with tiny idle-conn limits —
	// every Bananagine/Resend call pays a TCP (and TLS for Resend)
	// handshake. A per-host idle pool collapses that to one handshake
	// per host across the process lifetime.
	//
	// DialContext uses a net.Dialer whose Control hook runs AFTER DNS
	// resolution with the concrete IP about to be dialed — the SSRF egress
	// guard. Checking the resolved IP (not the hostname) defeats DNS
	// rebinding. See ssrfguard.EgressGuard.
	guard := ssrfguard.NewEgressGuard(os.Getenv("HTTP_FETCH_ALLOW"), nil)
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   guard.DialControl,
	}
	transport := &http.Transport{
		DialContext:           guard.DialContext(dialer.DialContext),
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   32,
		MaxConnsPerHost:       64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	p := &workerPool{
		logger: logger,
		client: &http.Client{
			// No global Timeout: each task sets its own deadline via the
			// request context (runTask wraps in context.WithTimeout using
			// req.TimeoutMs, falling back to maxFetchTimeout). A hard client
			// Timeout of 30 s would silently truncate any task with
			// timeout_ms > 30 000, ignoring the cell's explicit request.
			Transport: transport,
			// Re-validate the scheme on every redirect hop; the IP block is
			// enforced by the dialer Control hook on each hop's connection,
			// so a redirect to an internal target is refused at dial time.
			CheckRedirect: func(req *http.Request, _ []*http.Request) error {
				return guard.CheckScheme(req)
			},
		},
		guard:         guard,
		maxFetchBytes: maxFetchBytes,
		sem:            make(chan struct{}, maxConcurrency),
		maxConcurrency: maxConcurrency,
		maxQueued:      maxQueued,
		cells:          newCellTracker(maxPerCell),
		inflight:       make(map[uint32]*inflightTask),
		results:        make(map[uint32]*taskResult),
		cleanupDone:    make(chan struct{}),
		cleanupStop:    cancel,
	}
	go p.cleanupLoop(ctx)
	return p
}

// inflightCount returns total inflight (running + queued results not yet reaped).
func (p *workerPool) inflightCount() int {
	p.mu.Lock()
	n := len(p.inflight)
	p.mu.Unlock()
	return n
}

// firstTaskID is the first task id handed out. Kept above the largest
// reserved host error code (99) so callers can unambiguously distinguish
// a task id from an error code returned on the same channel.
const firstTaskID = 100

// submit queues a task and returns (id, code). On success code == codeOK.
// cellID identifies the owning cell for per-cell teardown; empty is fine
// in single-cell deployments.
func (p *workerPool) submit(cellID string, req taskRequest) (uint32, uint32) {
	if p.inflightCount() >= p.maxQueued {
		return 0, codeQueueFull
	}
	if cellID != "" && !p.cells.acquire(cellID) {
		return 0, codeCellFull
	}

	// Skip reserved values so task IDs never collide with error codes.
	var id uint32
	for {
		id = p.nextID.Add(1)
		if id >= firstTaskID {
			break
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	task := &inflightTask{cancel: cancel, done: make(chan struct{}), cellID: cellID}

	p.mu.Lock()
	p.inflight[id] = task
	p.mu.Unlock()

	select {
	case p.sem <- struct{}{}:
	default:
		// All concurrency slots are occupied. Reject immediately so the
		// WASM host import returns rather than blocking the caller indefinitely.
		p.mu.Lock()
		delete(p.inflight, id)
		p.mu.Unlock()
		task.cancel()
		if cellID != "" {
			p.cells.release(cellID)
		}
		return 0, codeSaturated
	}
	go func() {
		defer func() {
			<-p.sem
			if cellID != "" {
				p.cells.release(cellID)
			}
			close(task.done)
		}()
		defer func() {
			if r := recover(); r != nil {
				p.logger.Error("worker task panicked", "id", id, "cell", cellID, "type", req.Type, "panic", r)
				p.mu.Lock()
				delete(p.inflight, id)
				p.results[id] = &taskResult{
					data:      []byte(fmt.Sprintf("panic: %v", r)),
					status:    statusPanic,
					completed: time.Now(),
					cellID:    cellID,
				}
				p.mu.Unlock()
			}
		}()

		data, err := p.runTask(ctx, req)

		p.mu.Lock()
		delete(p.inflight, id)
		if err != nil {
			p.results[id] = &taskResult{
				data:      []byte(err.Error()),
				status:    statusError,
				completed: time.Now(),
				cellID:    cellID,
			}
		} else {
			p.results[id] = &taskResult{
				data:      data,
				status:    statusComplete,
				completed: time.Now(),
				cellID:    cellID,
			}
		}
		p.mu.Unlock()
	}()

	return id, codeOK
}

// submitFire runs a task without tracking results. Returns host code.
func (p *workerPool) submitFire(cellID string, req taskRequest) uint32 {
	if p.inflightCount() >= p.maxQueued {
		return codeQueueFull
	}
	if cellID != "" && !p.cells.acquire(cellID) {
		return codeCellFull
	}

	select {
	case p.sem <- struct{}{}:
	default:
		if cellID != "" {
			p.cells.release(cellID)
		}
		return codeSaturated
	}
	go func() {
		defer func() {
			<-p.sem
			if cellID != "" {
				p.cells.release(cellID)
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				p.logger.Error("fire-and-forget task panicked", "cell", cellID, "type", req.Type, "panic", r)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), maxFetchTimeout)
		defer cancel()
		if _, err := p.runTask(ctx, req); err != nil {
			p.logger.Warn("fire-and-forget task failed", "cell", cellID, "type", req.Type, "err", err)
		}
	}()
	return codeOK
}

// result polls for a completed task. Returns (data, status). On
// statusComplete the data is a msgpack-encoded abi.HTTPResponse. On
// statusError or statusPanic the data is a raw UTF-8 error string —
// the cell-side wrapper surfaces it via TaskResult.Error.
//
// cellID scopes ownership: a cell may only poll its OWN tasks. Task IDs are
// a global, sequential, enumerable counter, so without this check a hostile
// sibling could sweep IDs and steal (and delete) another cell's result. A
// mismatch is reported as statusUnknown — indistinguishable from a
// never-submitted ID, leaking nothing. Single-cell deployments (cellID=="")
// own the matching "" tasks, so they are unaffected.
func (p *workerPool) result(cellID string, id uint32) ([]byte, uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if r, ok := p.results[id]; ok {
		if r.cellID != cellID {
			return nil, statusUnknown
		}
		// Only consume the result on terminal statuses — keeps polling
		// idempotent for any non-terminal snapshot that somehow lands
		// here (shouldn't happen today, but cheap insurance).
		if r.status == statusComplete || r.status == statusError || r.status == statusPanic {
			delete(p.results, id)
		}
		return r.data, r.status
	}
	if t, ok := p.inflight[id]; ok {
		if t.cellID != cellID {
			return nil, statusUnknown
		}
		return nil, statusPending
	}
	return nil, statusUnknown
}

// cancel attempts to cancel an in-flight task owned by cellID. A cell may
// only cancel its OWN tasks — a cross-cell cancel would be a DoS against a
// sibling's in-flight work. A mismatch (or missing id) returns 1 (not
// found), same as an already-done task.
func (p *workerPool) cancel(cellID string, id uint32) uint32 {
	p.mu.Lock()
	task, ok := p.inflight[id]
	if ok && task.cellID != cellID {
		ok = false
	}
	p.mu.Unlock()
	if !ok {
		return 1 // not found, already done, or not owned by this cell
	}
	task.cancel()
	return 0
}

// pending returns the number of in-flight tasks.
func (p *workerPool) pending() uint32 {
	p.mu.Lock()
	n := len(p.inflight)
	p.mu.Unlock()
	return uint32(n)
}

// teardown cancels all in-flight tasks and waits up to teardownGrace.
func (p *workerPool) teardown() {
	p.cleanupStop()
	<-p.cleanupDone

	p.mu.Lock()
	tasks := make([]*inflightTask, 0, len(p.inflight))
	for _, t := range p.inflight {
		tasks = append(tasks, t)
	}
	p.mu.Unlock()

	for _, t := range tasks {
		t.cancel()
	}

	deadline := time.After(teardownGrace)
	for _, t := range tasks {
		select {
		case <-t.done:
		case <-deadline:
			return
		}
	}
}

// teardownCell cancels in-flight tasks owned by cellID, waits up to
// teardownGrace for them to exit, and drops any completed-but-unpolled
// results belonging to that cell. Other cells' state is left untouched.
// Returns (cancelled, results) counts for logging.
func (p *workerPool) teardownCell(cellID string) (int, int) {
	p.mu.Lock()
	tasks := make([]*inflightTask, 0)
	for _, t := range p.inflight {
		if t.cellID == cellID {
			tasks = append(tasks, t)
		}
	}
	resultsDropped := 0
	for id, r := range p.results {
		if r.cellID == cellID {
			delete(p.results, id)
			resultsDropped++
		}
	}
	p.mu.Unlock()

	for _, t := range tasks {
		t.cancel()
	}

	deadline := time.After(teardownGrace)
	for _, t := range tasks {
		select {
		case <-t.done:
		case <-deadline:
			// Tasks that didn't finish in time still hold cell slots.
			// Drop the entire cell counter — those goroutines will
			// call release() when they eventually exit, and release()
			// clamps to zero so the extra decrements are harmless.
			p.cells.dropCell(cellID)
			return len(tasks), resultsDropped
		}
	}

	// All tasks exited cleanly; their defers already called release().
	// Drop any residual counter (shouldn't be needed, but defensive).
	p.cells.dropCell(cellID)
	return len(tasks), resultsDropped
}

// cleanupLoop removes stale results every 60 seconds.
func (p *workerPool) cleanupLoop(ctx context.Context) {
	defer close(p.cleanupDone)
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			p.mu.Lock()
			for id, r := range p.results {
				if now.Sub(r.completed) > resultTTL {
					delete(p.results, id)
				}
			}
			p.mu.Unlock()
		}
	}
}

// runTask dispatches to the appropriate task handler based on type.
func (p *workerPool) runTask(ctx context.Context, req taskRequest) ([]byte, error) {
	timeout := maxFetchTimeout
	if req.TimeoutMs > 0 {
		timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()
	switch req.Type {
	case "http.fetch":
		return p.doHTTPFetch(ctx, req)
	default:
		return nil, fmt.Errorf("unknown task type: %s", req.Type)
	}
}

// doHTTPFetch performs an outbound HTTP request.
func (p *workerPool) doHTTPFetch(ctx context.Context, req taskRequest) ([]byte, error) {
	if strings.TrimSpace(req.URL) == "" {
		return nil, errors.New("url is required")
	}
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method == "" {
		method = http.MethodGet
	}

	var body io.Reader
	if len(req.Body) > 0 {
		body = bytes.NewReader(req.Body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, req.URL, body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	// Scheme allowlist (http/https only) before any dial. Use guard.Prepare
	// (the canonical pre-flight entry point, consistent with ext-http) rather
	// than guard.CheckScheme directly; Prepare is the extension hook if the
	// guard ever gains pre-flight mutation. The resolved-IP block and redirect
	// re-validation are enforced by the dialer Control/DialContext hooks.
	httpReq, err = p.guard.Prepare(httpReq)
	if err != nil {
		return nil, err
	}

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	// Bounded read — without a cap a cell-controlled URL pointing at a
	// gigabyte stream (or a Content-Length-less peer) buffers the whole body
	// in host memory and, ×maxConcurrency, OOMs the Pulp host. Past the cap
	// we surface an explicit error rather than silently truncating.
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, p.maxFetchBytes))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if int64(len(respBody)) == p.maxFetchBytes {
		var probe [1]byte
		if n, _ := resp.Body.Read(probe[:]); n > 0 {
			return nil, fmt.Errorf("response body exceeds %d bytes", p.maxFetchBytes)
		}
	}

	headers := map[string]string{}
	for k, vs := range resp.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	result := abi.HTTPResponse{
		Status:  uint32(resp.StatusCode),
		Headers: headers,
		Body:    respBody,
	}
	return abi.EncodeHTTPResponse(result)
}

// =====================================================================
// Capability lifecycle
// =====================================================================

// readPositiveIntEnv reads an env var as a positive int, falling back to def.
func readPositiveIntEnv(name string, def int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// readPositiveInt64Env reads an env var as a positive int64, falling back to
// def. Used for byte-size caps whose default exceeds a 32-bit int range.
func readPositiveInt64Env(name string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func workersSetup(env ext.SetupEnv) error {
	logger := env.Logger
	if logger == nil {
		logger = slog.Default()
	}
	maxConcurrency := readPositiveIntEnv("PULP_WORKERS_MAX_CONCURRENCY", defaultMaxConcurrency)
	maxQueued := readPositiveIntEnv("PULP_WORKERS_MAX_QUEUED", defaultMaxQueued)
	// Accept the new canonical name first; fall back to the legacy singular
	// spelling ("PULP_WORKER_MAX_PER_CELL") so existing deployments aren't
	// silently broken on upgrade.
	maxPerCell := readPositiveIntEnv("PULP_WORKERS_MAX_PER_CELL", 0)
	if maxPerCell == 0 {
		maxPerCell = readPositiveIntEnv("PULP_WORKER_MAX_PER_CELL", defaultMaxPerCell)
	}
	maxFetchBytes := readPositiveInt64Env("PULP_WORKERS_MAX_FETCH_BYTES", defaultMaxFetchBytes)
	pool = newWorkerPool(logger, maxConcurrency, maxQueued, maxPerCell, maxFetchBytes)
	logger.Info("workers extension initialized",
		"max_concurrency", maxConcurrency,
		"max_queued", maxQueued,
		"max_per_cell", maxPerCell,
		"max_fetch_bytes", maxFetchBytes,
	)
	return nil
}

func workersTeardown(_ context.Context) error {
	if pool != nil {
		pool.teardown()
	}
	return nil
}

// workersTeardownCell drops per-cell state when the control socket
// shuts down a single cell. Cancels that cell's in-flight tasks and
// purges any completed results the cell never polled.
func workersTeardownCell(_ context.Context, cellID string) error {
	if pool == nil {
		return nil
	}
	cancelled, dropped := pool.teardownCell(cellID)
	if cancelled > 0 || dropped > 0 {
		pool.logger.Info("workers teardown_cell",
			"cell", cellID,
			"cancelled", cancelled,
			"dropped_results", dropped,
		)
	}
	return nil
}

// =====================================================================
// Host import bindings
// =====================================================================

func workersRegister(b wazero.HostModuleBuilder, cell ext.Cell) error {
	// Capture cell identity in the closure so per-cell teardown can
	// cancel only this cell's tasks without disturbing others.
	cellID := ""
	if cell != nil {
		cellID = cell.Name()
	}

	// workers_submit(req_ptr, req_len) -> task_id_or_code:uint32
	// On success returns the task id (>0). On failure returns a host error code
	// (1=empty, 2=mem read, 3=decode, 15=queue full). Callers disambiguate by
	// checking whether the returned value corresponds to a submitted task.
	b.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
			if reqLen == 0 {
				return codeEmptyReq
			}
			data, ok := m.Memory().Read(reqPtr, reqLen)
			if !ok {
				return codeMemRead
			}
			var req taskRequest
			if err := msgpack.Unmarshal(data, &req); err != nil {
				return codeDecode
			}
			id, code := pool.submit(cellID, req)
			if code != codeOK {
				return code
			}
			return id
		}).
		Export("workers_submit")

	// workers_submit_fire(req_ptr, req_len) -> error_code:uint32
	b.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
			if reqLen == 0 {
				return codeEmptyReq
			}
			data, ok := m.Memory().Read(reqPtr, reqLen)
			if !ok {
				return codeMemRead
			}
			var req taskRequest
			if err := msgpack.Unmarshal(data, &req); err != nil {
				return codeDecode
			}
			return pool.submitFire(cellID, req)
		}).
		Export("workers_submit_fire")

	// workers_result(task_id, result_ptr_out, result_len_out) -> status:uint32
	b.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, taskID, resultPtrOut, resultLenOut uint32) uint32 {
			data, status := pool.result(cellID, taskID)
			if status == statusPending || status == statusUnknown {
				return status
			}

			// Write data into WASM memory via pulp_alloc.
			if len(data) == 0 {
				if !m.Memory().WriteUint32Le(resultPtrOut, 0) {
					return status
				}
				if !m.Memory().WriteUint32Le(resultLenOut, 0) {
					return status
				}
				return status
			}

			allocFn := m.ExportedFunction("pulp_alloc")
			if allocFn == nil {
				return status
			}
			results, err := allocFn.Call(ctx, uint64(len(data)))
			if err != nil || len(results) == 0 {
				return status
			}
			ptr := uint32(results[0])
			if ptr == 0 {
				return status
			}
			if !m.Memory().Write(ptr, data) {
				return status
			}
			if !m.Memory().WriteUint32Le(resultPtrOut, ptr) {
				return status
			}
			if !m.Memory().WriteUint32Le(resultLenOut, uint32(len(data))) {
				return status
			}
			return status
		}).
		Export("workers_result")

	// workers_cancel(task_id) -> error_code:uint32
	b.NewFunctionBuilder().
		WithFunc(func(_ context.Context, _ api.Module, taskID uint32) uint32 {
			return pool.cancel(cellID, taskID)
		}).
		Export("workers_cancel")

	// workers_pending() -> count:uint32
	b.NewFunctionBuilder().
		WithFunc(func(_ context.Context, _ api.Module) uint32 {
			return pool.pending()
		}).
		Export("workers_pending")

	return nil
}

// =====================================================================
// Stub bindings (when capability is not active)
// =====================================================================

func workersStub(b wazero.HostModuleBuilder, _ ext.Cell) error {
	b.NewFunctionBuilder().
		WithFunc(func(_ context.Context, _ api.Module, _, _ uint32) uint32 { return codeCapAbsent }).
		Export("workers_submit")
	b.NewFunctionBuilder().
		WithFunc(func(_ context.Context, _ api.Module, _, _ uint32) uint32 { return codeCapAbsent }).
		Export("workers_submit_fire")
	b.NewFunctionBuilder().
		WithFunc(func(_ context.Context, _ api.Module, _, _, _ uint32) uint32 { return statusUnknown }).
		Export("workers_result")
	b.NewFunctionBuilder().
		WithFunc(func(_ context.Context, _ api.Module, _ uint32) uint32 { return 1 }).
		Export("workers_cancel")
	b.NewFunctionBuilder().
		WithFunc(func(_ context.Context, _ api.Module) uint32 { return 0 }).
		Export("workers_pending")
	return nil
}
