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
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BananaLabs-OSS/Pulp/abi"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

// ---------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------

const (
	defaultFetchTimeout    = 30 * time.Second
	resultTTL              = 5 * time.Minute
	teardownGrace          = 5 * time.Second
	defaultMaxConcurrency  = 32
	defaultMaxQueued       = 1024
)

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
	codeCapAbsent   = 99
)

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
// Worker pool
// ---------------------------------------------------------------------

type workerPool struct {
	logger *slog.Logger
	client *http.Client
	nextID atomic.Uint32

	sem            chan struct{}
	maxConcurrency int
	maxQueued      int

	mu       sync.Mutex
	inflight map[uint32]*inflightTask
	results  map[uint32]*taskResult

	// Background cleanup
	cleanupDone chan struct{}
	cleanupStop context.CancelFunc
}

func newWorkerPool(logger *slog.Logger, maxConcurrency, maxQueued int) *workerPool {
	ctx, cancel := context.WithCancel(context.Background())
	// Reuse one transport with a real keep-alive pool. Default
	// http.Client builds a fresh transport with tiny idle-conn limits —
	// every Bananagine/Resend call pays a TCP (and TLS for Resend)
	// handshake. A per-host idle pool collapses that to one handshake
	// per host across the process lifetime.
	transport := &http.Transport{
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   32,
		MaxConnsPerHost:       64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	p := &workerPool{
		logger:         logger,
		client:         &http.Client{Timeout: defaultFetchTimeout, Transport: transport},
		sem:            make(chan struct{}, maxConcurrency),
		maxConcurrency: maxConcurrency,
		maxQueued:      maxQueued,
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

	p.sem <- struct{}{}
	go func() {
		defer func() {
			<-p.sem
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

	p.sem <- struct{}{}
	go func() {
		defer func() { <-p.sem }()
		defer func() {
			if r := recover(); r != nil {
				p.logger.Error("fire-and-forget task panicked", "cell", cellID, "type", req.Type, "panic", r)
			}
		}()

		timeout := defaultFetchTimeout
		if req.TimeoutMs > 0 {
			timeout = time.Duration(req.TimeoutMs) * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
func (p *workerPool) result(id uint32) ([]byte, uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if r, ok := p.results[id]; ok {
		// Only consume the result on terminal statuses — keeps polling
		// idempotent for any non-terminal snapshot that somehow lands
		// here (shouldn't happen today, but cheap insurance).
		if r.status == statusComplete || r.status == statusError || r.status == statusPanic {
			delete(p.results, id)
		}
		return r.data, r.status
	}
	if _, ok := p.inflight[id]; ok {
		return nil, statusPending
	}
	return nil, statusUnknown
}

// cancel attempts to cancel an in-flight task.
func (p *workerPool) cancel(id uint32) uint32 {
	p.mu.Lock()
	task, ok := p.inflight[id]
	p.mu.Unlock()
	if !ok {
		return 1 // not found or already done
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
			return len(tasks), resultsDropped
		}
	}
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
	if req.TimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMs)*time.Millisecond)
		defer cancel()
	}
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

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
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

func workersSetup(env ext.SetupEnv) error {
	logger := env.Logger
	if logger == nil {
		logger = slog.Default()
	}
	maxConcurrency := readPositiveIntEnv("PULP_WORKERS_MAX_CONCURRENCY", defaultMaxConcurrency)
	maxQueued := readPositiveIntEnv("PULP_WORKERS_MAX_QUEUED", defaultMaxQueued)
	pool = newWorkerPool(logger, maxConcurrency, maxQueued)
	logger.Info("workers extension initialized",
		"max_concurrency", maxConcurrency,
		"max_queued", maxQueued,
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
			data, status := pool.result(taskID)
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
			return pool.cancel(taskID)
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
