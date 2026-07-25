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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
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
	defaultFetchTimeout   = 30 * time.Second
	maxFetchTimeout       = 300 * time.Second // upper bound when no timeout_ms is set
	resultTTL             = 5 * time.Minute
	teardownGrace         = 5 * time.Second
	defaultMaxConcurrency = 32
	defaultMaxQueued      = 1024
	defaultMaxPerCell     = 8
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
	codeOK         = 0
	codeEmptyReq   = 1
	codeMemRead    = 2
	codeDecode     = 3
	codeFireFailed = 4
	codeQueueFull  = 15
	codeCellFull   = 16
	codeSaturated  = 17
	// codeIdempotencyConflict means a scope reused a key for different work.
	codeIdempotencyConflict = 18
	codeCapAbsent           = 99
)

// ErrWorkerSaturated is returned when the concurrency semaphore is full and
// the submit call would block the WASM host import indefinitely.
var ErrWorkerSaturated = errors.New("worker pool saturated: all concurrency slots in use")

// ---------------------------------------------------------------------
// Host-shared module state
// ---------------------------------------------------------------------

// workersHost owns the one process-level worker implementation. Pulp calls a
// capability's Setup and Teardown once per application, but Teardown carries
// no application scope; replacing or stopping a pool there would let one
// application kill another application's jobs. Mutable task state inside the
// pool is already keyed by ext.Scope, so sharing the implementation is safe.
var workersHost = struct {
	mu       sync.RWMutex
	pool     *workerPool
	owners   map[ext.Scope]struct{}
	runtimes map[workerApplicationKey]workerApplicationRuntime
}{owners: make(map[ext.Scope]struct{}), runtimes: make(map[workerApplicationKey]workerApplicationRuntime)}

type workerApplicationKey struct {
	id       string
	instance string
}

type workerApplicationRuntime struct {
	storageRoot string
}

func applicationKey(scope ext.Scope) workerApplicationKey {
	return workerApplicationKey{id: scope.ApplicationID(), instance: scope.ApplicationInstanceID()}
}

func sharedWorkerPool() *workerPool {
	workersHost.mu.RLock()
	p := workersHost.pool
	workersHost.mu.RUnlock()
	return p
}

func setupSharedWorkerPool(env ext.SetupEnv, newPool func() *workerPool) (p *workerPool, created bool) {
	scope := env.EffectiveScope()
	workersHost.mu.Lock()
	defer workersHost.mu.Unlock()
	if workersHost.pool == nil {
		workersHost.pool = newPool()
		created = true
	}
	workersHost.owners[scope] = struct{}{}
	workersHost.runtimes[applicationKey(scope)] = workerApplicationRuntime{storageRoot: env.StorageRoot}
	return workersHost.pool, created
}

func teardownSharedWorkerPool(scope ext.Scope) (p *workerPool, lastOwner bool, owned bool) {
	workersHost.mu.Lock()
	p = workersHost.pool
	if _, owned = workersHost.owners[scope]; !owned {
		workersHost.mu.Unlock()
		return p, false, false
	}
	delete(workersHost.owners, scope)
	delete(workersHost.runtimes, applicationKey(scope))
	lastOwner = len(workersHost.owners) == 0
	if lastOwner {
		// Detach before stopping so a concurrent new application setup receives
		// a fresh pool instead of attaching work to a pool being torn down.
		workersHost.pool = nil
	}
	workersHost.mu.Unlock()
	return p, lastOwner, true
}

func workersStorageRoot(scope ext.Scope) (string, bool) {
	workersHost.mu.RLock()
	runtime, ok := workersHost.runtimes[applicationKey(scope)]
	workersHost.mu.RUnlock()
	return runtime.storageRoot, ok && strings.TrimSpace(runtime.storageRoot) != ""
}

// ---------------------------------------------------------------------
// init — register the workers capability
// ---------------------------------------------------------------------

func init() {
	ext.Register(ext.Capability{
		Name:          "workers",
		Register:      workersRegister,
		Stub:          workersStub,
		Setup:         workersSetup,
		TeardownScope: workersTeardownScope,
		TeardownCell:  workersTeardownCell,
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
	// IdempotencyKey makes a retry of the same request return its original
	// task ID while that task is in flight or awaiting its result. It is scoped
	// to the application/cell instance that submitted it, never process-wide.
	// Empty retains the legacy submit-on-every-call behaviour.
	IdempotencyKey string `msgpack:"idempotency_key,omitempty"`
}

func scopeLogAttrs(scope ext.Scope) []any {
	return []any{
		"application", scope.ApplicationID(),
		"application_instance", scope.ApplicationInstanceID(),
		"cell", scope.CellID(),
		"cell_instance", scope.CellInstanceID(),
	}
}

type idempotencyEntry struct {
	id          uint32
	fingerprint [sha256.Size]byte
	// completed is zero while the task is still running. Once set, data/status
	// retain the terminal outcome for resultTTL so a retry after the original
	// caller consumed its result cannot accidentally repeat an effect.
	completed time.Time
	data      []byte
	status    uint32
}

type scopedIdempotencyKey struct {
	scope ext.Scope
	key   string
}

func requestFingerprint(req taskRequest) [sha256.Size]byte {
	// A deterministic fingerprint rejects accidental key reuse for different
	// effects. Header maps are sorted so equivalent requests hash identically.
	h := sha256.New()
	_, _ = h.Write([]byte(req.Type))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.Method))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.URL))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(req.Body)
	_, _ = h.Write([]byte{0})
	var timeout [4]byte
	timeout[0] = byte(req.TimeoutMs >> 24)
	timeout[1] = byte(req.TimeoutMs >> 16)
	timeout[2] = byte(req.TimeoutMs >> 8)
	timeout[3] = byte(req.TimeoutMs)
	_, _ = h.Write(timeout[:])
	keys := make([]string, 0, len(req.Headers))
	for key := range req.Headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(req.Headers[key]))
	}
	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

// ---------------------------------------------------------------------
// Task result stored after completion
// ---------------------------------------------------------------------

type taskResult struct {
	data           []byte // payload: msgpack abi.HTTPResponse on complete; raw error string on error/panic
	status         uint32 // statusComplete, statusError, or statusPanic
	completed      time.Time
	scope          ext.Scope // owning application/instance/cell
	idempotencyKey string
}

// ---------------------------------------------------------------------
// In-flight task
// ---------------------------------------------------------------------

type inflightTask struct {
	cancel         context.CancelFunc
	done           chan struct{}
	scope          ext.Scope // owning application/instance/cell
	idempotencyKey string
}

// ---------------------------------------------------------------------
// Per-scope task tracker
// ---------------------------------------------------------------------

// scopeTracker enforces a per-cell-instance ceiling on concurrent tasks so
// one misbehaving cell cannot consume all global worker slots. The complete
// application/instance/cell tuple matters: names such as "api" may repeat in
// different Pulp applications sharing this process.
type scopeTracker struct {
	mu         sync.Mutex
	inflight   map[ext.Scope]int // scope -> count of active tasks (submit + submitFire)
	maxPerCell int
}

func newScopeTracker(maxPerCell int) *scopeTracker {
	return &scopeTracker{
		inflight:   make(map[ext.Scope]int),
		maxPerCell: maxPerCell,
	}
}

// acquire increments a scope's counter if under the limit.
// Returns true if the slot was acquired, false if the scoped cell is full.
func (ct *scopeTracker) acquire(scope ext.Scope) bool {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if ct.inflight[scope] >= ct.maxPerCell {
		return false
	}
	ct.inflight[scope]++
	return true
}

// release decrements the cell's counter by one. Safe to call even if
// the counter is already zero (clamps to zero).
func (ct *scopeTracker) release(scope ext.Scope) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if ct.inflight[scope] > 0 {
		ct.inflight[scope]--
	}
	if ct.inflight[scope] == 0 {
		delete(ct.inflight, scope)
	}
}

// drop zeroes the scoped cell's counter. Used during per-cell teardown
// after all in-flight tasks have been cancelled.
func (ct *scopeTracker) drop(scope ext.Scope) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	delete(ct.inflight, scope)
}

// count returns the current inflight count for the scoped cell.
func (ct *scopeTracker) count(scope ext.Scope) int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return ct.inflight[scope]
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
	logger        *slog.Logger
	client        *http.Client
	guard         *ssrfguard.EgressGuard
	maxFetchBytes int64
	nextID        atomic.Uint32

	sem            chan struct{}
	maxConcurrency int
	maxQueued      int

	scopes *scopeTracker

	mu          sync.Mutex
	inflight    map[uint32]*inflightTask
	results     map[uint32]*taskResult
	idempotency map[scopedIdempotencyKey]idempotencyEntry
	// scopesByRoutingID lets TeardownCell accept Pulp's backwards-compatible
	// control identifier while still resolving a full scoped placement.
	scopesByRoutingID map[string]ext.Scope

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
		guard:             guard,
		maxFetchBytes:     maxFetchBytes,
		sem:               make(chan struct{}, maxConcurrency),
		maxConcurrency:    maxConcurrency,
		maxQueued:         maxQueued,
		scopes:            newScopeTracker(maxPerCell),
		inflight:          make(map[uint32]*inflightTask),
		results:           make(map[uint32]*taskResult),
		idempotency:       make(map[scopedIdempotencyKey]idempotencyEntry),
		scopesByRoutingID: make(map[string]ext.Scope),
		cleanupDone:       make(chan struct{}),
		cleanupStop:       cancel,
	}
	go p.cleanupLoop(ctx)
	return p
}

func (p *workerPool) registerScope(scope ext.Scope) {
	p.mu.Lock()
	p.scopesByRoutingID[scope.RoutingID()] = scope
	// Legacy Pulp control paths still send Cell.Name(). New scoped cells use
	// the injective RoutingID so equal names in sibling applications remain
	// unambiguous.
	if scope == ext.LegacyScope(scope.CellID()) {
		p.scopesByRoutingID[scope.CellID()] = scope
	}
	p.mu.Unlock()
}

func (p *workerPool) scopeForTeardown(cellID string) ext.Scope {
	p.mu.Lock()
	scope, ok := p.scopesByRoutingID[cellID]
	p.mu.Unlock()
	if ok {
		return scope
	}
	return ext.LegacyScope(cellID)
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
// scope identifies the owning application/instance/cell for teardown,
// scheduling quotas, results, and idempotency. ext.LegacyScope preserves the
// existing single-app cell-name behavior for callers that need it.
func (p *workerPool) submit(scope ext.Scope, req taskRequest) (uint32, uint32) {
	fingerprint := requestFingerprint(req)
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)

	p.mu.Lock()
	if idempotencyKey != "" {
		key := scopedIdempotencyKey{scope: scope, key: idempotencyKey}
		if existing, ok := p.idempotency[key]; ok {
			if existing.fingerprint != fingerprint {
				p.mu.Unlock()
				return 0, codeIdempotencyConflict
			}
			if !existing.completed.IsZero() {
				// workers_result may already have consumed the original result.
				// Rehydrate it from the scoped idempotency receipt for this retry.
				p.results[existing.id] = &taskResult{
					data:           append([]byte(nil), existing.data...),
					status:         existing.status,
					completed:      existing.completed,
					scope:          scope,
					idempotencyKey: idempotencyKey,
				}
			}
			p.mu.Unlock()
			return existing.id, codeOK
		}
	}
	if len(p.inflight) >= p.maxQueued {
		p.mu.Unlock()
		return 0, codeQueueFull
	}
	if !p.scopes.acquire(scope) {
		p.mu.Unlock()
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
	task := &inflightTask{cancel: cancel, done: make(chan struct{}), scope: scope, idempotencyKey: idempotencyKey}
	p.inflight[id] = task
	if idempotencyKey != "" {
		p.idempotency[scopedIdempotencyKey{scope: scope, key: idempotencyKey}] = idempotencyEntry{id: id, fingerprint: fingerprint}
	}
	p.mu.Unlock()

	select {
	case p.sem <- struct{}{}:
	default:
		// All concurrency slots are occupied. Reject immediately so the
		// WASM host import returns rather than blocking the caller indefinitely.
		p.mu.Lock()
		delete(p.inflight, id)
		if idempotencyKey != "" {
			delete(p.idempotency, scopedIdempotencyKey{scope: scope, key: idempotencyKey})
		}
		p.mu.Unlock()
		task.cancel()
		p.scopes.release(scope)
		return 0, codeSaturated
	}
	go func() {
		defer func() {
			<-p.sem
			p.scopes.release(scope)
			close(task.done)
		}()
		defer func() {
			if r := recover(); r != nil {
				p.logger.Error("worker task panicked", append([]any{"id", id, "type", req.Type, "panic", r}, scopeLogAttrs(scope)...)...)
				p.storeResult(id, scope, idempotencyKey, []byte(fmt.Sprintf("panic: %v", r)), statusPanic)
			}
		}()

		data, err := p.runTask(ctx, req)

		if err != nil {
			p.storeResult(id, scope, idempotencyKey, []byte(err.Error()), statusError)
		} else {
			p.storeResult(id, scope, idempotencyKey, data, statusComplete)
		}
	}()

	return id, codeOK
}

func (p *workerPool) storeResult(id uint32, scope ext.Scope, idempotencyKey string, data []byte, status uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.inflight, id)
	completed := time.Now()
	p.results[id] = &taskResult{
		data:           data,
		status:         status,
		completed:      completed,
		scope:          scope,
		idempotencyKey: idempotencyKey,
	}
	if idempotencyKey != "" {
		key := scopedIdempotencyKey{scope: scope, key: idempotencyKey}
		if receipt, ok := p.idempotency[key]; ok && receipt.id == id {
			receipt.completed = completed
			receipt.data = append([]byte(nil), data...)
			receipt.status = status
			p.idempotency[key] = receipt
		}
	}
}

// submitFire runs a task without tracking results. Returns host code.
func (p *workerPool) submitFire(scope ext.Scope, req taskRequest) uint32 {
	if p.inflightCount() >= p.maxQueued {
		return codeQueueFull
	}
	if !p.scopes.acquire(scope) {
		return codeCellFull
	}

	select {
	case p.sem <- struct{}{}:
	default:
		p.scopes.release(scope)
		return codeSaturated
	}
	go func() {
		defer func() {
			<-p.sem
			p.scopes.release(scope)
		}()
		defer func() {
			if r := recover(); r != nil {
				p.logger.Error("fire-and-forget task panicked", append([]any{"type", req.Type, "panic", r}, scopeLogAttrs(scope)...)...)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), maxFetchTimeout)
		defer cancel()
		if _, err := p.runTask(ctx, req); err != nil {
			p.logger.Warn("fire-and-forget task failed", append([]any{"type", req.Type, "err", err}, scopeLogAttrs(scope)...)...)
		}
	}()
	return codeOK
}

// result polls for a completed task. Returns (data, status). On
// statusComplete the data is a msgpack-encoded abi.HTTPResponse. On
// statusError or statusPanic the data is a raw UTF-8 error string —
// the cell-side wrapper surfaces it via TaskResult.Error.
//
// scope scopes ownership: an application/cell instance may only poll its OWN
// tasks. Task IDs are
// a global, sequential, enumerable counter, so without this check a hostile
// sibling could sweep IDs and steal (and delete) another cell's result. A
// mismatch is reported as statusUnknown — indistinguishable from a
// never-submitted ID, leaking nothing. Legacy single-cell deployments retain
// their stable legacy/default/<cell>/default scope.
// result peeks at the completed result for id without removing it from the
// map. The caller must call consume(scope, id) after successfully writing the data
// into WASM memory so that a failed alloc never silently drops the result.
func (p *workerPool) result(scope ext.Scope, id uint32) ([]byte, uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if r, ok := p.results[id]; ok {
		if r.scope != scope {
			return nil, statusUnknown
		}
		return r.data, r.status
	}
	if t, ok := p.inflight[id]; ok {
		if t.scope != scope {
			return nil, statusUnknown
		}
		return nil, statusPending
	}
	return nil, statusUnknown
}

// consume removes the completed result for id from the map. Must be called
// only after the WASM memory write succeeds; leaving the result in the map
// until then lets the cell retry if alloc fails.
func (p *workerPool) consume(scope ext.Scope, id uint32) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.results[id]
	if !ok || r.scope != scope {
		return false
	}
	delete(p.results, id)
	return true
}

// cancel attempts to cancel an in-flight task owned by scope. A cell may
// only cancel its OWN tasks — a cross-cell cancel would be a DoS against a
// sibling's in-flight work. A mismatch (or missing id) returns 1 (not
// found), same as an already-done task.
func (p *workerPool) cancel(scope ext.Scope, id uint32) uint32 {
	p.mu.Lock()
	task, ok := p.inflight[id]
	if ok && task.scope != scope {
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

func sameApplication(scope, application ext.Scope) bool {
	return scope.ApplicationID() == application.ApplicationID() &&
		scope.ApplicationInstanceID() == application.ApplicationInstanceID()
}

// teardownApplication releases all cell-instance records that belong to one
// application setup scope. The underlying semaphore and transport stay alive
// while another application still owns the shared pool.
func (p *workerPool) teardownApplication(application ext.Scope) (int, int) {
	p.mu.Lock()
	scopes := make(map[ext.Scope]struct{})
	for _, task := range p.inflight {
		if sameApplication(task.scope, application) {
			scopes[task.scope] = struct{}{}
		}
	}
	for _, result := range p.results {
		if sameApplication(result.scope, application) {
			scopes[result.scope] = struct{}{}
		}
	}
	for key := range p.idempotency {
		if sameApplication(key.scope, application) {
			scopes[key.scope] = struct{}{}
		}
	}
	for routingID, scope := range p.scopesByRoutingID {
		if sameApplication(scope, application) {
			scopes[scope] = struct{}{}
			delete(p.scopesByRoutingID, routingID)
		}
	}
	p.mu.Unlock()

	cancelled, dropped := 0, 0
	for scope := range scopes {
		c, d := p.teardownScope(scope)
		cancelled += c
		dropped += d
	}
	return cancelled, dropped
}

// teardownScope cancels in-flight tasks owned by scope, waits up to
// teardownGrace for them to exit, and drops any completed-but-unpolled
// results belonging to that cell. Other cells' state is left untouched.
// Returns (cancelled, results) counts for logging.
func (p *workerPool) teardownScope(scope ext.Scope) (int, int) {
	p.mu.Lock()
	tasks := make([]*inflightTask, 0)
	for _, t := range p.inflight {
		if t.scope == scope {
			tasks = append(tasks, t)
		}
	}
	resultsDropped := 0
	for id, r := range p.results {
		if r.scope == scope {
			delete(p.results, id)
			resultsDropped++
		}
	}
	for key := range p.idempotency {
		if key.scope == scope {
			delete(p.idempotency, key)
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
			p.scopes.drop(scope)
			return len(tasks), resultsDropped
		}
	}

	// All tasks exited cleanly; their defers already called release().
	// Drop any residual counter (shouldn't be needed, but defensive).
	p.scopes.drop(scope)
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
			for key, receipt := range p.idempotency {
				if !receipt.completed.IsZero() && now.Sub(receipt.completed) > resultTTL {
					delete(p.idempotency, key)
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
	_, created := setupSharedWorkerPool(env, func() *workerPool {
		return newWorkerPool(logger, maxConcurrency, maxQueued, maxPerCell, maxFetchBytes)
	})
	if created {
		logger.Info("workers extension initialized",
			"max_concurrency", maxConcurrency,
			"max_queued", maxQueued,
			"max_per_cell", maxPerCell,
			"max_fetch_bytes", maxFetchBytes,
		)
	} else {
		logger.Info("workers extension attached application scope",
			append(scopeLogAttrs(env.EffectiveScope()), "shared_pool", true)...,
		)
	}
	return nil
}

func workersTeardownScope(_ context.Context, scope ext.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("workers: teardown scope: %w", err)
	}
	p, lastOwner, owned := teardownSharedWorkerPool(scope)
	if !owned || p == nil {
		return nil
	}
	cancelled, dropped := p.teardownApplication(scope)
	if lastOwner {
		p.teardown()
	}
	if cancelled > 0 || dropped > 0 {
		p.logger.Info("workers teardown application",
			append(scopeLogAttrs(scope), "cancelled", cancelled, "dropped_results", dropped, "last_owner", lastOwner)...,
		)
	}
	return nil
}

// workersTeardownCell drops per-cell state when the control socket
// shuts down a single cell. Cancels that cell's in-flight tasks and
// purges any completed results the cell never polled.
func workersTeardownCell(_ context.Context, cellID string) error {
	p := sharedWorkerPool()
	if p == nil {
		return nil
	}
	scope := p.scopeForTeardown(cellID)
	cancelled, dropped := p.teardownScope(scope)
	if cancelled > 0 || dropped > 0 {
		p.logger.Info("workers teardown_cell",
			"cell", cellID,
			"routing_id", scope.RoutingID(),
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
	// Capture an immutable full placement scope in every import closure. The
	// extension binary and worker pool are shared, while work ownership is not.
	scope := ext.ScopeOf(cell)
	p := sharedWorkerPool()
	if p == nil {
		return workersStub(b, cell)
	}
	p.registerScope(scope)

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
			id, code := p.submit(scope, req)
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
			return p.submitFire(scope, req)
		}).
		Export("workers_submit_fire")

	// workers_result(task_id, result_ptr_out, result_len_out) -> status:uint32
	b.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, taskID, resultPtrOut, resultLenOut uint32) uint32 {
			data, status := p.result(scope, taskID)
			if status == statusPending || status == statusUnknown {
				return status
			}

			// Write data into WASM memory via pulp_alloc.
			if len(data) == 0 {
				// Nothing to write; consume before returning so the slot is freed.
				p.consume(scope, taskID)
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
				// Alloc unavailable; leave result in map so the cell can retry.
				return status
			}
			results, err := allocFn.Call(ctx, uint64(len(data)))
			if err != nil || len(results) == 0 {
				// Alloc failed; leave result in map so the cell can retry.
				return status
			}
			ptr := uint32(results[0])
			if ptr == 0 {
				// Alloc returned null; leave result in map so the cell can retry.
				return status
			}
			if !m.Memory().Write(ptr, data) {
				// Write failed; leave result in map so the cell can retry.
				return status
			}
			// Write succeeded — now safe to consume the result.
			p.consume(scope, taskID)
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
			return p.cancel(scope, taskID)
		}).
		Export("workers_cancel")

	// workers_pending() -> count:uint32
	b.NewFunctionBuilder().
		WithFunc(func(_ context.Context, _ api.Module) uint32 {
			return p.pending()
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
