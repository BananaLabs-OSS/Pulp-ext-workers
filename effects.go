package workersext

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/vmihailenco/msgpack/v5"
)

// EffectKindNotificationEmail is retained as a source-compatible alias. New
// callers should use effect.KindNotificationEmailSend directly.
const EffectKindNotificationEmail = effect.KindNotificationEmailSend

var (
	ErrEffectInvalid  = errors.New("workers: invalid host effect")
	ErrEffectConflict = errors.New("workers: host effect idempotency conflict")
	ErrEffectNotFound = errors.New("workers: host effect receipt not found")
)

// These aliases keep the initial executor API readable while making Fiber's
// versioned canonical wire contract the only persisted representation.
type EffectRequest = effect.Intent
type EffectStatus = effect.Status

const (
	EffectPending   = effect.Pending
	EffectCompleted = effect.Completed
	EffectFailed    = effect.Failed
)

// EffectReceipt binds Fiber's canonical receipt to its Pulp placement and
// persistence bookkeeping. Receipt is embedded so outbox code can read
// Pending/Completed/Failed, Result, and Failure directly while the canonical
// wire envelope remains available for persistence and transport.
type EffectReceipt struct {
	Scope  ext.Scope
	Intent effect.Intent
	effect.Receipt
	Fingerprint [sha256.Size]byte
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// EffectStore persists placement-scoped canonical receipts. A production
// implementation should durably store every Put before returning;
// MemoryEffectStore exists for tests and local hosts only.
type EffectStore interface {
	Get(ctx context.Context, scope ext.Scope, idempotencyKey string) (EffectReceipt, bool, error)
	Put(ctx context.Context, receipt EffectReceipt) error
}

// FencedEffectStore is the additive multi-process protocol used by durable
// production stores. Claim grants one expiring execution lease; Finish accepts
// a terminal receipt only from the current fence. File and memory stores keep
// the original single-process interface for local compatibility.
type FencedEffectStore interface {
	EffectStore
	Claim(ctx context.Context, scope ext.Scope, idempotencyKey string, fingerprint [sha256.Size]byte, claimant string, now time.Time, leaseDuration time.Duration) (fence uint64, claimed bool, err error)
	Finish(ctx context.Context, receipt EffectReceipt, claimant string, fence uint64) (settled bool, err error)
}

// EffectHandler owns the privileged implementation (for example, email
// delivery). Result must be a kind-owned MessagePack value. A handler should
// return a stable Failure for an expected provider error; an ordinary error is
// reduced to a generic non-secret failure before it reaches the state owner.
type EffectHandler func(ctx context.Context, intent effect.Intent) (result msgpack.RawMessage, failure *effect.Failure, err error)

// effectIntentValidator narrows one executor to its host-owned effect family.
// It is intentionally internal plumbing: a capability supplies a fixed
// validator when it constructs an executor, never guest-selected policy.
type effectIntentValidator func(scope ext.Scope, intent *effect.Intent) error

// EffectWorker is the scoped worker queue boundary. It accepts a closure so
// the worker extension stays independent of email providers and tests can use
// a deterministic fake queue.
type EffectWorker interface {
	Submit(ctx context.Context, scope ext.Scope, run func(context.Context)) error
}

// EffectExecutor persists and schedules one effect per (scope,
// idempotency_key). Equal keys in different applications, application
// instances, cells, or cell instances are independent. Submit accepts only a
// canonical Fiber v1 Intent; SubmitWire is the explicit compatibility path
// that decodes and normalizes supported legacy aliases before persistence.
type EffectExecutor struct {
	store     EffectStore
	worker    EffectWorker
	handler   EffectHandler
	validate  effectIntentValidator
	claimant  string
	scheduled map[scopedIdempotencyKey]struct{}
	mu        sync.Mutex
}

func NewEffectExecutor(store EffectStore, worker EffectWorker, handler EffectHandler) (*EffectExecutor, error) {
	return newValidatedEffectExecutor(store, worker, handler, normalizeAndValidateEffect)
}

func newValidatedEffectExecutor(store EffectStore, worker EffectWorker, handler EffectHandler, validate effectIntentValidator) (*EffectExecutor, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrEffectInvalid)
	}
	if worker == nil {
		return nil, fmt.Errorf("%w: worker queue is required", ErrEffectInvalid)
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrEffectInvalid)
	}
	if validate == nil {
		return nil, fmt.Errorf("%w: intent validator is required", ErrEffectInvalid)
	}
	claimantBytes := make([]byte, 16)
	if _, err := rand.Read(claimantBytes); err != nil {
		return nil, fmt.Errorf("%w: create worker identity: %v", ErrEffectInvalid, err)
	}
	return &EffectExecutor{store: store, worker: worker, handler: handler, validate: validate, claimant: hex.EncodeToString(claimantBytes), scheduled: make(map[scopedIdempotencyKey]struct{})}, nil
}

// NewHostEffectExecutor binds an executor to the extension's already-created
// shared worker pool. It adds no provider and no WASM import: the handler is
// still the only privileged email/notification implementation.
func NewHostEffectExecutor(store EffectStore, handler EffectHandler) (*EffectExecutor, error) {
	p := sharedWorkerPool()
	if p == nil {
		return nil, errors.New("workers: extension is not initialized")
	}
	return NewEffectExecutor(store, pooledEffectWorker{pool: p}, handler)
}

// Submit persists a canonical pending receipt before queueing. Pending and
// completed duplicates replay the saved receipt without scheduling another
// delivery. Failed receipts are retried with the same stable key.
func (e *EffectExecutor) Submit(ctx context.Context, scope ext.Scope, intent effect.Intent) (EffectReceipt, error) {
	if e == nil {
		return EffectReceipt{}, fmt.Errorf("%w: executor is nil", ErrEffectInvalid)
	}
	if e.validate == nil {
		return EffectReceipt{}, fmt.Errorf("%w: intent validator is required", ErrEffectInvalid)
	}
	if err := e.validate(scope, &intent); err != nil {
		return EffectReceipt{}, err
	}
	fingerprint := effectFingerprint(intent)
	scheduledKey := scopedIdempotencyKey{scope: scope, key: intent.IdempotencyKey}

	e.mu.Lock()
	receipt, found, err := e.store.Get(ctx, scope, intent.IdempotencyKey)
	if err != nil {
		e.mu.Unlock()
		return EffectReceipt{}, fmt.Errorf("load host effect receipt: %w", err)
	}
	if found {
		if receipt.Scope != scope || receipt.Fingerprint != fingerprint || receipt.Intent.ID != intent.ID {
			e.mu.Unlock()
			return EffectReceipt{}, ErrEffectConflict
		}
		if receipt.Status == effect.Completed {
			e.mu.Unlock()
			return cloneReceipt(receipt), nil
		}
		if receipt.Status == effect.Pending {
			if _, active := e.scheduled[scheduledKey]; active {
				e.mu.Unlock()
				return cloneReceipt(receipt), nil
			}
			// A durable pending receipt without an in-process job was recovered
			// after restart. Requeue the same immutable intent and stable key.
		}
		if receipt.Status == effect.Failed {
			pending, err := effect.NewPendingReceipt(intent)
			if err != nil {
				e.mu.Unlock()
				return EffectReceipt{}, fmt.Errorf("build retry effect receipt: %w", err)
			}
			receipt.Intent = cloneIntent(intent)
			receipt.Receipt = pending
			receipt.UpdatedAt = time.Now().UTC()
		}
	} else {
		pending, err := effect.NewPendingReceipt(intent)
		if err != nil {
			e.mu.Unlock()
			return EffectReceipt{}, fmt.Errorf("build pending effect receipt: %w", err)
		}
		now := time.Now().UTC()
		receipt = EffectReceipt{
			Scope:       scope,
			Intent:      cloneIntent(intent),
			Receipt:     pending,
			Fingerprint: fingerprint,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
	}
	if err := e.store.Put(ctx, receipt); err != nil {
		e.mu.Unlock()
		return EffectReceipt{}, fmt.Errorf("persist pending host effect receipt: %w", err)
	}
	var fence uint64
	if store, ok := e.store.(FencedEffectStore); ok {
		fence, found, err = store.Claim(ctx, scope, intent.IdempotencyKey, fingerprint, e.claimant, time.Now().UTC(), 2*time.Minute)
		if err != nil {
			e.mu.Unlock()
			return EffectReceipt{}, fmt.Errorf("claim host effect receipt: %w", err)
		}
		if !found {
			e.mu.Unlock()
			return cloneReceipt(receipt), nil
		}
	}
	e.scheduled[scheduledKey] = struct{}{}
	e.mu.Unlock()

	if err := e.worker.Submit(ctx, scope, func(runCtx context.Context) {
		result, failure, runErr := e.handler(runCtx, cloneIntent(intent))
		e.finish(scope, intent, fingerprint, fence, result, failure, runErr)
	}); err != nil {
		e.finish(scope, intent, fingerprint, fence, nil, nil, err)
		failed, loadErr := e.Receipt(ctx, scope, intent.IdempotencyKey)
		if loadErr != nil {
			return EffectReceipt{}, fmt.Errorf("queue host effect: %w", err)
		}
		return failed, fmt.Errorf("queue host effect: %w", err)
	}
	return cloneReceipt(receipt), nil
}

// SubmitWire decodes a persisted or in-flight intent at the canonical Fiber
// wire boundary. effect.UnmarshalIntent is deliberately the only alias
// normalization path: direct Submit callers must construct a canonical v1
// Intent, while decoded legacy aliases converge to the v1 kind on replay.
func (e *EffectExecutor) SubmitWire(ctx context.Context, scope ext.Scope, wire []byte) (EffectReceipt, error) {
	intent, err := effect.UnmarshalIntent(wire)
	if err != nil {
		return EffectReceipt{}, fmt.Errorf("decode host effect intent: %w", err)
	}
	return e.Submit(ctx, scope, intent)
}

// Receipt returns the persisted result for outbox acknowledgement. It never
// falls back across scopes, so a key cannot reveal another application's work.
func (e *EffectExecutor) Receipt(ctx context.Context, scope ext.Scope, idempotencyKey string) (EffectReceipt, error) {
	if e == nil {
		return EffectReceipt{}, fmt.Errorf("%w: executor is nil", ErrEffectInvalid)
	}
	if err := scope.Validate(); err != nil {
		return EffectReceipt{}, fmt.Errorf("%w: scope: %v", ErrEffectInvalid, err)
	}
	if idempotencyKey == "" {
		return EffectReceipt{}, fmt.Errorf("%w: idempotency key is required", ErrEffectInvalid)
	}
	e.mu.Lock()
	receipt, found, err := e.store.Get(ctx, scope, idempotencyKey)
	e.mu.Unlock()
	if err != nil {
		return EffectReceipt{}, fmt.Errorf("load host effect receipt: %w", err)
	}
	if !found || receipt.Scope != scope {
		return EffectReceipt{}, ErrEffectNotFound
	}
	return cloneReceipt(receipt), nil
}

func (e *EffectExecutor) finish(scope ext.Scope, intent effect.Intent, fingerprint [sha256.Size]byte, fence uint64, result msgpack.RawMessage, failure *effect.Failure, runErr error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.scheduled, scopedIdempotencyKey{scope: scope, key: intent.IdempotencyKey})

	receipt, found, err := e.store.Get(context.Background(), scope, intent.IdempotencyKey)
	if err != nil || !found || receipt.Scope != scope || receipt.Fingerprint != fingerprint || receipt.Status != effect.Pending {
		return
	}
	if runErr == nil && failure == nil {
		receipt.Receipt, err = completedEffectReceipt(intent, result)
	} else {
		if failure == nil {
			failure = genericFailure("host_execution_failed", "host effect failed")
		}
		receipt.Receipt, err = effect.NewFailedReceipt(intent, *failure)
	}
	if err != nil {
		// An invalid provider result must not leave the outbox pending forever.
		receipt.Receipt, _ = effect.NewFailedReceipt(intent, *genericFailure("invalid_host_result", "host effect returned an invalid result"))
	}
	receipt.UpdatedAt = time.Now().UTC()
	if store, ok := e.store.(FencedEffectStore); ok {
		_, _ = store.Finish(context.Background(), receipt, e.claimant, fence)
		return
	}
	_ = e.store.Put(context.Background(), receipt)
}

func normalizeAndValidateEffect(scope ext.Scope, intent *effect.Intent) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: scope: %v", ErrEffectInvalid, err)
	}
	if intent == nil {
		return fmt.Errorf("%w: intent is required", ErrEffectInvalid)
	}
	if err := intent.Validate(); err != nil {
		return fmt.Errorf("%w: intent: %v", ErrEffectInvalid, err)
	}
	if intent.Kind != effect.KindNotificationEmailSend {
		return fmt.Errorf("%w: unsupported host effect kind %q", ErrEffectInvalid, intent.Kind)
	}
	return nil
}

func effectFingerprint(intent effect.Intent) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(intent.Version))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(intent.ID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(intent.Kind))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(intent.Payload)
	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func completedEffectReceipt(intent effect.Intent, result msgpack.RawMessage) (effect.Receipt, error) {
	receipt := effect.Receipt{
		Version: effect.VersionV1, IntentID: intent.ID, Kind: intent.Kind,
		IdempotencyKey: intent.IdempotencyKey, Status: effect.Completed,
		Result: append(msgpack.RawMessage(nil), result...),
	}
	return receipt, receipt.ValidateFor(intent)
}

func genericFailure(code, message string) *effect.Failure {
	return &effect.Failure{Code: code, Message: message}
}

func cloneIntent(intent effect.Intent) effect.Intent {
	intent.Payload = append(msgpack.RawMessage(nil), intent.Payload...)
	return intent
}

func cloneReceipt(receipt EffectReceipt) EffectReceipt {
	receipt.Intent = cloneIntent(receipt.Intent)
	receipt.Receipt = cloneCanonicalReceipt(receipt.Receipt)
	return receipt
}

func cloneCanonicalReceipt(receipt effect.Receipt) effect.Receipt {
	receipt.Result = append(msgpack.RawMessage(nil), receipt.Result...)
	if receipt.Failure != nil {
		failure := *receipt.Failure
		receipt.Failure = &failure
	}
	return receipt
}

type pooledEffectWorker struct{ pool *workerPool }

func (w pooledEffectWorker) Submit(ctx context.Context, scope ext.Scope, run func(context.Context)) error {
	if w.pool == nil {
		return errors.New("workers: extension is not initialized")
	}
	if !w.pool.scopes.acquire(scope) {
		return errors.New("workers: scoped queue full")
	}
	select {
	case w.pool.sem <- struct{}{}:
	case <-ctx.Done():
		w.pool.scopes.release(scope)
		return ctx.Err()
	}
	detached := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			<-w.pool.sem
			w.pool.scopes.release(scope)
		}()
		run(detached)
	}()
	return nil
}

// MemoryEffectStore is a concurrency-safe test/local-only store. Sessions
// production should supply a durable outbox-backed EffectStore before it
// acknowledges delivery to a state-owning cell.
type MemoryEffectStore struct {
	mu       sync.Mutex
	receipts map[scopedIdempotencyKey]EffectReceipt
}

func NewMemoryEffectStore() *MemoryEffectStore {
	return &MemoryEffectStore{receipts: make(map[scopedIdempotencyKey]EffectReceipt)}
}

func (s *MemoryEffectStore) Get(_ context.Context, scope ext.Scope, idempotencyKey string) (EffectReceipt, bool, error) {
	if s == nil {
		return EffectReceipt{}, false, errors.New("workers: effect store is nil")
	}
	s.mu.Lock()
	receipt, ok := s.receipts[scopedIdempotencyKey{scope: scope, key: idempotencyKey}]
	s.mu.Unlock()
	return cloneReceipt(receipt), ok, nil
}

func (s *MemoryEffectStore) Put(_ context.Context, receipt EffectReceipt) error {
	if s == nil {
		return errors.New("workers: effect store is nil")
	}
	if err := receipt.Receipt.ValidateFor(receipt.Intent); err != nil {
		return fmt.Errorf("validate host effect receipt: %w", err)
	}
	s.mu.Lock()
	s.receipts[scopedIdempotencyKey{scope: receipt.Scope, key: receipt.Intent.IdempotencyKey}] = cloneReceipt(receipt)
	s.mu.Unlock()
	return nil
}
