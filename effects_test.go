package workersext

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/vmihailenco/msgpack/v5"
)

type fakeEffectWorker struct {
	mu   sync.Mutex
	jobs []func(context.Context)
	err  error
}

func (w *fakeEffectWorker) Submit(_ context.Context, _ ext.Scope, run func(context.Context)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.jobs = append(w.jobs, run)
	return nil
}

func (w *fakeEffectWorker) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.jobs)
}

func (w *fakeEffectWorker) runNext(t *testing.T) {
	t.Helper()
	w.mu.Lock()
	if len(w.jobs) == 0 {
		w.mu.Unlock()
		t.Fatal("fake worker has no queued job")
	}
	job := w.jobs[0]
	w.jobs = w.jobs[1:]
	w.mu.Unlock()
	job(context.Background())
}

func newEffectExecutor(t *testing.T, store EffectStore, worker EffectWorker, handler EffectHandler) *EffectExecutor {
	t.Helper()
	executor, err := NewEffectExecutor(store, worker, handler)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	return executor
}

func emailIntent(t *testing.T, id, key, kind string) effect.Intent {
	t.Helper()
	payload, err := msgpack.Marshal(map[string]string{"template": "session-ready"})
	if err != nil {
		t.Fatal(err)
	}
	return effect.Intent{
		Version: effect.VersionV1, ID: id, Kind: kind, IdempotencyKey: key,
		Payload: payload,
	}
}

func receiptResult(t *testing.T, value map[string]bool) msgpack.RawMessage {
	t.Helper()
	result, err := msgpack.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestEffectExecutor_CanonicalWireReplayAndCrossScopeIsolation(t *testing.T) {
	store := NewMemoryEffectStore()
	worker := &fakeEffectWorker{}
	var calls int
	executor := newEffectExecutor(t, store, worker, func(_ context.Context, intent effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		calls++
		if intent.Kind != effect.KindNotificationEmailSend {
			t.Fatalf("handler kind = %q", intent.Kind)
		}
		return receiptResult(t, map[string]bool{"sent": true}), nil, nil
	})

	evolution := scopedTestScope(t, "evolution", "blue", "notifications", "one")
	sessions := scopedTestScope(t, "sessions", "blue", "notifications", "one")
	request := emailIntent(t, "effect-42", "order-42:email:receipt", effect.KindNotificationEmailSend)

	pending, err := executor.Submit(context.Background(), evolution, request)
	if err != nil || pending.Status != effect.Pending || pending.Intent.Kind != effect.KindNotificationEmailSend {
		t.Fatalf("first submit = (%+v, %v), want canonical pending", pending, err)
	}
	if worker.count() != 1 {
		t.Fatalf("jobs after first submit = %d, want 1", worker.count())
	}

	// Same-scope retries replay pending work and do not enqueue another email.
	replayedPending, err := executor.Submit(context.Background(), evolution, request)
	if err != nil || replayedPending.Status != effect.Pending || worker.count() != 1 {
		t.Fatalf("pending replay = (%+v, %v), jobs=%d", replayedPending, err, worker.count())
	}
	conflict := emailIntent(t, "other-effect", request.IdempotencyKey, effect.KindNotificationEmailSend)
	if _, err := executor.Submit(context.Background(), evolution, conflict); !errors.Is(err, ErrEffectConflict) {
		t.Fatalf("same-scope conflict error = %v, want ErrEffectConflict", err)
	}

	// The same local key in Sessions is independent from Evolution.
	sessionsPending, err := executor.Submit(context.Background(), sessions, request)
	if err != nil || sessionsPending.Status != effect.Pending || worker.count() != 2 {
		t.Fatalf("cross-scope submit = (%+v, %v), jobs=%d", sessionsPending, err, worker.count())
	}

	worker.runNext(t)
	completed, err := executor.Receipt(context.Background(), evolution, request.IdempotencyKey)
	if err != nil || completed.Status != effect.Completed {
		t.Fatalf("completed receipt = (%+v, %v)", completed, err)
	}
	if err := completed.ValidateFor(completed.Intent); err != nil {
		t.Fatalf("canonical receipt invalid: %v", err)
	}
	result, err := effect.DecodeResult[map[string]bool](completed.Receipt)
	if err != nil || !result["sent"] {
		t.Fatalf("completed result = %#v, %v", result, err)
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}

	// Terminal receipts survive a new executor over the same durable store.
	resumed := newEffectExecutor(t, store, worker, executor.handler)
	replayedCompleted, err := resumed.Submit(context.Background(), evolution, request)
	if err != nil || replayedCompleted.Status != effect.Completed || worker.count() != 1 {
		t.Fatalf("completed replay = (%+v, %v), jobs=%d", replayedCompleted, err, worker.count())
	}
	if calls != 1 {
		t.Fatalf("completed replay re-ran handler: calls=%d", calls)
	}

	other := scopedTestScope(t, "resolver", "blue", "notifications", "one")
	if _, err := resumed.Receipt(context.Background(), other, request.IdempotencyKey); !errors.Is(err, ErrEffectNotFound) {
		t.Fatalf("cross-scope receipt error = %v, want ErrEffectNotFound", err)
	}
}

func TestEffectExecutor_CanonicalWireAndLegacyAlias(t *testing.T) {
	store := NewMemoryEffectStore()
	worker := &fakeEffectWorker{}
	executor := newEffectExecutor(t, store, worker, func(_ context.Context, _ effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		return receiptResult(t, map[string]bool{"sent": true}), nil, nil
	})
	scope := scopedTestScope(t, "sessions", "green", "commerce", "one")
	intent := emailIntent(t, "effect-7", "order-7:email", "sessions.notification.extension.ready.v1")

	wire, err := effect.MarshalIntent(effect.Intent{
		Version: intent.Version, ID: intent.ID, Kind: effect.KindNotificationEmailSend,
		IdempotencyKey: intent.IdempotencyKey, Payload: intent.Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Submit(context.Background(), scope, intent); err == nil {
		t.Fatal("direct non-canonical intent was accepted")
	}
	legacyWire, err := msgpack.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.SubmitWire(context.Background(), scope, legacyWire); err != nil {
		t.Fatalf("submit decoded legacy intent: %v", err)
	}
	worker.runNext(t)
	receipt, err := executor.Receipt(context.Background(), scope, intent.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Intent.Kind != effect.KindNotificationEmailSend || receipt.Kind != effect.KindNotificationEmailSend {
		t.Fatalf("legacy kind was not canonicalized: %#v", receipt)
	}
	canonicalIntentWire, err := effect.MarshalIntent(receipt.Intent)
	if err != nil || !bytes.Equal(canonicalIntentWire, wire) {
		t.Fatalf("canonical intent wire mismatch: %s, %v", hex.EncodeToString(canonicalIntentWire), err)
	}
	receiptWire, err := effect.MarshalReceipt(receipt.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := effect.UnmarshalReceipt(receiptWire)
	if err != nil || decoded.Kind != effect.KindNotificationEmailSend {
		t.Fatalf("canonical receipt wire = %#v, %v", decoded, err)
	}
}

func TestEffectExecutor_FailedIntentCanRetryWithSameStableKey(t *testing.T) {
	store := NewMemoryEffectStore()
	worker := &fakeEffectWorker{}
	attempt := 0
	executor := newEffectExecutor(t, store, worker, func(_ context.Context, _ effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		attempt++
		if attempt == 1 {
			return nil, &effect.Failure{Code: "provider_unavailable", Message: "provider unavailable"}, nil
		}
		return receiptResult(t, map[string]bool{"delivered": true}), nil, nil
	})
	scope := scopedTestScope(t, "sessions", "green", "commerce", "one")
	intent := emailIntent(t, "effect-9", "order-9:email:renewed", effect.KindNotificationEmailSend)

	if _, err := executor.Submit(context.Background(), scope, intent); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	worker.runNext(t)
	failed, err := executor.Receipt(context.Background(), scope, intent.IdempotencyKey)
	if err != nil || failed.Status != effect.Failed || failed.Failure == nil || failed.Failure.Code != "provider_unavailable" {
		t.Fatalf("failed receipt = (%+v, %v)", failed, err)
	}

	pending, err := executor.Submit(context.Background(), scope, intent)
	if err != nil || pending.Status != effect.Pending || worker.count() != 1 {
		t.Fatalf("retry = (%+v, %v), jobs=%d", pending, err, worker.count())
	}
	worker.runNext(t)
	completed, err := executor.Receipt(context.Background(), scope, intent.IdempotencyKey)
	if err != nil || completed.Status != effect.Completed {
		t.Fatalf("retried receipt = (%+v, %v)", completed, err)
	}
	if attempt != 2 {
		t.Fatalf("handler attempts = %d, want 2", attempt)
	}
}

func TestEffectExecutor_RecoversDurablePendingReceiptAfterRestart(t *testing.T) {
	store := NewMemoryEffectStore()
	firstWorker := &fakeEffectWorker{}
	scope := scopedTestScope(t, "sessions", "green", "identity", "one")
	intent := emailIntent(t, "effect-recovered", "verify-recovered:email", effect.KindNotificationEmailSend)
	first := newEffectExecutor(t, store, firstWorker, func(context.Context, effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		t.Fatal("job from stopped process must not run")
		return nil, nil, nil
	})
	if receipt, err := first.Submit(context.Background(), scope, intent); err != nil || receipt.Status != effect.Pending || firstWorker.count() != 1 {
		t.Fatalf("initial pending = (%+v, %v), jobs=%d", receipt, err, firstWorker.count())
	}

	secondWorker := &fakeEffectWorker{}
	second := newEffectExecutor(t, store, secondWorker, func(context.Context, effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		return receiptResult(t, map[string]bool{"delivered": true}), nil, nil
	})
	recovered, err := second.Submit(context.Background(), scope, intent)
	if err != nil || recovered.Status != effect.Pending || secondWorker.count() != 1 {
		t.Fatalf("recovered pending = (%+v, %v), jobs=%d", recovered, err, secondWorker.count())
	}
	if _, err := second.Submit(context.Background(), scope, intent); err != nil || secondWorker.count() != 1 {
		t.Fatalf("in-process pending replay scheduled duplicate: err=%v jobs=%d", err, secondWorker.count())
	}
	secondWorker.runNext(t)
	completed, err := second.Receipt(context.Background(), scope, intent.IdempotencyKey)
	if err != nil || completed.Status != effect.Completed {
		t.Fatalf("recovered completion = (%+v, %v)", completed, err)
	}
}

func TestEffectExecutor_QueueFailurePersistsGenericFailure(t *testing.T) {
	store := NewMemoryEffectStore()
	worker := &fakeEffectWorker{err: errors.New("queue saturated")}
	executor := newEffectExecutor(t, store, worker, func(context.Context, effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		t.Fatal("handler must not run after queue rejection")
		return nil, nil, nil
	})
	scope := scopedTestScope(t, "sessions", "green", "identity", "one")
	intent := emailIntent(t, "effect-verify", "verify-1:email", effect.KindNotificationEmailSend)

	if _, err := executor.Submit(context.Background(), scope, intent); err == nil {
		t.Fatal("queue rejection returned nil error")
	}
	failed, err := executor.Receipt(context.Background(), scope, intent.IdempotencyKey)
	if err != nil || failed.Status != effect.Failed || failed.Failure == nil || failed.Failure.Code != "host_execution_failed" {
		t.Fatalf("queue failure receipt = (%+v, %v)", failed, err)
	}
}

func TestScopedNotificationEffectFactory_DurableScopeReplay(t *testing.T) {
	resetSharedWorkerPoolForTest(t)
	storageRoot := t.TempDir()
	application := scopedTestScope(t, "sessions", "blue", "host", "primary")
	cell := scopedTestScope(t, "sessions", "blue", "commerce", "one")
	if err := workersSetup(ext.SetupEnv{Scope: application, StorageRoot: storageRoot, Logger: slog.Default()}); err != nil {
		t.Fatalf("workers setup: %v", err)
	}
	var deliveries int
	var deliveredKey string
	var deliveredScope ext.Scope
	var deliveryMu sync.Mutex
	factory, err := NewScopedNotificationEffectExecutorFactory(func(scope ext.Scope) (NotificationEmailDelivery, error) {
		return NotificationEmailDeliveryFunc(func(_ context.Context, intent effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
			deliveryMu.Lock()
			deliveries++
			deliveredKey = intent.IdempotencyKey
			deliveredScope = scope
			deliveryMu.Unlock()
			return receiptResult(t, map[string]bool{"sent": true}), nil, nil
		}), nil
	})
	if err != nil {
		t.Fatalf("new notification factory: %v", err)
	}
	executor, err := factory.ForScope(cell)
	if err != nil {
		t.Fatalf("factory scope: %v", err)
	}
	intent := emailIntent(t, "effect-42", "mail-42", effect.KindNotificationEmailSend)
	if receipt, err := executor.Submit(context.Background(), cell, intent); err != nil || receipt.Status != effect.Pending {
		t.Fatalf("submit = (%+v, %v), want pending", receipt, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		receipt, err := executor.Receipt(context.Background(), cell, intent.IdempotencyKey)
		if err != nil {
			t.Fatalf("poll receipt: %v", err)
		}
		if receipt.Status == effect.Completed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("receipt remained %q", receipt.Status)
		}
		time.Sleep(time.Millisecond)
	}
	deliveryMu.Lock()
	gotDeliveries, gotKey, gotScope := deliveries, deliveredKey, deliveredScope
	deliveryMu.Unlock()
	if gotDeliveries != 1 {
		t.Fatalf("deliveries = %d, want 1", gotDeliveries)
	}
	if gotKey != "mail-42" || gotScope != cell {
		t.Fatalf("delivery binding = (%q, %#v), want (mail-42, %#v)", gotKey, gotScope, cell)
	}
	if err := factory.TeardownScope(application); err != nil {
		t.Fatalf("factory teardown: %v", err)
	}
	if factory.Count() != 0 {
		t.Fatalf("factory count after scoped teardown = %d, want 0", factory.Count())
	}

	// A fresh executor loads the durable completed receipt and does not invoke
	// the real delivery adapter again before the owner outbox acknowledges it.
	resumed, err := factory.ForScope(cell)
	if err != nil {
		t.Fatalf("resume factory scope: %v", err)
	}
	replayed, err := resumed.Submit(context.Background(), cell, intent)
	if err != nil || replayed.Status != effect.Completed {
		t.Fatalf("durable replay = (%+v, %v)", replayed, err)
	}
	deliveryMu.Lock()
	gotDeliveries = deliveries
	deliveryMu.Unlock()
	if gotDeliveries != 1 {
		t.Fatalf("durable replay delivered twice: %d", gotDeliveries)
	}
}

func TestScopedNotificationEffectFactory_FailsClosedWithoutRuntimeOrHandler(t *testing.T) {
	resetSharedWorkerPoolForTest(t)
	cell := scopedTestScope(t, "sessions", "red", "commerce", "one")
	factory, err := NewScopedNotificationEffectExecutorFactory(func(ext.Scope) (NotificationEmailDelivery, error) {
		return nil, errors.New("email provider is not configured")
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.ForScope(cell); err == nil {
		t.Fatal("factory accepted a scope with no configured workers runtime")
	}
	if _, err := NewScopedNotificationEffectExecutorFactory(nil); err == nil {
		t.Fatal("factory accepted nil handler source")
	}
}
