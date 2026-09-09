package workersext

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/vmihailenco/msgpack/v5"
)

type fencedMemoryEffectStore struct {
	*MemoryEffectStore
	claimMu sync.Mutex
	claims  map[scopedIdempotencyKey]fencedMemoryClaim
}

type fencedMemoryClaim struct {
	owner string
	until time.Time
	fence uint64
}

func newFencedMemoryEffectStore() *fencedMemoryEffectStore {
	return &fencedMemoryEffectStore{MemoryEffectStore: NewMemoryEffectStore(), claims: make(map[scopedIdempotencyKey]fencedMemoryClaim)}
}

func (s *fencedMemoryEffectStore) Claim(_ context.Context, scope ext.Scope, key string, fingerprint [sha256.Size]byte, claimant string, now time.Time, duration time.Duration) (uint64, bool, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	receipt, found, err := s.Get(context.Background(), scope, key)
	if err != nil || !found || receipt.Fingerprint != fingerprint || receipt.Status != effect.Pending {
		return 0, false, err
	}
	claimKey := scopedIdempotencyKey{scope: scope, key: key}
	claim := s.claims[claimKey]
	if claim.owner != "" && claim.until.After(now) {
		return 0, false, nil
	}
	claim.fence++
	claim.owner, claim.until = claimant, now.Add(duration)
	s.claims[claimKey] = claim
	return claim.fence, true, nil
}

func (s *fencedMemoryEffectStore) Finish(_ context.Context, receipt EffectReceipt, claimant string, fence uint64) (bool, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	key := scopedIdempotencyKey{scope: receipt.Scope, key: receipt.Intent.IdempotencyKey}
	claim := s.claims[key]
	if claim.owner != claimant || claim.fence != fence {
		return false, nil
	}
	if err := s.Put(context.Background(), receipt); err != nil {
		return false, err
	}
	delete(s.claims, key)
	return true, nil
}

type goroutineEffectWorker struct{}

func (goroutineEffectWorker) Submit(_ context.Context, _ ext.Scope, run func(context.Context)) error {
	go run(context.Background())
	return nil
}

func TestFencedEffectStoreAllowsOnlyOneConcurrentExecutor(t *testing.T) {
	store := newFencedMemoryEffectStore()
	var calls atomic.Int32
	handler := func(context.Context, effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		result, _ := msgpack.Marshal(map[string]bool{"sent": true})
		return result, nil, nil
	}
	first, err := NewEffectExecutor(store, goroutineEffectWorker{}, handler)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewEffectExecutor(store, goroutineEffectWorker{}, handler)
	if err != nil {
		t.Fatal(err)
	}
	scope := postgresEffectTestScope(t, "app-a", "cell-a")
	intent := postgresEffectTestIntent(t, "key-a")
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, executor := range []*EffectExecutor{first, second} {
		wg.Add(1)
		go func(executor *EffectExecutor) {
			defer wg.Done()
			<-start
			_, _ = executor.Submit(t.Context(), scope, intent)
		}(executor)
	}
	close(start)
	wg.Wait()
	deadline := time.Now().Add(time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(40 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider executions = %d, want 1", got)
	}
}

func TestFencedEffectStoreExpiresClaimsAndRejectsStaleCompletion(t *testing.T) {
	store := newFencedMemoryEffectStore()
	scope := postgresEffectTestScope(t, "app-a", "cell-a")
	intent := postgresEffectTestIntent(t, "key-a")
	pending, _ := effect.NewPendingReceipt(intent)
	now := time.Now().UTC()
	record := EffectReceipt{Scope: scope, Intent: intent, Receipt: pending, Fingerprint: effectFingerprint(intent), CreatedAt: now, UpdatedAt: now}
	if err := store.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	firstFence, claimed, err := store.Claim(t.Context(), scope, intent.IdempotencyKey, record.Fingerprint, "worker-a", now, time.Second)
	if err != nil || !claimed {
		t.Fatalf("first claim = %d/%v/%v", firstFence, claimed, err)
	}
	secondFence, claimed, err := store.Claim(t.Context(), scope, intent.IdempotencyKey, record.Fingerprint, "worker-b", now.Add(2*time.Second), time.Second)
	if err != nil || !claimed || secondFence <= firstFence {
		t.Fatalf("replacement claim = %d/%v/%v", secondFence, claimed, err)
	}
	completed, _ := effect.NewCompletedReceipt(intent, map[string]bool{"sent": true})
	record.Receipt, record.UpdatedAt = completed, now.Add(3*time.Second)
	if settled, err := store.Finish(t.Context(), record, "worker-a", firstFence); err != nil || settled {
		t.Fatalf("stale completion = %v/%v", settled, err)
	}
	if settled, err := store.Finish(t.Context(), record, "worker-b", secondFence); err != nil || !settled {
		t.Fatalf("current completion = %v/%v", settled, err)
	}
}

func TestMigrateFileEffectStoreImportsThenArchivesLegacyLedger(t *testing.T) {
	root := t.TempDir()
	scope := postgresEffectTestScope(t, "app-a", "cell-a")
	legacy, err := newFileEffectStore(root, scope, "workers-notification-effect")
	if err != nil {
		t.Fatal(err)
	}
	intent := postgresEffectTestIntent(t, "key-a")
	pending, err := effect.NewPendingReceipt(intent)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	record := EffectReceipt{Scope: scope, Intent: intent, Receipt: pending, Fingerprint: effectFingerprint(intent), CreatedAt: now, UpdatedAt: now}
	if err := legacy.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	destination := NewMemoryEffectStore()
	if err := migrateFileEffectStore(t.Context(), root, scope, "workers-notification-effect", destination); err != nil {
		t.Fatal(err)
	}
	got, found, err := destination.Get(t.Context(), scope, "key-a")
	if err != nil || !found || got.Fingerprint != record.Fingerprint {
		t.Fatalf("imported record = %#v, %v/%v", got, found, err)
	}
	if _, err := os.Stat(legacy.path + ".postgres-migrated-v1"); err != nil {
		t.Fatalf("legacy archive: %v", err)
	}
	if _, err := os.Stat(legacy.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active legacy path still exists: %v", err)
	}
}

func TestLegacyMigrationArchivesAreFamilySpecific(t *testing.T) {
	root := t.TempDir()
	scope := postgresEffectTestScope(t, "app-a", "cell-a")
	notifications, err := newFileEffectStore(root, scope, "workers-notification-effect")
	if err != nil {
		t.Fatal(err)
	}
	status, err := newFileEffectStore(root, scope, statusSignalEffectResourceType)
	if err != nil {
		t.Fatal(err)
	}
	if notifications.path == status.path || notifications.path+".postgres-migrated-v1" == status.path+".postgres-migrated-v1" {
		t.Fatalf("effect families share migration path: %q", notifications.path)
	}
}

func TestPostgresEffectStoreRequiresExactScopedExistingDatabase(t *testing.T) {
	scope := postgresEffectTestScope(t, "app-a", "cell-a")
	called := false
	_, err := NewPostgresEffectStore(func(got ext.Scope) (*sql.DB, error) {
		called = true
		if got != scope {
			t.Fatalf("resolved scope = %#v, want %#v", got, scope)
		}
		return nil, errors.New("not registered")
	}, scope, "family-a")
	if err == nil || !called {
		t.Fatalf("constructor error/called = %v/%v", err, called)
	}
}

func TestPostgresEffectReceiptTransitionsRejectStaleOrConflictingWrites(t *testing.T) {
	intent := postgresEffectTestIntent(t, "key-a")
	pending, err := effect.NewPendingReceipt(intent)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := effect.NewCompletedReceipt(intent, map[string]bool{"sent": true})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := effect.NewFailedReceipt(intent, effect.Failure{Code: "temporary", Message: "temporary"})
	if err != nil {
		t.Fatal(err)
	}
	pendingWire, _ := effect.MarshalReceipt(pending)
	completedWire, _ := effect.MarshalReceipt(completed)
	failedWire, _ := effect.MarshalReceipt(failed)

	for _, test := range []struct {
		name       string
		stored     effect.Status
		incoming   effect.Status
		storedWire []byte
		newWire    []byte
		want       bool
	}{
		{"pending completes", effect.Pending, effect.Completed, pendingWire, completedWire, true},
		{"pending fails", effect.Pending, effect.Failed, pendingWire, failedWire, true},
		{"failed retries", effect.Failed, effect.Pending, failedWire, pendingWire, true},
		{"completed is immutable", effect.Completed, effect.Pending, completedWire, pendingWire, false},
		{"failed cannot become completed without retry claim", effect.Failed, effect.Completed, failedWire, completedWire, false},
		{"different terminal receipt conflicts", effect.Completed, effect.Completed, completedWire, failedWire, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validEffectReceiptTransition(test.stored, test.incoming, test.storedWire, test.newWire); got != test.want {
				t.Fatalf("transition = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDecodePostgresEffectRecordValidatesFingerprintAndReceipt(t *testing.T) {
	scope := postgresEffectTestScope(t, "app-a", "cell-a")
	intent := postgresEffectTestIntent(t, "key-a")
	receipt, err := effect.NewPendingReceipt(intent)
	if err != nil {
		t.Fatal(err)
	}
	intentWire, _ := effect.MarshalIntent(intent)
	receiptWire, _ := effect.MarshalReceipt(receipt)
	fingerprint := effectFingerprint(intent)
	now := time.Now().UTC().Truncate(time.Microsecond)
	record, err := decodePostgresEffectRecord(scope, intentWire, receiptWire, fingerprint[:], now, now)
	if err != nil {
		t.Fatal(err)
	}
	if record.Scope != scope || record.Intent.IdempotencyKey != "key-a" || record.Status != effect.Pending {
		t.Fatalf("record = %#v", record)
	}
	bad := sha256.Sum256([]byte("different"))
	if _, err := decodePostgresEffectRecord(scope, intentWire, receiptWire, bad[:], now, now); err == nil {
		t.Fatal("mismatched fingerprint was accepted")
	}
}

func TestEffectStoreRetentionIsOwnedByEffectFamily(t *testing.T) {
	if retention, limit := effectStoreRetention(statusSignalEffectResourceType); retention != statusSignalReceiptRetention || limit != statusSignalReceiptLimit {
		t.Fatalf("status retention = %v/%d", retention, limit)
	}
	if retention, limit := effectStoreRetention("workers-notification-effect"); retention != notificationReceiptRetention || limit != notificationReceiptLimit {
		t.Fatalf("notification retention = %v/%d", retention, limit)
	}
}

func postgresEffectTestScope(t *testing.T, application, cell string) ext.Scope {
	t.Helper()
	scope, err := ext.NewScope(application, "primary", cell, "primary")
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func postgresEffectTestIntent(t *testing.T, key string) effect.Intent {
	t.Helper()
	payload, err := msgpack.Marshal(NotificationEmailPayload{To: "person@example.test", Subject: "subject", Text: "text"})
	if err != nil {
		t.Fatal(err)
	}
	intent := effect.Intent{Version: effect.VersionV1, ID: "intent-" + key, Kind: effect.KindNotificationEmailSend, IdempotencyKey: key, Payload: payload}
	if err := intent.Validate(); err != nil {
		t.Fatal(err)
	}
	return intent
}
