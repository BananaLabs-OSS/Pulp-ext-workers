package workersext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	durableEffectStoreVersion = 1

	// Terminal receipts bridge the short interval between host completion and
	// owner acknowledgement. They must survive a restart, but retaining them
	// forever makes every Put rewrite an ever-growing snapshot. Status signals
	// are high-frequency and expire quickly; notification receipts get a much
	// longer replay window. Pending receipts are never pruned.
	notificationReceiptRetention = 30 * 24 * time.Hour
	notificationReceiptLimit     = 10_000
	statusSignalReceiptRetention = time.Hour
	statusSignalReceiptLimit     = 1_024
)

// FileEffectStore is a scope-owned, durable receipt store. It is deliberately
// separate from an owner's business outbox: the outbox remains authoritative
// for acknowledgement, while this store prevents a host restart from losing a
// completed provider receipt before that acknowledgement lands.
type FileEffectStore struct {
	mu            sync.Mutex
	scope         ext.Scope
	path          string
	receipts      map[string]durableEffectRecord
	retention     time.Duration
	terminalLimit int
}

type durableEffectFile struct {
	Version  uint8                          `msgpack:"version"`
	Receipts map[string]durableEffectRecord `msgpack:"receipts"`
}

type durableEffectRecord struct {
	Intent      effect.Intent     `msgpack:"intent"`
	Receipt     effect.Receipt    `msgpack:"receipt"`
	Fingerprint [sha256.Size]byte `msgpack:"fingerprint"`
	CreatedAt   int64             `msgpack:"created_at_unix_nano"`
	UpdatedAt   int64             `msgpack:"updated_at_unix_nano"`
}

// NewFileEffectStore creates or loads the one receipt file owned by scope.
// root must be the host-provided application storage root, never an ambient
// working directory; that keeps two applications from sharing durable state.
func NewFileEffectStore(root string, scope ext.Scope) (*FileEffectStore, error) {
	return newFileEffectStore(root, scope, "workers-notification-effect")
}

// newFileEffectStore retains one durable receipt file per effect family and
// scope. Families must use distinct resource types: sharing one file between
// independent stores would let two mutexes overwrite each other's receipts.
// Notification remains on its original resource type for compatibility.
func newFileEffectStore(root string, scope ext.Scope, resourceType string) (*FileEffectStore, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("workers effect store: scope: %w", err)
	}
	if root == "" {
		return nil, errors.New("workers effect store: storage root is required")
	}
	key, err := scope.ResourceKey(resourceType, "receipt-store")
	if err != nil {
		return nil, fmt.Errorf("workers effect store: resource key: %w", err)
	}
	digest := sha256.Sum256([]byte(key.String()))
	dir := filepath.Join(root, "workers-effects")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("workers effect store: create directory: %w", err)
	}
	store := &FileEffectStore{
		scope:         scope,
		path:          filepath.Join(dir, hex.EncodeToString(digest[:])+".msgpack"),
		receipts:      make(map[string]durableEffectRecord),
		retention:     notificationReceiptRetention,
		terminalLimit: notificationReceiptLimit,
	}
	if resourceType == statusSignalEffectResourceType {
		store.retention = statusSignalReceiptRetention
		store.terminalLimit = statusSignalReceiptLimit
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileEffectStore) Get(_ context.Context, scope ext.Scope, idempotencyKey string) (EffectReceipt, bool, error) {
	if s == nil {
		return EffectReceipt{}, false, errors.New("workers effect store is nil")
	}
	if scope != s.scope {
		return EffectReceipt{}, false, nil
	}
	s.mu.Lock()
	record, ok := s.receipts[idempotencyKey]
	s.mu.Unlock()
	if !ok {
		return EffectReceipt{}, false, nil
	}
	return effectReceiptFromRecord(s.scope, record), true, nil
}

func (s *FileEffectStore) Put(_ context.Context, receipt EffectReceipt) error {
	if s == nil {
		return errors.New("workers effect store is nil")
	}
	if receipt.Scope != s.scope {
		return errors.New("workers effect store: cross-scope receipt")
	}
	if err := receipt.Receipt.ValidateFor(receipt.Intent); err != nil {
		return fmt.Errorf("workers effect store: validate receipt: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receipts[receipt.Intent.IdempotencyKey] = durableEffectRecord{
		Intent: cloneIntent(receipt.Intent), Receipt: cloneCanonicalReceipt(receipt.Receipt),
		Fingerprint: receipt.Fingerprint, CreatedAt: receipt.CreatedAt.UnixNano(), UpdatedAt: receipt.UpdatedAt.UnixNano(),
	}
	return s.persistLocked()
}

func (s *FileEffectStore) load() error {
	wire, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("workers effect store: read: %w", err)
	}
	var file durableEffectFile
	if err := msgpack.Unmarshal(wire, &file); err != nil {
		return fmt.Errorf("workers effect store: decode: %w", err)
	}
	if file.Version != durableEffectStoreVersion {
		return fmt.Errorf("workers effect store: unsupported version %d", file.Version)
	}
	for key, record := range file.Receipts {
		if key == "" || record.Intent.IdempotencyKey != key {
			return errors.New("workers effect store: malformed receipt key")
		}
		if err := record.Intent.Validate(); err != nil {
			return fmt.Errorf("workers effect store: validate intent: %w", err)
		}
		if err := record.Receipt.ValidateFor(record.Intent); err != nil {
			return fmt.Errorf("workers effect store: validate receipt: %w", err)
		}
		s.receipts[key] = durableEffectRecord{
			Intent: cloneIntent(record.Intent), Receipt: cloneCanonicalReceipt(record.Receipt),
			Fingerprint: record.Fingerprint, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
		}
	}
	return nil
}

func (s *FileEffectStore) persistLocked() error {
	s.pruneTerminalLocked(time.Now().UTC())
	file := durableEffectFile{Version: durableEffectStoreVersion, Receipts: make(map[string]durableEffectRecord, len(s.receipts))}
	for key, record := range s.receipts {
		file.Receipts[key] = durableEffectRecord{
			Intent: cloneIntent(record.Intent), Receipt: cloneCanonicalReceipt(record.Receipt),
			Fingerprint: record.Fingerprint, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
		}
	}
	wire, err := msgpack.Marshal(file)
	if err != nil {
		return fmt.Errorf("workers effect store: encode: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".workers-effects-*.tmp")
	if err != nil {
		return fmt.Errorf("workers effect store: create temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("workers effect store: protect temporary file: %w", err)
	}
	if _, err := temporary.Write(wire); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("workers effect store: write: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("workers effect store: sync: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("workers effect store: close: %w", err)
	}
	if err := os.Rename(temporaryName, s.path); err != nil {
		return fmt.Errorf("workers effect store: replace: %w", err)
	}
	return nil
}

func (s *FileEffectStore) pruneTerminalLocked(now time.Time) {
	type terminalRecord struct {
		key       string
		updatedAt int64
	}
	terminal := make([]terminalRecord, 0, len(s.receipts))
	cutoff := now.Add(-s.retention).UnixNano()
	for key, record := range s.receipts {
		if record.Receipt.Status == effect.Pending {
			continue
		}
		if s.retention > 0 && record.UpdatedAt < cutoff {
			delete(s.receipts, key)
			continue
		}
		terminal = append(terminal, terminalRecord{key: key, updatedAt: record.UpdatedAt})
	}
	if s.terminalLimit <= 0 || len(terminal) <= s.terminalLimit {
		return
	}
	sort.Slice(terminal, func(i, j int) bool {
		if terminal[i].updatedAt == terminal[j].updatedAt {
			return terminal[i].key < terminal[j].key
		}
		return terminal[i].updatedAt < terminal[j].updatedAt
	})
	for _, record := range terminal[:len(terminal)-s.terminalLimit] {
		delete(s.receipts, record.key)
	}
}

func effectReceiptFromRecord(scope ext.Scope, record durableEffectRecord) EffectReceipt {
	return EffectReceipt{
		Scope: scope, Intent: cloneIntent(record.Intent), Receipt: cloneCanonicalReceipt(record.Receipt),
		Fingerprint: record.Fingerprint, CreatedAt: time.Unix(0, record.CreatedAt).UTC(), UpdatedAt: time.Unix(0, record.UpdatedAt).UTC(),
	}
}
