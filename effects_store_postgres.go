package workersext

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
)

const postgresEffectReceiptTable = "pulp_worker_effect_receipts_v1"

const postgresEffectReceiptSchema = `CREATE TABLE IF NOT EXISTS pulp_worker_effect_receipts_v1 (
	scope_routing_id TEXT NOT NULL,
	effect_family TEXT NOT NULL,
	idempotency_key TEXT NOT NULL,
	intent_wire BYTEA NOT NULL,
	receipt_wire BYTEA NOT NULL,
	fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 32),
	receipt_status TEXT NOT NULL CHECK (receipt_status IN ('pending', 'completed', 'failed')),
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL,
	claim_owner TEXT,
	claim_until TIMESTAMPTZ,
	claim_fence BIGINT NOT NULL DEFAULT 0 CHECK (claim_fence >= 0),
	PRIMARY KEY (scope_routing_id, effect_family, idempotency_key)
)`

var postgresEffectReceiptUpgrades = []string{
	`ALTER TABLE pulp_worker_effect_receipts_v1 ADD COLUMN IF NOT EXISTS claim_owner TEXT`,
	`ALTER TABLE pulp_worker_effect_receipts_v1 ADD COLUMN IF NOT EXISTS claim_until TIMESTAMPTZ`,
	`ALTER TABLE pulp_worker_effect_receipts_v1 ADD COLUMN IF NOT EXISTS claim_fence BIGINT NOT NULL DEFAULT 0`,
}

const postgresEffectReceiptPruneIndex = `CREATE INDEX IF NOT EXISTS pulp_worker_effect_receipts_v1_prune
	ON pulp_worker_effect_receipts_v1 (scope_routing_id, effect_family, receipt_status, updated_at)`

// EffectStoreDatabase resolves an already-owned pool for an exact Pulp scope.
// The workers extension never opens or closes the pool and never accepts a DSN.
type EffectStoreDatabase func(ext.Scope) (*sql.DB, error)

// EffectStoreFactory constructs the receipt ledger for one exact effect family
// and Pulp placement. Production uses PostgreSQL; local hosts may retain files.
type EffectStoreFactory func(scope ext.Scope, effectFamily string) (EffectStore, error)
type LegacyEffectStoreRoot func(ext.Scope) (string, error)

// PostgresEffectStore is a placement- and family-scoped durable receipt ledger.
// Its primary key prevents separate applications, cells, and effect families
// from observing or overwriting one another even when they share a database.
type PostgresEffectStore struct {
	scope         ext.Scope
	effectFamily  string
	database      *sql.DB
	retention     time.Duration
	terminalLimit int
	schemaMu      sync.Mutex
	schemaReady   bool
	schemaErr     error
}

// NewPostgresEffectStore resolves the exact placement's existing PostgreSQL
// pool. It deliberately does not fall back to a broader application scope.
func NewPostgresEffectStore(databaseForScope EffectStoreDatabase, scope ext.Scope, effectFamily string) (*PostgresEffectStore, error) {
	if databaseForScope == nil {
		return nil, errors.New("workers postgres effect store: scoped database resolver is required")
	}
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("workers postgres effect store: scope: %w", err)
	}
	if effectFamily == "" {
		return nil, errors.New("workers postgres effect store: effect family is required")
	}
	database, err := databaseForScope(scope)
	if err != nil {
		return nil, fmt.Errorf("workers postgres effect store: resolve scoped database: %w", err)
	}
	if database == nil {
		return nil, errors.New("workers postgres effect store: scoped database is unavailable")
	}
	store := &PostgresEffectStore{scope: scope, effectFamily: effectFamily, database: database}
	store.retention, store.terminalLimit = effectStoreRetention(effectFamily)
	return store, nil
}

// NewPostgresEffectStoreFactory adapts an exact-scope connection resolver for
// the notification and status-signal factories.
func NewPostgresEffectStoreFactory(databaseForScope EffectStoreDatabase) (EffectStoreFactory, error) {
	if databaseForScope == nil {
		return nil, errors.New("workers postgres effect store: scoped database resolver is required")
	}
	return func(scope ext.Scope, effectFamily string) (EffectStore, error) {
		return NewPostgresEffectStore(databaseForScope, scope, effectFamily)
	}, nil
}

// NewMigratingPostgresEffectStoreFactory imports the previous scope-owned file
// before returning the PostgreSQL store. Import is idempotent across crashes:
// PostgreSQL rejects different intents and accepts identical records. After a
// complete import the original file is recoverably renamed, preventing a later
// restart from replaying a stale pending record over a terminal receipt.
func NewMigratingPostgresEffectStoreFactory(databaseForScope EffectStoreDatabase, legacyRoot LegacyEffectStoreRoot) (EffectStoreFactory, error) {
	if legacyRoot == nil {
		return nil, errors.New("workers postgres effect store: legacy root resolver is required")
	}
	return func(scope ext.Scope, effectFamily string) (EffectStore, error) {
		store, err := NewPostgresEffectStore(databaseForScope, scope, effectFamily)
		if err != nil {
			return nil, err
		}
		root, err := legacyRoot(scope)
		if err != nil {
			return nil, fmt.Errorf("workers postgres effect store: resolve legacy root: %w", err)
		}
		if err := migrateFileEffectStore(context.Background(), root, scope, effectFamily, store); err != nil {
			return nil, err
		}
		return store, nil
	}, nil
}

func migrateFileEffectStore(ctx context.Context, root string, scope ext.Scope, effectFamily string, destination EffectStore) error {
	legacy, err := newFileEffectStore(root, scope, effectFamily)
	if err != nil {
		return err
	}
	legacy.mu.Lock()
	keys := make([]string, 0, len(legacy.receipts))
	for key := range legacy.receipts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	records := make([]EffectReceipt, 0, len(keys))
	for _, key := range keys {
		records = append(records, effectReceiptFromRecord(scope, legacy.receipts[key]))
	}
	legacy.mu.Unlock()
	for _, record := range records {
		if err := destination.Put(ctx, record); err != nil {
			return fmt.Errorf("workers postgres effect store: import legacy receipt %q: %w", record.Intent.IdempotencyKey, err)
		}
	}
	if len(records) == 0 {
		return nil
	}
	archived := legacy.path + ".postgres-migrated-v1"
	if _, err := os.Stat(archived); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("workers postgres effect store: inspect legacy archive: %w", err)
	}
	if err := os.Rename(legacy.path, archived); err != nil {
		return fmt.Errorf("workers postgres effect store: archive imported legacy store: %w", err)
	}
	return nil
}

func (s *PostgresEffectStore) Get(ctx context.Context, scope ext.Scope, idempotencyKey string) (EffectReceipt, bool, error) {
	if s == nil || s.database == nil {
		return EffectReceipt{}, false, errors.New("workers postgres effect store is nil")
	}
	if scope != s.scope {
		return EffectReceipt{}, false, nil
	}
	if idempotencyKey == "" {
		return EffectReceipt{}, false, errors.New("workers postgres effect store: idempotency key is required")
	}
	if err := s.ensureSchema(ctx); err != nil {
		return EffectReceipt{}, false, err
	}
	var intentWire, receiptWire, fingerprint []byte
	var createdAt, updatedAt time.Time
	err := s.database.QueryRowContext(ctx, `SELECT intent_wire, receipt_wire, fingerprint, created_at, updated_at
		FROM `+postgresEffectReceiptTable+` WHERE scope_routing_id = $1 AND effect_family = $2 AND idempotency_key = $3`,
		s.scope.RoutingID(), s.effectFamily, idempotencyKey,
	).Scan(&intentWire, &receiptWire, &fingerprint, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return EffectReceipt{}, false, nil
	}
	if err != nil {
		return EffectReceipt{}, false, fmt.Errorf("workers postgres effect store: load receipt: %w", err)
	}
	result, err := decodePostgresEffectRecord(s.scope, intentWire, receiptWire, fingerprint, createdAt, updatedAt)
	if err != nil {
		return EffectReceipt{}, false, err
	}
	return result, true, nil
}

func (s *PostgresEffectStore) Put(ctx context.Context, incoming EffectReceipt) error {
	if s == nil || s.database == nil {
		return errors.New("workers postgres effect store is nil")
	}
	if incoming.Scope != s.scope {
		return errors.New("workers postgres effect store: cross-scope receipt")
	}
	if err := incoming.Receipt.ValidateFor(incoming.Intent); err != nil {
		return fmt.Errorf("workers postgres effect store: validate receipt: %w", err)
	}
	if incoming.Fingerprint != effectFingerprint(incoming.Intent) {
		return errors.New("workers postgres effect store: fingerprint does not match intent")
	}
	intentWire, err := effect.MarshalIntent(incoming.Intent)
	if err != nil {
		return fmt.Errorf("workers postgres effect store: encode intent: %w", err)
	}
	receiptWire, err := effect.MarshalReceipt(incoming.Receipt)
	if err != nil {
		return fmt.Errorf("workers postgres effect store: encode receipt: %w", err)
	}
	if err := s.ensureSchema(ctx); err != nil {
		return err
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workers postgres effect store: begin: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO `+postgresEffectReceiptTable+`
		(scope_routing_id, effect_family, idempotency_key, intent_wire, receipt_wire, fingerprint, receipt_status, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`, s.scope.RoutingID(), s.effectFamily,
		incoming.Intent.IdempotencyKey, intentWire, receiptWire, incoming.Fingerprint[:], string(incoming.Status), incoming.CreatedAt, incoming.UpdatedAt)
	if err != nil {
		return fmt.Errorf("workers postgres effect store: insert receipt: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("workers postgres effect store: inspect insert: %w", err)
	}
	if inserted == 0 {
		var storedIntent, storedReceipt, storedFingerprint []byte
		var storedStatus string
		err = tx.QueryRowContext(ctx, `SELECT intent_wire, receipt_wire, fingerprint, receipt_status FROM `+postgresEffectReceiptTable+`
			WHERE scope_routing_id = $1 AND effect_family = $2 AND idempotency_key = $3 FOR UPDATE`,
			s.scope.RoutingID(), s.effectFamily, incoming.Intent.IdempotencyKey,
		).Scan(&storedIntent, &storedReceipt, &storedFingerprint, &storedStatus)
		if err != nil {
			return fmt.Errorf("workers postgres effect store: lock receipt: %w", err)
		}
		if !bytes.Equal(storedIntent, intentWire) || !bytes.Equal(storedFingerprint, incoming.Fingerprint[:]) {
			return ErrEffectConflict
		}
		if !validEffectReceiptTransition(effect.Status(storedStatus), incoming.Status, storedReceipt, receiptWire) {
			return ErrEffectConflict
		}
		if !bytes.Equal(storedReceipt, receiptWire) {
			_, err = tx.ExecContext(ctx, `UPDATE `+postgresEffectReceiptTable+` SET receipt_wire=$1, receipt_status=$2, updated_at=$3
				WHERE scope_routing_id=$4 AND effect_family=$5 AND idempotency_key=$6`, receiptWire, string(incoming.Status), incoming.UpdatedAt,
				s.scope.RoutingID(), s.effectFamily, incoming.Intent.IdempotencyKey)
			if err != nil {
				return fmt.Errorf("workers postgres effect store: advance receipt: %w", err)
			}
		}
	}
	if err := s.pruneTerminal(ctx, tx, time.Now().UTC()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workers postgres effect store: commit: %w", err)
	}
	return nil
}

func (s *PostgresEffectStore) Claim(ctx context.Context, scope ext.Scope, idempotencyKey string, fingerprint [sha256.Size]byte, claimant string, now time.Time, leaseDuration time.Duration) (uint64, bool, error) {
	if s == nil || s.database == nil {
		return 0, false, errors.New("workers postgres effect store is nil")
	}
	if scope != s.scope || idempotencyKey == "" || claimant == "" || leaseDuration <= 0 {
		return 0, false, errors.New("workers postgres effect store: invalid claim")
	}
	if err := s.ensureSchema(ctx); err != nil {
		return 0, false, err
	}
	var fence int64
	err := s.database.QueryRowContext(ctx, `UPDATE `+postgresEffectReceiptTable+` SET
		claim_owner=$1, claim_until=$2, claim_fence=claim_fence+1
		WHERE scope_routing_id=$3 AND effect_family=$4 AND idempotency_key=$5
		AND fingerprint=$6 AND receipt_status='pending'
		AND (claim_until IS NULL OR claim_until <= $7)
		RETURNING claim_fence`, claimant, now.Add(leaseDuration), s.scope.RoutingID(), s.effectFamily,
		idempotencyKey, fingerprint[:], now).Scan(&fence)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("workers postgres effect store: claim receipt: %w", err)
	}
	if fence <= 0 {
		return 0, false, errors.New("workers postgres effect store: database returned an invalid fence")
	}
	return uint64(fence), true, nil
}

func (s *PostgresEffectStore) Finish(ctx context.Context, incoming EffectReceipt, claimant string, fence uint64) (bool, error) {
	if s == nil || s.database == nil {
		return false, errors.New("workers postgres effect store is nil")
	}
	if incoming.Scope != s.scope || claimant == "" || fence == 0 || incoming.Status == effect.Pending {
		return false, errors.New("workers postgres effect store: invalid fenced completion")
	}
	if incoming.Fingerprint != effectFingerprint(incoming.Intent) {
		return false, errors.New("workers postgres effect store: fingerprint does not match intent")
	}
	if err := incoming.Receipt.ValidateFor(incoming.Intent); err != nil {
		return false, fmt.Errorf("workers postgres effect store: validate completion: %w", err)
	}
	receiptWire, err := effect.MarshalReceipt(incoming.Receipt)
	if err != nil {
		return false, fmt.Errorf("workers postgres effect store: encode completion: %w", err)
	}
	result, err := s.database.ExecContext(ctx, `UPDATE `+postgresEffectReceiptTable+` SET
		receipt_wire=$1, receipt_status=$2, updated_at=$3, claim_owner=NULL, claim_until=NULL
		WHERE scope_routing_id=$4 AND effect_family=$5 AND idempotency_key=$6
		AND fingerprint=$7 AND receipt_status='pending' AND claim_owner=$8 AND claim_fence=$9`,
		receiptWire, string(incoming.Status), incoming.UpdatedAt, s.scope.RoutingID(), s.effectFamily,
		incoming.Intent.IdempotencyKey, incoming.Fingerprint[:], claimant, int64(fence))
	if err != nil {
		return false, fmt.Errorf("workers postgres effect store: finish receipt: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("workers postgres effect store: inspect completion: %w", err)
	}
	return rows == 1, nil
}

func validEffectReceiptTransition(stored, incoming effect.Status, storedWire, incomingWire []byte) bool {
	if stored == incoming {
		return bytes.Equal(storedWire, incomingWire) || stored == effect.Pending
	}
	return (stored == effect.Pending && (incoming == effect.Completed || incoming == effect.Failed)) ||
		(stored == effect.Failed && incoming == effect.Pending)
}

func (s *PostgresEffectStore) ensureSchema(ctx context.Context) error {
	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()
	if s.schemaReady {
		return nil
	}
	s.schemaErr = nil
	if _, err := s.database.ExecContext(ctx, postgresEffectReceiptSchema); err != nil {
		s.schemaErr = fmt.Errorf("workers postgres effect store: create schema: %w", err)
		return s.schemaErr
	}
	for _, statement := range postgresEffectReceiptUpgrades {
		if _, err := s.database.ExecContext(ctx, statement); err != nil {
			s.schemaErr = fmt.Errorf("workers postgres effect store: upgrade schema: %w", err)
			return s.schemaErr
		}
	}
	if _, err := s.database.ExecContext(ctx, postgresEffectReceiptPruneIndex); err != nil {
		s.schemaErr = fmt.Errorf("workers postgres effect store: create prune index: %w", err)
		return s.schemaErr
	}
	s.schemaReady = true
	return s.schemaErr
}

func (s *PostgresEffectStore) pruneTerminal(ctx context.Context, tx *sql.Tx, now time.Time) error {
	if s.retention > 0 {
		_, err := tx.ExecContext(ctx, `DELETE FROM `+postgresEffectReceiptTable+` WHERE scope_routing_id=$1 AND effect_family=$2
			AND receipt_status <> 'pending' AND updated_at < $3`, s.scope.RoutingID(), s.effectFamily, now.Add(-s.retention))
		if err != nil {
			return fmt.Errorf("workers postgres effect store: prune expired receipts: %w", err)
		}
	}
	if s.terminalLimit > 0 {
		_, err := tx.ExecContext(ctx, `DELETE FROM `+postgresEffectReceiptTable+` WHERE (scope_routing_id,effect_family,idempotency_key) IN
			(SELECT scope_routing_id,effect_family,idempotency_key FROM `+postgresEffectReceiptTable+` WHERE scope_routing_id=$1 AND effect_family=$2
			 AND receipt_status <> 'pending' ORDER BY updated_at DESC, idempotency_key DESC OFFSET $3)`,
			s.scope.RoutingID(), s.effectFamily, s.terminalLimit)
		if err != nil {
			return fmt.Errorf("workers postgres effect store: enforce receipt limit: %w", err)
		}
	}
	return nil
}

func decodePostgresEffectRecord(scope ext.Scope, intentWire, receiptWire, fingerprint []byte, createdAt, updatedAt time.Time) (EffectReceipt, error) {
	intent, err := effect.UnmarshalIntent(intentWire)
	if err != nil {
		return EffectReceipt{}, fmt.Errorf("workers postgres effect store: decode intent: %w", err)
	}
	receipt, err := effect.UnmarshalReceipt(receiptWire)
	if err != nil {
		return EffectReceipt{}, fmt.Errorf("workers postgres effect store: decode receipt: %w", err)
	}
	if err := receipt.ValidateFor(intent); err != nil {
		return EffectReceipt{}, fmt.Errorf("workers postgres effect store: validate stored receipt: %w", err)
	}
	if len(fingerprint) != sha256.Size {
		return EffectReceipt{}, errors.New("workers postgres effect store: stored fingerprint is invalid")
	}
	var sum [sha256.Size]byte
	copy(sum[:], fingerprint)
	if sum != effectFingerprint(intent) {
		return EffectReceipt{}, errors.New("workers postgres effect store: stored fingerprint does not match intent")
	}
	return EffectReceipt{Scope: scope, Intent: intent, Receipt: receipt, Fingerprint: sum,
		CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC()}, nil
}

func effectStoreRetention(effectFamily string) (time.Duration, int) {
	if effectFamily == statusSignalEffectResourceType {
		return statusSignalReceiptRetention, statusSignalReceiptLimit
	}
	return notificationReceiptRetention, notificationReceiptLimit
}

var _ EffectStore = (*PostgresEffectStore)(nil)
