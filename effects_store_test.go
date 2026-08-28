package workersext

import (
	"fmt"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
)

func TestFileEffectStorePrunesOnlyTerminalReceipts(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	store := &FileEffectStore{
		receipts:      make(map[string]durableEffectRecord),
		retention:     time.Hour,
		terminalLimit: 2,
	}
	store.receipts["pending-old"] = durableEffectRecord{
		Receipt: effect.Receipt{Status: effect.Pending}, UpdatedAt: now.Add(-24 * time.Hour).UnixNano(),
	}
	store.receipts["terminal-expired"] = durableEffectRecord{
		Receipt: effect.Receipt{Status: effect.Completed}, UpdatedAt: now.Add(-2 * time.Hour).UnixNano(),
	}
	for index := 0; index < 3; index++ {
		key := fmt.Sprintf("terminal-%d", index)
		store.receipts[key] = durableEffectRecord{
			Receipt: effect.Receipt{Status: effect.Completed}, UpdatedAt: now.Add(time.Duration(index) * time.Minute).UnixNano(),
		}
	}

	store.pruneTerminalLocked(now)

	if _, ok := store.receipts["pending-old"]; !ok {
		t.Fatal("old pending receipt was pruned")
	}
	if _, ok := store.receipts["terminal-expired"]; ok {
		t.Fatal("expired terminal receipt was retained")
	}
	if _, ok := store.receipts["terminal-0"]; ok {
		t.Fatal("oldest terminal receipt above the limit was retained")
	}
	for _, key := range []string{"terminal-1", "terminal-2"} {
		if _, ok := store.receipts[key]; !ok {
			t.Fatalf("new terminal receipt %q was pruned", key)
		}
	}
}
