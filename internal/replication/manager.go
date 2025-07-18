package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ksharma120497/adaptive-geo-distributed-database/internal/hashring"
	"github.com/ksharma120497/adaptive-geo-distributed-database/internal/metadata"
)

// DecisionFunc is your cost model: for key x and node y, should we add/remove?
type DecisionFunc func(key, node string) (shouldAdd bool, score float64)

// Manager drives adaptive placement.
type Manager struct {
	ring   *hashring.Ring
	md     *metadata.Client
	R      int
	decide DecisionFunc

	// in-memory metrics; you can swap for a more scalable store
	mu     sync.Mutex
	reads  map[string]map[string]int // reads[key][region]
	writes map[string]map[string]int // writes[key][region]
}

// NewManager constructs the replica manager.
func NewManager(r *hashring.Ring, md *metadata.Client, R int, decide DecisionFunc) *Manager {
	return &Manager{
		ring:   r,
		md:     md,
		R:      R,
		decide: decide,
		reads:  make(map[string]map[string]int),
		writes: make(map[string]map[string]int),
	}
}

// RecordRead logs a single Get(key) from region.
func (m *Manager) RecordRead(key, region string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.reads[key]; !ok {
		m.reads[key] = make(map[string]int)
	}
	m.reads[key][region]++
}

// RecordWrite logs a single Put(key) from region.
func (m *Manager) RecordWrite(key, region string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.writes[key]; !ok {
		m.writes[key] = make(map[string]int)
	}
	m.writes[key][region]++
}

// Run starts the periodic decision loop.
// It polls metrics, runs the cost model, and updates etcd when needed.
func (m *Manager) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evaluateAllKeys(ctx)
		}
	}
}

func (m *Manager) evaluateAllKeys(ctx context.Context) {
	m.mu.Lock()
	keys := make([]string, 0, len(m.reads))
	for k := range m.reads {
		keys = append(keys, k)
	}
	m.mu.Unlock()

	for _, key := range keys {
		// for simplicity, consider all nodes in ring.nodes
		// you could restrict to regions seen in metrics
		for _, node := range m.ring.AllNodes() {
			add, score := m.decide(key, node)
			if add {
				// fetch current list, append node if missing
				current := m.ring.GetReplicaList(key, m.R)
				if !contains(current, node) {
					newList := append(current, node)
					// write to etcd
					if err := m.md.SetReplicas(key, newList); err != nil {
						fmt.Printf("replica update error for %s: %v\n", key, err)
					}
				}
			}
			_ = score // you could log or threshold
		}
	}
}

// helper
func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
