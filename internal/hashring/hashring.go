package hashring

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

// Ring implements a consistent-hash ring with per-key replica overrides.
type Ring struct {
	mu             sync.RWMutex
	hashes         []uint32            // sorted hashes of virtual nodes
	nodes          map[uint32]string   // hash -> physical node ID
	vnodes         int                 // virtual nodes per physical node
	perKeyReplicas map[string][]string // override replica lists by key
}

// New creates a Ring with the given number of virtual nodes per physical node.
func New(vnodes int) *Ring {
	return &Ring{
		nodes:          make(map[uint32]string),
		vnodes:         vnodes,
		perKeyReplicas: make(map[string][]string),
	}
}

// AddNode adds a physical node to the ring by creating virtual nodes.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.vnodes; i++ {
		label := nodeID + "#" + strconv.Itoa(i)
		hash := hashKey(label)
		r.hashes = append(r.hashes, hash)
		r.nodes[hash] = nodeID
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// RemoveNode removes a physical node and its virtual replicas from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	filtered := r.hashes[:0]
	for _, h := range r.hashes {
		if r.nodes[h] == nodeID {
			delete(r.nodes, h)
		} else {
			filtered = append(filtered, h)
		}
	}
	r.hashes = filtered
}

// GetNode returns the primary owner for the given key.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h := hashKey(key)
	idx := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if idx == len(r.hashes) {
		idx = 0
	}
	return r.nodes[r.hashes[idx]]
}

// GetReplicaList returns either the override replicas for a key or the default R successors.
func (r *Ring) GetReplicaList(key string, R int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if repls, ok := r.perKeyReplicas[key]; ok {
		return repls
	}
	// default: primary + R-1 successors
	list := make([]string, 0, R)
	h := hashKey(key)
	// find starting index
	idx := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	for i := 0; len(list) < R && i < len(r.hashes); i++ {
		i2 := (idx + i) % len(r.hashes)
		node := r.nodes[r.hashes[i2]]
		// avoid duplicates
		if !contains(list, node) {
			list = append(list, node)
		}
	}
	return list
}

// Update rebuilds the ring configuration from JSON-encoded metadata.
func (r *Ring) Update(raw []byte) {
	var cfg struct {
		VNodes int      `json:"vnodes_per_node"`
		Nodes  []string `json:"nodes"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.vnodes = cfg.VNodes
	r.hashes = r.hashes[:0]
	// clear nodes map
	r.nodes = make(map[uint32]string)
	// re-add all physical nodes
	for _, node := range cfg.Nodes {
		for i := 0; i < r.vnodes; i++ {
			hash := hashKey(fmt.Sprintf("%s#%d", node, i))
			r.hashes = append(r.hashes, hash)
			r.nodes[hash] = node
		}
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// UpdateReplicas updates the per-key replica list for a specific key.
func (r *Ring) UpdateReplicas(key string, raw []byte) {
	var repls []string
	if err := json.Unmarshal(raw, &repls); err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.perKeyReplicas[key] = repls
}

// helper: check if slice contains a string
func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// hashKey hashes a string to a uint32 using SHA-1.
func hashKey(key string) uint32 {
	sum := sha1.Sum([]byte(key))
	return (uint32(sum[0]) << 24) | (uint32(sum[1]) << 16) |
		(uint32(sum[2]) << 8) | uint32(sum[3])
}
