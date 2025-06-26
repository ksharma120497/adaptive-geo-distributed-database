package hashring

import (
	"crypto/sha1"
	"fmt"
	"sort"
)

type Ring struct {
	hashes []uint32
	nodes  map[uint32]string
	vnodes int
}

func New(vnodes int) *Ring {
	return &Ring{
		nodes:  make(map[uint32]string),
		vnodes: vnodes,
	}
}

func (r *Ring) AddNode(nodeID string) {
	for i := 0; i < r.vnodes; i++ {
		label := fmt.Sprintf("%s#%d", nodeID, i)
		hash := hashKey(label)
		r.hashes = append(r.hashes, hash)
		r.nodes[hash] = nodeID
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

func (r *Ring) RemoveNode(nodeID string) {
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

func (r *Ring) GetNode(key string) string {
	h := hashKey(key)
	idx := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if idx == len(r.hashes) {
		idx = 0
	}
	return r.nodes[r.hashes[idx]]
}

func hashKey(key string) uint32 {
	sum := sha1.Sum([]byte(key))
	return (uint32(sum[0]) << 24) | (uint32(sum[1]) << 16) |
		(uint32(sum[2]) << 8) | uint32(sum[3])
}
