// internal/kvstore/service.go
package kvstore

import (
	"context"

	"github.com/ksharma120497/adaptive-geo-distributed-database/proto"
)

// Service implements the KV gRPC interface.
type Service struct {
	*proto.UnimplementedKVServer
	store *KVStore
}

// NewService returns a new KV service wrapping the given store.
func NewService(s *KVStore) *Service {
	return &Service{
		UnimplementedKVServer: &proto.UnimplementedKVServer{},
		store:                 s,
	}
}

// Put writes the key/value into the store.
func (s *Service) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutReply, error) {
	if err := s.store.Append(req.Key, req.Value); err != nil {
		return nil, err
	}
	s.store.Set(req.Key, req.Value)
	return &proto.PutReply{Success: true}, nil
}

// Get reads the value for a key from the store.
func (s *Service) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetReply, error) {
	val, ok := s.store.Get(req.Key)
	if !ok {
		return &proto.GetReply{Found: false}, nil
	}
	return &proto.GetReply{Value: val, Found: true}, nil
}
