// internal/proxy/proxy.go
package proxy

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/ksharma120497/adaptive-geo-distributed-database/internal/hashring"
	"github.com/ksharma120497/adaptive-geo-distributed-database/internal/metadata"
	"github.com/ksharma120497/adaptive-geo-distributed-database/proto"
)

// Server implements proto.KVServer.
// Embedding UnimplementedKVServer gives you the mustEmbedUnimplementedKVServer method.
type Server struct {
	proto.UnimplementedKVServer

	ring *hashring.Ring
	md   *metadata.Client
	R    int
}

// NewProxyServer constructs the proxy service.
func NewProxyServer(r *hashring.Ring, md *metadata.Client, R int) *Server {
	return &Server{
		UnimplementedKVServer: proto.UnimplementedKVServer{},
		ring:                  r,
		md:                    md,
		R:                     R,
	}
}

func (s *Server) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutReply, error) {
	replicas := s.ring.GetReplicaList(req.Key, s.R)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no replicas for key %q", req.Key)
	}
	for _, addr := range replicas {
		dialCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure(), grpc.WithBlock())
		cancel()
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		defer conn.Close()

		client := proto.NewKVClient(conn)
		if _, err := client.Put(ctx, req); err != nil {
			return nil, fmt.Errorf("put to %s: %w", addr, err)
		}
	}
	return &proto.PutReply{Success: true}, nil
}

func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetReply, error) {
	replicas := s.ring.GetReplicaList(req.Key, s.R)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no replicas for key %q", req.Key)
	}
	target := replicas[0]

	dialCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	conn, err := grpc.DialContext(dialCtx, target, grpc.WithInsecure(), grpc.WithBlock())
	cancel()
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}
	defer conn.Close()

	client := proto.NewKVClient(conn)
	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get from %s: %w", target, err)
	}
	return resp, nil
}
