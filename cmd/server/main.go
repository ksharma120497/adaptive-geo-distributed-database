// cmd/server/main.go
package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	//   clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/ksharma120497/adaptive-geo-distributed-database/internal/kvstore"
	"github.com/ksharma120497/adaptive-geo-distributed-database/proto"
	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 50051, "gRPC port")
	walPath := flag.String("wal", "wal.log", "WAL file path")
	//   etcdEndpoints := flag.String("etcd", "localhost:2379", "etcd endpoints")
	flag.Parse()

	// Initialize KVStore
	store, err := kvstore.NewWALStore(*walPath)
	if err != nil {
		log.Fatalf("failed to open WAL store: %v", err)
	}
	if err := store.Replay(); err != nil {
		log.Fatalf("failed to replay WAL: %v", err)
	}

	// (Optional) etcd connection omitted since Service doesn't yet use it

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	svc := kvstore.NewService(store)
	proto.RegisterKVServer(grpcServer, svc)
	log.Printf("Server listening on :%d", *port)
	grpcServer.Serve(lis)
}
