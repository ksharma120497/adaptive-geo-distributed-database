// cmd/proxy/main.go
package main

import (
	"flag"
	"log"
	"net"
	"strings"

	// "time"

	"google.golang.org/grpc"

	"github.com/ksharma120497/adaptive-geo-distributed-database/internal/hashring"
	"github.com/ksharma120497/adaptive-geo-distributed-database/internal/metadata"
	"github.com/ksharma120497/adaptive-geo-distributed-database/internal/proxy"
	"github.com/ksharma120497/adaptive-geo-distributed-database/proto"
)

func main() {
	// Command‐line flags
	etcdEndpoints := flag.String("etcd", "localhost:2379", "comma-separated etcd endpoints")
	listenAddr := flag.String("listen", ":8080", "proxy listen address")
	vnodes := flag.Int("vnodes", 100, "number of virtual nodes per physical node")
	R := flag.Int("replicas", 3, "replication factor")
	flag.Parse()

	// Create metadata client (it will connect to etcd internally)
	md, err := metadata.NewClient(strings.Split(*etcdEndpoints, ","))
	if err != nil {
		log.Fatalf("failed to connect to etcd: %v", err)
	}

	// Initialize consistent-hash ring
	ring := hashring.New(*vnodes)
	md.WatchRingConfig(ring.Update)
	md.WatchReplicas(ring.UpdateReplicas)

	// Create and start gRPC server
	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *listenAddr, err)
	}
	grpcServer := grpc.NewServer()

	// Register proxy service
	svc := proxy.NewProxyServer(ring, md, *R)
	proto.RegisterKVServer(grpcServer, svc)

	log.Printf("Proxy listening on %s (etcd endpoints: %s)", *listenAddr, *etcdEndpoints)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC serve error: %v", err)
	}
}
