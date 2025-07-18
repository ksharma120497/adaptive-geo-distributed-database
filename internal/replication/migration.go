package replication

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/ksharma120497/adaptive-geo-distributed-database/proto"
)

// Migrate streams the current value of key from a source node to a destination.
func Migrate(ctx context.Context, key, srcAddr, dstAddr string) error {
	// 1) Dial source
	srcConn, err := grpc.DialContext(ctx, srcAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial source %s: %w", srcAddr, err)
	}
	defer srcConn.Close()
	srcClient := proto.NewKVClient(srcConn)

	// 2) Retrieve value
	getCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	resp, err := srcClient.Get(getCtx, &proto.GetRequest{Key: key})
	if err != nil {
		return fmt.Errorf("get %s from %s: %w", key, srcAddr, err)
	}
	if !resp.Found {
		return fmt.Errorf("key %s not found on source %s", key, srcAddr)
	}

	// 3) Dial destination
	dstConn, err := grpc.DialContext(ctx, dstAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial dest %s: %w", dstAddr, err)
	}
	defer dstConn.Close()
	dstClient := proto.NewKVClient(dstConn)

	// 4) Put value
	putCtx, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()
	if _, err := dstClient.Put(putCtx, &proto.PutRequest{Key: key, Value: resp.Value}); err != nil {
		return fmt.Errorf("put %s to %s: %w", key, dstAddr, err)
	}

	return nil
}
