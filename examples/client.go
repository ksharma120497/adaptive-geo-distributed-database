// examples/client.go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ksharma120497/adaptive-geo-distributed-database/proto"
	"google.golang.org/grpc"
)

func main() {
	// 1) Connect to the proxy
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := proto.NewKVClient(conn)

	// 2) Put a value
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = client.Put(ctx, &proto.PutRequest{
		Key:   "foo",
		Value: []byte("hello"),
	})
	cancel()
	if err != nil {
		panic(err)
	}
	fmt.Println("Wrote key=foo value=hello")

	// 3) Get it back
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, &proto.GetRequest{Key: "foo"})
	cancel()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read key=foo value=%q found=%v\n", string(resp.Value), resp.Found)
}
