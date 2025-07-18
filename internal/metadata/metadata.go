// internal/metadata/metadata.go
package metadata

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Client wraps an etcd connection and provides watch hooks.
type Client struct {
	etcd *clientv3.Client
}

// NewClient connects to etcd. Accepts a comma-separated list of endpoints.
func NewClient(endpoints []string) (*Client, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{etcd: cli}, nil
}

// WatchRingConfig watches "/ring/config" and calls updateFn each time it changes.
func (c *Client) WatchRingConfig(updateFn func(config []byte)) {
	go func() {
		rch := c.etcd.Watch(context.Background(), "/ring/config")
		for wr := range rch {
			for _, ev := range wr.Events {
				updateFn(ev.Kv.Value)
			}
		}
	}()
}

// WatchReplicas watches "/replicas/" prefix and calls updateFn(key, value) on each change.
func (c *Client) WatchReplicas(updateFn func(key string, value []byte)) {
	go func() {
		rch := c.etcd.Watch(context.Background(), "/replicas/", clientv3.WithPrefix())
		for wr := range rch {
			for _, ev := range wr.Events {
				// key is the part after "/replicas/"
				k := strings.TrimPrefix(string(ev.Kv.Key), "/replicas/")
				updateFn(k, ev.Kv.Value)
			}
		}
	}()
}

// SetReplicas writes the replica list for a specific key into etcd.
func (c *Client) SetReplicas(key string, replicas []string) error {
	buf, err := json.Marshal(replicas)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = c.etcd.Put(ctx, "/replicas/"+key, string(buf))
	return err
}
