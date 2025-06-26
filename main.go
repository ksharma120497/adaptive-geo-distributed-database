package main

import (
	"adaptive-geo-distributed-database/kvstore"
	"fmt"
)

func main() {
	store := kvstore.New()

	store.Put("Testing", []byte("database"))

	if v, ok := store.Get("Testing"); ok {
		fmt.Println("got:", string(v))
	} else {
		fmt.Println("key not found")
	}
}
