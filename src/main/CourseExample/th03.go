package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	Alice := 1000
	Bob := 1000
	var mu sync.Mutex
	go func() {
		for i := 0; i < 1000; i++ {
			mu.Lock()
			Alice -= 1
			//mu.Unlock()
			//mu.Lock()
			Bob += 1
			mu.Unlock()
		}
	}()
	go func() {
		for i := 0; i < 1000; i++ {
			mu.Lock()
			Bob -= 1
			//mu.Unlock()
			//mu.Lock()
			Alice += 1
			mu.Unlock()
		}
	}()
	start := time.Now()
	for time.Since(start) < 1*time.Second {
		mu.Lock()
		if Alice+Bob != 2000 {
			fmt.Printf("observed violation, alice = %d , bob = %d , total = %d\n", Alice, Bob, Alice+Bob)
		}
		mu.Unlock()
	}
	time.Sleep(1 * time.Second)
	fmt.Printf("[Final Result] alice = %d , bob = %d , total = %d\n", Alice, Bob, Alice+Bob)
}
