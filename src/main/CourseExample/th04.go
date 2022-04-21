package main

import (
	"fmt"
	"sync"
	"time"
)

var mu sync.Mutex
var cond = sync.NewCond(&mu)

func main() {

	go fun1()
	go fun2()
	for {
		//mu.Lock()
		cond.Signal()
		time.Sleep(10 * time.Second)
		//mu.Unlock()
	}
}
func fun1() {
	i := 0
	for {
		mu.Lock()
		cond.Wait()
		i++
		fmt.Println("i=", i)

		mu.Unlock()
	}
}
func fun2() {
	for {
		mu.Lock()
		fmt.Println("fun2")
		time.Sleep(1 * time.Second)
		mu.Unlock()
	}
}
