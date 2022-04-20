package main

import (
	"fmt"
	"time"
)

func main() {
	counter := 0
	//var mu sync.Mutex
	for i := 0; i < 1000; i++ {
		go func() {
			//mu.Lock()
			//defer mu.Unlock()
			counter++
		}()
	}
	time.Sleep(time.Second)
	fmt.Println(counter)
}
