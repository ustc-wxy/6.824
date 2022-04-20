package main

import (
	"fmt"
	"hash/fnv"
	"sync"
)

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func main() {
	//fmt.Println("hash value is %d", ihash("ACTRESS")%10)
	//fmt.Println("hash value is %d", ihash("actress")%10)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(x int) {
			sendMsg(x)
			wg.Done()
		}(i)
	}
	wg.Wait()
}
func sendMsg(i int) {
	fmt.Println(i)
}
