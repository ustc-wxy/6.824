package main

//use Channel to imp a WaitGroup
func main() {
	done := make(chan bool)
	for i := 0; i < 5; i++ {
		go func(x int) {
			sendRPC(x)
			done <- true
		}(i)
	}
	for i := 0; i < 5; i++ {
		<-done
	}
}
func sendRPC(i int) {
	println(i)
}
