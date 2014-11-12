package main

import (
	"fmt"
)


func main() {
	fmt.Println("RedisMerger starting.....")

	fake := NewFakeRedis(1,"10.0.1.4",6401)
	fake.Connect()
	fake.LoopRead()
}