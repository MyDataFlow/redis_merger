package main

import (
	"fmt"
	"bytes"
)


func main() {
	fmt.Println("RedisMerger starting.....")

	fake := NewFakeRedis(1,"10.0.1.4",6401)
	fake.Connect()
	cmd := bytes.NewBufferString("SYNC\r\n")
	fake.Write(cmd.Bytes())
	fake.LoopRead()
}