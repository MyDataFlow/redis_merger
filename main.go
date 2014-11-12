package main

import (
	"fmt"
)


func main() {
	fmt.Println("RedisMerger starting.....")

	slave := NewFakeRedis(1,"10.0.1.4",6380)
	slave.Connect()

	target := NewFakeRedis(0,"127.0.0.1",6379)
	target.Connect()

	ch := make(chan *RedisCommand,1024)

	go target.WaitChannelToConn(ch)
	slave.Write(SYNC_CMD)
	slave.WaitConnToChannel(ch)

}