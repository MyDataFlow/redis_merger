package main

import (
	"fmt"
)


func main() {
	fmt.Println("RedisMerger starting.....")

	slave := NewFakeRedis(1,"10.0.2.8",6380)
	slave.Connect()

	target := NewFakeRedis(0,"127.0.0.1",6379)
	target.Connect()

	ch := make(chan *RedisCommand,1024)


	rdb := NewRedisRDB(slave.upstreamReader)
	slave.Write(SYNC_CMD)
	go target.WaitChannelToConn(ch)

	rdb.WaitRDBToChannel(ch)
	slave.WaitConnToChannel(ch)
}