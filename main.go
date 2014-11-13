package main

import (
	"fmt"
	"runtime"
)

func slave(gid int,host string,port int,ch chan *RedisCommand) {
	redis := NewFakeRedis(gid,host,port)
	redis.Connect()
	rdb := NewRedisRDB(redis.upstreamReader)
	redis.Write(SYNC_CMD)
	rdb.WaitRDBToChannel(ch)
	redis.WaitConnToChannel(ch)
}

func main() {
	fmt.Println("RedisMerger starting.....")

	target := NewFakeRedis(0,"127.0.0.1",6379)
	target.Connect()

	NumCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(NumCPU)

	ch := make(chan *RedisCommand,1024)
	length := 5
	base := 6380

	for i := 0; i < length; i++ {
		go slave(i + 1, "10.0.2.8",base + i,ch)
	}

	target.WaitChannelToConn(ch)
}