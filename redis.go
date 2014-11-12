package main

import (
	"bufio"
	"os"
	"net"
	"fmt"
	"log"
)

const (
	BUFF_SIZE       = 16384
)

type FakeRedis struct {
	gid            int
	host		   string
	port           int
	upstreamReader *bufio.Reader
	persistentFile *os.File
	upstreamConn 	net.Conn

}


func NewFakeRedis(gid int,host string,port int) (fake *FakeRedis) {
	fake = &FakeRedis{
		gid: gid,
		host: host,
		port: port,
	}

	return
}

func (redis *FakeRedis) Connect() (err error){
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", redis.host, redis.port))
	if err != nil {
		log.Printf("Failed to connect to redis: %v\n", err)
		return
	}

	redis.upstreamConn = conn
	redis.upstreamReader = bufio.NewReaderSize(conn,BUFF_SIZE)
	return 
}

func (redis *FakeRedis) Write(cmd []byte) (err error) {
	_, err = redis.upstreamConn.Write(cmd)
	if err != nil {
		log.Printf("Fail to write command: %v\n",err)
		return
	}
	return
}

func (redis *FakeRedis) LoopRead() {
	defer redis.upstreamConn.Close()
	for {
		resp, err := redis.upstreamReader.ReadString('\n')

		if err != nil {
			log.Printf("Error while reading from master: %v\n", err)
			return
		}

		log.Printf("Result is: %v\n",resp)
	}


}