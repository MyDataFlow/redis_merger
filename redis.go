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

var (
	SYNC_CMD = []byte("SYNC\r\n")
	PING_CMD = []byte("PING\r\n")
)

type FakeRedis struct {
	gid            int
	host		   string
	port           int
	upstreamReader *bufio.Reader
	persistentFile *os.File
	upstreamConn 	 net.Conn

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

func (redis *FakeRedis) Read() (command *RedisCommand,err error) {
	command, err = ParseCommand(redis.upstreamReader)
	if err != nil {
		return nil,err
	}
	return
}

func (redis *FakeRedis) Close() (err error){
	if redis.upstreamConn != nil {
		err = redis.upstreamConn.Close()
		if err != nil {
			return err
		}
		redis.upstreamConn = nil
		return nil
	}
	return fmt.Errorf("Already Close")
}



func (redis *FakeRedis) WaitChannelToConn(ch chan *RedisCommand) {
	for {
		cmd := <- ch
		if cmd.respType != ErrorResp && cmd.respType != OtherResp {
		
				log.Printf("Write command: %s",cmd.raw)
				_,err := redis.upstreamConn.Write(cmd.raw)
				if err != nil {
					log.Printf("Write Error: %s",err)
				}

		}
	}
}

func (redis *FakeRedis) WaitConnToChannel(ch chan *RedisCommand) {
	defer redis.upstreamConn.Close()
	for {
		resp, err := ParseCommand(redis.upstreamReader)

		if err != nil {
			log.Printf("Error while reading from master: %v\n", err)
			return
		}
		ch <- resp
	}
}