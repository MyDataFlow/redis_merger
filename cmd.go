package main

import (
	"bufio"
	"fmt"
)

type RedisCommand struct {
	raw []byte
	command string
}

func ReadLine(reader *bufio.Reader) (line []byte,err error) {
	line, err = reader.ReadBytes('\n')
	lineSize := len(line)
	if lineSize < 2 || line[lineSize-2] != '\r' { 
		return nil, fmt.Errorf("invalid redis packet %v, err:%v", line, err)
	}
	line = line[:lineSize-2]
	return
}