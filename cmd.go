package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
)

const (
	NoneResp = iota
	ErrorResp
	SimpleStringResp
	IntegerResp
	BulkResp
	MultiResp
	OtherResp
)

type RedisCommand struct {
	respType int 
	raw []byte
	bulkSize int64
}

func ReadLine(reader *bufio.Reader) (line []byte,err error) {
	line, err = reader.ReadBytes('\n')
	lineSize := len(line)
	if lineSize < 2 || line[lineSize-2] != '\r' { 
		log.Printf("invalid redis packet %s, err:%v", line, err)
		return nil, fmt.Errorf("invalid redis packet %s, err:%v", line, err)
	}
	return
}

func ByteToInt(b []byte) (n int64, err error) {
	n = 0
	sign := int64(1)
	for i := uint8(0); i < uint8(len(b)); i++ {
		if i == 0 && b[i] == '-' {
			if len(b) == 1 {
				return 0, fmt.Errorf("Invalid number %s", string(b))
			}
			sign = -1
			continue
		}

		if b[i] >= 0 && b[i] <= '9' {
			if i > 0 {
				n *= 10
			}
			n += int64(b[i]) - '0'
			continue
		}

		return 0, fmt.Errorf("Invalid number %s", string(b))
	}
	n = sign * n
	return n , nil
}

func ParseCommand(reader *bufio.Reader) (resp *RedisCommand,err error) {
	line, err := ReadLine(reader)
	if err != nil {
		return nil, err
	}
	resp = NewRedisCommand()

	switch line[0] {
	case '-':
		resp.respType = ErrorResp
		resp.Append(line)
		return resp,nil
	case '+':
		resp.respType = SimpleStringResp
		resp.Append(line)
		return resp, nil
	case ':':
		resp.respType = IntegerResp
		resp.Append(line)
		return resp, nil
	case '$':
		resp.respType = BulkResp
		size, err := ByteToInt(line[1 : len(line)-2])
		if err != nil {
			return nil, err
		}
		resp.Append(line)
		resp.bulkSize = size
		err = resp.ReadBulk(reader)
		if err != nil {
			return nil, err
		}
		return resp, nil
	case '*':
		resp.respType = MultiResp
		i, err := ByteToInt(line[1 : len(line)-2]) 
		if err != nil {
			return nil, err
		}
		resp.Append(line)
		if i >= 0 {
			for j := int64(0); j < i; j++ {
				rp, err := ParseCommand(reader)
				if err != nil {
					return nil, err
				}
				resp.Append(rp.raw)
			}
		}
		return resp, nil
	default:
		resp.respType = OtherResp
		resp.Append(line)
		return resp, nil
	}
	return resp,nil
}

func NewRedisCommand() (command* RedisCommand) {
	command = &RedisCommand {
		respType: NoneResp,
		raw: nil,
		bulkSize: 0,
	}
	return 
}

func (command *RedisCommand) Append(raw []byte) {
	if command.raw == nil {
		command.raw = raw
	} else {
		command.raw = append(command.raw,raw...)
	}
}

func (command *RedisCommand) ReadBulk(reader *bufio.Reader ) (err error) { 

	buf := make([]byte, command.bulkSize)
	if _, err = io.ReadFull(reader, buf); err != nil {
		return err
	}

	command.raw = append(command.raw, buf...)

	return nil
}