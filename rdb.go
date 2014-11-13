package main 

import (
	"fmt"
	"bufio"
	"io"
	"log"
	"strconv"
	"encoding/binary"
	"bytes"
)

const (
	RDBOpDB         = 0xFE
	RDBOpExpirySec  = 0xFD
	RDBOpExpiryMSec = 0xFC
	RDBOpEOF        = 0xFF

	RDBLen6Bit  = 0x0
	RDBLen14bit = 0x1
	RDBLen32Bit = 0x2
	RDBLenEnc   = 0x3

	RDBString    = 0x00
	RDBList      = 0x01
	RDBSet       = 0x02
	RDBZset      = 0x03
	RDBHash      = 0x04
	RDBZipmap    = 0x09
	RDBZiplist   = 0x0a
	RDBIntset    = 0x0b
	RDBSortedSet = 0x0c
	RDBHashmap   = 0x0d
)

type RedisRDB struct {
		reader *bufio.Reader
}


// Taken from Golly: https://github.com/tav/golly/blob/master/lzf/lzf.go
// Removed part that gets outputLength from data
func lzfDecompress(input []byte, outputLength uint32) (output []byte) {

	inputLength := uint32(len(input))

	var backref int64
	var ctrl, iidx, length, oidx uint32

	output = make([]byte, outputLength, outputLength)
	iidx = 0

	for iidx < inputLength {
		// Get the control byte.
		ctrl = uint32(input[iidx])
		iidx++

		if ctrl < (1 << 5) {
			// The control byte indicates a literal reference.
			ctrl++
			if oidx+ctrl > outputLength {
				return nil
			}

			// Safety check.
			if iidx+ctrl > inputLength {
				return nil
			}

			for {
				output[oidx] = input[iidx]
				iidx++
				oidx++
				ctrl--
				if ctrl == 0 {
					break
				}
			}
		} else {
			// The control byte indicates a back reference.
			length = ctrl >> 5
			backref = int64(oidx - ((ctrl & 31) << 8) - 1)

			// Safety check.
			if iidx >= inputLength {
				return nil
			}

			// It's an extended back reference. Read the extended length before
			// reading the full back reference location.
			if length == 7 {
				length += uint32(input[iidx])
				iidx++
				// Safety check.
				if iidx >= inputLength {
					return nil
				}
			}

			// Put together the full back reference location.
			backref -= int64(input[iidx])
			iidx++

			if oidx+length+2 > outputLength {
				return nil
			}

			if backref < 0 {
				return nil
			}

			output[oidx] = output[backref]
			oidx++
			backref++
			output[oidx] = output[backref]
			oidx++
			backref++

			for {
				output[oidx] = output[backref]
				oidx++
				backref++
				length--
				if length == 0 {
					break
				}
			}

		}
	}

	return output
}

func NewRedisRDB(reader *bufio.Reader) (rdb *RedisRDB) {
	rdb = &RedisRDB{
		reader: reader,
	}
	return
}


func (rdb *RedisRDB) readBytes(n uint32) (result []byte, err error) {
	result = make([]byte, n)
	_, err = io.ReadFull(rdb.reader, result)
	return
}

func (rdb *RedisRDB) readLength() (length uint32, encoding int8, err error) {
	prefix, err := rdb.reader.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	kind := (prefix & 0xC0) >> 6

	switch kind {
	case RDBLen6Bit:
		length = uint32(prefix & 0x3F)
		return length, -1, nil
	case RDBLen14bit:
		data, err := rdb.reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		length = ((uint32(prefix) & 0x3F) << 8) | uint32(data)
		return length, -1, nil
	case RDBLen32Bit:
		data, err := rdb.readBytes(4)
		if err != nil {
			return 0, 0, err
		}
		length = binary.BigEndian.Uint32(data)
		return length, -1, nil
	case RDBLenEnc:
		encoding = int8(prefix & 0x3F)
		return 0, encoding, nil
	}
	panic("never reached")
}

func (rdb *RedisRDB) readString() (str string, err error) {

	length, encoding, err := rdb.readLength()
	if err != nil {
		return "", err
	}

	switch encoding {
	// length-prefixed string
	case -1:
		data, err := rdb.readBytes(length)
		if err != nil {
			return "", err
		}
		str = string(data)
	case 0, 1, 2:
		data, err := rdb.readBytes(1 << uint8(encoding))
		if err != nil {
			return "", err
		}

		var num uint32

		if encoding == 0 {
			num = uint32(data[0])
		} else if encoding == 1 {
			num = uint32(data[0] | (data[1] << 8))
		} else if encoding == 2 {
			num = uint32(data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24))
		}

		str = fmt.Sprintf("%d", num)
	// compressed string
	case 3:
		clength, _, err := rdb.readLength()
		if err != nil {
			return "", err
		}
		length, _, err := rdb.readLength()
		if err != nil {
			return "", err
		}
		data, err := rdb.readBytes(clength)
		if err != nil {
			return "", err
		}
		str = string(lzfDecompress(data, length))
	default:
		return "", fmt.Errorf("Unknown encoding type")
	}

	return str, nil
}

//Discard header
func (rdb *RedisRDB) readHeader() (err error) {
	_, err = rdb.readBytes(5)
	if err != nil {
		return  err
	}

	_, err = rdb.readBytes(4)
	if err != nil {
		return err
	}

	return nil
}
//Discard database
func (rdb *RedisRDB) readDatabase() (err error) {
	rdb.reader.ReadByte()
	_, _, err = rdb.readLength()
	if err != nil {
		return err
	}
	return nil
}

func (rdb *RedisRDB) readExpirySec() (exp string, err error) {
	expiry, err := rdb.readBytes(4)
	if err != nil {
		return "", err
	}
	return string(expiry),nil
}

func (rdb *RedisRDB) readExpiryMSec() (exp string, err error) {
	expiry, err :=  rdb.readBytes(8)
	if err != nil {
		return "", err
	}
	return string(expiry),nil
}


//Read List Or Set
func (rdb *RedisRDB)  readSetOrList() (strs []string, err error) {
	length, _, err := rdb.readLength()
	if err != nil {
		return nil, err
	}

	var i uint32
	strs = make([]string,length)
	for i = 0; i < length; i++ {
		// list element
		str, err := rdb.readString()
		if err != nil {
			return nil, err
		}
		strs[i] = str
	}
	return strs, nil
}

//Read Hash 
func (rdb *RedisRDB) readHash() (hash map[string]string, err error) {
	length, _, err := rdb.readLength()
	if err != nil {
		return nil, err
	}

	hash = make(map[string]string)
	var i uint32

	for i = 0; i < length; i++ {
		// key
		key, err := rdb.readString()
		if err != nil {
			return nil, err
		}

		// value
		value, err := rdb.readString()
		if err != nil {
			return nil, err
		}
		hash[key] = value
	}

	return hash, nil
}

//Read ZSet

func (rdb *RedisRDB) readZset() (hash map[string]string, err error) {
	length, _, err := rdb.readLength()
	if err != nil {
		return nil, err
	}

	var i uint32
	hash = make(map[string]string)
	for i = 0; i < length; i++ {
		value, err := rdb.readString()
		if err != nil {
			return nil, err
		}

		score, err := rdb.readString()
		if err != nil {
			return nil, err
		}
		hash[value] = score
	}
	return hash,nil
}


func (rdb *RedisRDB) WaitRDBToChannel(ch chan *RedisCommand) {
		ReadLine(rdb.reader)
		log.Printf("waiting for rdb sync")
		if err := rdb.readHeader() ; err != nil{
			log.Printf("%v",err)
			return
		}
		if err := rdb.readDatabase() ; err != nil {
			log.Printf("%v",err)
			return
		}
		
		timeout := ""

		for {
			op,err := rdb.reader.ReadByte()
			if err != nil {
				log.Printf("%v",err)
				return
			}
			switch op {
			case RDBOpExpirySec:
				exp,err :=  rdb.readExpirySec()
				if err !=  nil {
					log.Printf("%v",err)
					return
				}
				timeout = exp
			case RDBOpExpiryMSec:
				exp,err :=  rdb.readExpirySec()
				if err !=  nil {
					log.Printf("%v",err)
					return
				}
				expInt,err := strconv.Atoi(exp)
				if err !=  nil {
					log.Printf("%v",err)
					return
				}
				timeout = fmt.Sprintf("%d",expInt)
			case RDBString:
				key,err := rdb.readString()
				if err !=  nil {
					log.Printf("%v",err)
					return
				}
				value,err := rdb.readString()
				if err !=  nil {
					log.Printf("%v",err)
					return
				}
				command := newSETCommand(key,value)
				ch <- command

				if timeout != "" {
					command = newEXPIREATCommand(key,timeout)
					ch <- command
					timeout = ""
				}

			case RDBHash:
				key,err := rdb.readString()
				if err !=  nil {
					log.Printf("%v",err)
					return
				}

				value,err := rdb.readHash()
				if err !=  nil {
					log.Printf("%v",err)
					return
				}

				for innerKey,innerValue := range value {
					command := newHSETCommand(key,innerKey,innerValue)
					ch <- command
				}

				if timeout != "" {
					command := newEXPIREATCommand(key,timeout)
					ch <- command
					timeout = ""
				}
			case RDBZipmap:
				key,err := rdb.readString()
				if err !=  nil {
					log.Printf("%v",err)
					return
				}
				zip,err := rdb.readString()
				if err !=  nil {
					log.Printf("%v",err)
					return
				}
				value,err :=  ParseZipmap(string(zip))
				if err !=  nil {
					log.Printf("%v",err)
					return
				}
				for innerKey,innerValue := range value {
					command := newHSETCommand(key,innerKey,innerValue)
					ch <- command
				}
			case RDBOpEOF:
				return
		}
	}
}

func newEXPIREATCommand(key string,value string) (command *RedisCommand) {
	command = NewRedisCommand()
	keySize := len(key)
	valueSize := len(value)
	cmdString := fmt.Sprintf("*3\r\n$8\r\nEXPIREAT\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
						keySize,key,valueSize,value)
	buf := bytes.NewBufferString(cmdString)
	command.raw = buf.Bytes()
	command.respType = SimpleStringResp
	return 
}

func newHSETCommand(key string,innerKey string,innerValue string) (command *RedisCommand) {
	command = NewRedisCommand()
	keySize := len(key)
	innerKeySize := len(innerKey)
	innerValueSize := len(innerValue)
	cmdString := fmt.Sprintf("*4\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
						keySize,key,innerKeySize,innerKey,innerValueSize,innerValue)
	buf := bytes.NewBufferString(cmdString)
	command.raw = buf.Bytes()
	command.respType = SimpleStringResp
	return 
}

func newSETCommand(key string,value string) (command *RedisCommand) {
	command = NewRedisCommand()
	keySize := len(key)
	valueSize := len(value)
	cmdString := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",keySize,key,valueSize,value)
	buf := bytes.NewBufferString(cmdString)
	command.raw = buf.Bytes()
	command.respType = SimpleStringResp
	return 
}
