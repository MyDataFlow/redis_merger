package main

import (
	"bytes"
	"fmt"
	"encoding/binary"
)

func ParseZipmap(zip string) (hash map[string]string , err error) {
	length := 0
	b := bytes.NewBufferString(zip)
	buf := NewSliceBuffer(b.Bytes())
	lenByte, err := buf.ReadByte()
	if err != nil {
		return nil,err
	} 
	hash = make(map[string]string)
	// we need to count the items manually
	if lenByte >= 254 {
		length, err = countZipmapItems(buf)
		length /= 2
		if err != nil {
			return nil,err
		}
	} else {
		length = int(lenByte)
	}

	for i := 0; i < length; i++ {
		field, err := readZipmapItem(buf, false)
		if err != nil {
			return nil,err
		}
		value, err := readZipmapItem(buf, true)
		if err != nil {
			return nil,err
		}
		hash[string(field)] = string(value)
	}
	return hash,nil
}

func readZipmapItem(buf *SliceBuffer, readFree bool) ([]byte, error) {
	length, free, err := readZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func countZipmapItems(buf *SliceBuffer) (int, error) {
	n := 0
	for {
		strLen, free, err := readZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}

func readZipmapItemLength(buf *SliceBuffer,readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, fmt.Errorf("invalid zipmap item length")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}
	return int(b), int(free), err
}
