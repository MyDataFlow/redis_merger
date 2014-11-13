package main

import (
	"errors"
	"io"
)

type SliceBuffer struct {
	data []byte
	size int
	pos int
}

func NewSliceBuffer(data []byte) (slice *SliceBuffer) {
	slice = &SliceBuffer{
		data: data,
		size: len(data),
		pos: 0,
	}
	return
}

func (slice *SliceBuffer) Slice(n int) (data []byte, err error) {
	if slice.pos + n > slice.size {
		return nil, io.EOF
	}
	data = slice.data[slice.pos : slice.pos+n]
	slice.pos += n
	return data, nil
}

func (slice *SliceBuffer) ReadByte() (data byte, err error) {
	if slice.pos >= slice.size {
		return 0, io.EOF
	}
	data = slice.data[slice.pos]
	slice.pos++
	return data, nil
}

func (slice *SliceBuffer) Read(b []byte) (size int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	if slice.pos >= slice.size {
		return 0, io.EOF
	}
	size = copy(b, slice.data[slice.pos :])
	slice.pos += size
	return size, nil
}

func (slice *SliceBuffer) Seek(offset int64, whence int) (size int64, err error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(slice.pos) + offset
	case 2:
		abs = int64(slice.size) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	if abs >= 1<<31 {
		return 0, errors.New("position out of range")
	}
	slice.pos = int(abs)
	return abs, nil
}
