package cof

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

type DeltaIterator struct {
	buf    []byte
	vector []ColumnValue
	pos    int64
	end    int64
	base   int64
	first  bool
	size   int
	rdr    *bytes.Reader
}

func NewDeltaIterator(vectorSize int, start, end int64, buf []byte) Iterator {
	vals := make([]ColumnValue, vectorSize)
	return &DeltaIterator{
		buf:    buf,
		vector: vals,
		pos:    start,
		end:    end,
		base:   0,
		first:  true,
		rdr:    bytes.NewReader(buf[start:end]),
		size:   vectorSize,
	}
}

func (iter *DeltaIterator) Reset() {
	for idx, cv := range iter.vector {
		cv.Reset()
		iter.vector[idx] = cv
	}
}

func (iter *DeltaIterator) Next() (*[]ColumnValue, error) {
	iter.Reset()
	for i := 0; i < iter.size; i++ {
		intVal, err := binary.ReadVarint(iter.rdr)
		if err == io.EOF {
			return &iter.vector, io.EOF
		}
		if err != nil {
			return nil, err
		}

		iter.vector[i] = ColumnValue{
			Index: i,
			Value: intVal + iter.base,
		}

		// load prefix value
		if iter.first {
			iter.base = intVal
			iter.first = false
		}
	}
	return &iter.vector, nil
}

func (iter *DeltaIterator) Err() error {
	return nil
}

func (iter *DeltaIterator) Pos() int64 {
	pos, err := iter.rdr.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Println(err)
	}
	return pos
}
