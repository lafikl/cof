package cof

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

type intIterator struct {
	buf    []byte
	vector []ColumnValue
	pos    int64
	end    int64
	size   int
	rdr    *bytes.Reader
}

func NewIntIterator(vectorSize int, start, end int64, buf []byte) Iterator {
	vals := make([]ColumnValue, vectorSize)
	return &intIterator{
		buf:    buf,
		vector: vals,
		pos:    start,
		end:    end,
		rdr:    bytes.NewReader(buf[start:end]),
		size:   vectorSize,
	}
}

func (iter *intIterator) Reset() {
	for idx, cv := range iter.vector {
		cv.Reset()
		iter.vector[idx] = cv
	}
}

func (iter *intIterator) Next() (*[]ColumnValue, error) {
	iter.Reset()
	for i := 0; i < iter.size; i++ {
		intVal, err := binary.ReadVarint(iter.rdr)
		if err == io.EOF {
			return &iter.vector, io.EOF
		}
		if err != nil {
			return nil, err
		}

		iter.vector[i] = ColumnValue{Index: i, Value: intVal}

	}
	return &iter.vector, nil
}

func (iter *intIterator) Err() error {
	return nil
}

func (iter *intIterator) Pos() int64 {
	pos, err := iter.rdr.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Println(err)
	}
	return pos
}
