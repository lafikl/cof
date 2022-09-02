package cof

import (
	"bytes"
	"encoding/binary"
	"io"
)

type stringIterator struct {
	buf    []byte
	vector []ColumnValue
	pos    int64
	offset int64
	end    int64
	size   int
	rdr    *bytes.Reader
}

func NewStringIterator(vectorSize int, start, end int64, buf []byte) Iterator {
	vals := make([]ColumnValue, vectorSize)

	return &stringIterator{
		buf:    buf,
		vector: vals,
		offset: int64(start),
		pos:    start,
		end:    end,
		rdr:    bytes.NewReader(buf[start:end]),
		size:   vectorSize,
	}
}

func (iter *stringIterator) Reset() {
	for idx, cv := range iter.vector {
		cv.Reset()
		iter.vector[idx] = cv
	}
}

func (iter *stringIterator) Next() (*[]ColumnValue, error) {
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

// func (iter *stringIterator) Next2() (*[]ColumnValue, error) {
// 	iter.Reset()
// 	for i := 0; i < iter.size; i++ {
// 		if iter.pos >= iter.end {
// 			return &iter.vector, io.EOF
// 		}
// 		length := binary.LittleEndian.Uint32(iter.buf[iter.pos : iter.pos+4])
// 		iter.pos += 4

// 		strVal := string(iter.buf[iter.pos : iter.pos+int64(length)])
// 		iter.pos += int64(length)
// 		iter.vector[i] = ColumnValue{Index: i, Value: strVal}

// 	}
// 	return &iter.vector, nil
// }

func (iter *stringIterator) Err() error {
	return nil
}

func (iter *stringIterator) Pos() int64 {
	return iter.pos
}
