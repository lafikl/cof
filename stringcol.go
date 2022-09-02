package cof

import (
	"encoding/binary"
	"fmt"
	"io"
)

const MaxSize = 1e9

type StringCol struct {
	dict   map[string]int64
	values []int64
}

func NewStringCol() *StringCol {
	return &StringCol{
		dict:   map[string]int64{},
		values: []int64{},
	}
}

func (c *StringCol) Type() string {
	return "string"
}

func (s *StringCol) Rows() int {
	return len(s.values)
}

func (c *StringCol) Add(ival interface{}) error {
	stringVal, ok := ival.(string)
	if !ok {
		return ErrBadType
	}
	idx, ok := c.dict[stringVal]
	if !ok {
		idx = int64(len(c.dict))
		c.dict[stringVal] = idx
	}
	c.values = append(c.values, idx)
	return nil
}

func (c *StringCol) WriteTo(w io.Writer) (int64, error) {
	written := int64(0)

	buf := make([]byte, binary.MaxVarintLen64)
	for _, val := range c.values {
		n := binary.PutVarint(buf, val)
		nwritten, err := w.Write(buf[:n])
		if err != nil {
			return written, err
		}
		written += int64(nwritten)

	}
	fmt.Println("Dict Len", len(c.dict))
	return written, nil
}

func (c *StringCol) Dict() map[string]int64 {
	return c.dict
}

// implement dictionary encoding
// or prefix removal
// and add needles
// func (c *StringCol) WriteTo2(w io.Writer) (int64, error) {
// 	written := int64(0)
// 	for _, val := range c.values {
// 		// writing the length of the string
// 		err := binary.Write(w, binary.LittleEndian, uint32(len(val)))
// 		if err != nil {
// 			return written, err
// 		}
// 		written += int64(binary.Size(uint32(len(val))))
// 		// writing the string values {length, bytes}
// 		nwritten, err := w.Write([]byte(val))
// 		if err != nil {
// 			return written, err
// 		}

// 		// update the total written bytes counter
// 		written += int64(nwritten)
// 	}
// 	return written, nil
// }

func (c *StringCol) ReadFrom(rdr io.Reader) []string {
	return []string{}
}

func (c *StringCol) Reset() error {
	c.values = []int64{}
	return nil
}
