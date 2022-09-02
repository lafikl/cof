package cof

import (
	"encoding/binary"
	"io"
)

type IntColumn struct {
	min    int64
	max    int64
	values []int64
}

func NewIntCol() *IntColumn {
	return &IntColumn{
		min: 0,
		max: 0,
	}
}

func (c *IntColumn) Type() string {
	return "int"
}

func (i *IntColumn) Min() int64 {
	return i.min
}

func (i *IntColumn) Max() int64 {
	return i.max
}

func (i *IntColumn) Rows() int {
	return len(i.values)
}

func (i *IntColumn) Add(value interface{}) error {
	intVal, ok := value.(int64)
	if !ok {
		return ErrBadType
	}
	i.values = append(i.values, intVal)

	// update min and max values
	if intVal < i.min || i.min == 0 {
		i.min = intVal
	}
	if intVal > i.max {
		i.max = intVal
	}
	return nil
}

func (i *IntColumn) WriteTo(w io.Writer) (int64, error) {
	written := int64(0)
	buf := make([]byte, binary.MaxVarintLen64)
	for _, v := range i.values {
		n := binary.PutVarint(buf, v)
		nwritten, err := w.Write(buf[:n])
		if err != nil {
			return written, err
		}
		written += int64(nwritten)
	}
	return written, nil
}

func (i *IntColumn) Reset() error {
	i.values = []int64{}
	i.min = 0
	i.max = 0
	return nil
}
