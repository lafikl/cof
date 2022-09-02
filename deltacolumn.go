package cof

import (
	"encoding/binary"
	"io"
)

type DeltaColumn struct {
	min    int64
	max    int64
	base   int64
	first  bool
	values []int64
}

func NewDeltaCol() *DeltaColumn {
	return &DeltaColumn{
		min:   0,
		max:   0,
		base:  0,
		first: true,
	}
}

func (c *DeltaColumn) Type() string {
	return "delta"
}

func (i *DeltaColumn) Min() int64 {
	return i.min
}

func (i *DeltaColumn) Max() int64 {
	return i.max
}

func (i *DeltaColumn) Rows() int {
	return len(i.values)
}

func (i *DeltaColumn) Add(value interface{}) error {
	intVal, ok := value.(int64)
	if !ok {
		return ErrBadType
	}

	i.values = append(i.values, intVal-i.base)

	// update min and max values
	if intVal < i.min || i.min == 0 {
		i.min = intVal
	}
	if intVal > i.max {
		i.max = intVal
	}

	if i.first {
		i.base = intVal
		i.first = false
	}

	return nil
}

func (i *DeltaColumn) WriteTo(w io.Writer) (int64, error) {
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

func (i *DeltaColumn) Reset() error {
	i.values = []int64{}
	i.min = 0
	i.max = 0
	i.base = 0
	i.first = true
	return nil
}
