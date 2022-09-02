package cof

import (
	"encoding/binary"
	"io"
)

type RLEDeltaColumn struct {
	min      int64
	max      int64
	base     int64
	curRun   int64
	curDelta int64
	first    bool
	values   []rleVal
}

type rleVal struct {
	run int64
	val int64
}

func NewRLEDeltaCol() *RLEDeltaColumn {
	return &RLEDeltaColumn{
		min:      0,
		max:      0,
		base:     0,
		curRun:   0,
		curDelta: 0,
		first:    true,
	}
}

func (c *RLEDeltaColumn) Type() string {
	return "rledelta"
}

func (i *RLEDeltaColumn) Min() int64 {
	return i.min
}

func (i *RLEDeltaColumn) Max() int64 {
	return i.max
}

func (i *RLEDeltaColumn) Rows() int {
	return len(i.values)
}

func (i *RLEDeltaColumn) Add(value interface{}) error {
	intVal, ok := value.(int64)
	if !ok {
		return ErrBadType
	}

	// update min and max values
	if intVal < i.min || i.min == 0 {
		i.min = intVal
	}
	if intVal > i.max {
		i.max = intVal
	}

	if len(i.values) == 0 {
		i.base = intVal
		i.values = append(i.values, rleVal{run: 1, val: intVal})
		i.curDelta = intVal
		return nil
	}

	delta := intVal - i.base
	if delta == i.curDelta {
		i.curRun++
		return nil
	}

	rv := rleVal{run: i.curRun, val: i.curDelta}

	i.values = append(i.values, rv)
	i.curDelta = 0
	i.curRun = 0
	return nil
}

// WriteTo compresses and writes the encoded data to w
func (i *RLEDeltaColumn) WriteTo(w io.Writer) (int64, error) {
	written := int64(0)

	buf := make([]byte, binary.MaxVarintLen64)
	for _, v := range i.values {

		// write the run length
		n := binary.PutVarint(buf, v.run)
		nwritten, err := w.Write(buf[:n])
		if err != nil {
			return written, err
		}
		written += int64(nwritten)

		// write the delta value
		n = binary.PutVarint(buf, v.val)
		nwritten, err = w.Write(buf[:n])
		if err != nil {
			return written, err
		}
		written += int64(nwritten)
	}
	return written, nil
}

func (i *RLEDeltaColumn) Reset() error {
	i.values = []rleVal{}
	i.min = 0
	i.max = 0
	i.base = 0
	i.first = true
	return nil
}
