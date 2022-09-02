package cof

import (
	"errors"
	"io"
)

var (
	ErrBadType = errors.New("given value was of a bad type")
)

type ColumnValue struct {
	Index int
	Value interface{}
}

func (c *ColumnValue) Reset() {
	c.Index = 0
	c.Value = nil
}

type Column interface {
	Type() string
	WriteTo(io.Writer) (int64, error)
	Reset() error
	Add(value interface{}) error
	Rows() int
}

type Iterator interface {
	Next() (*[]ColumnValue, error)
	Err() error
	Pos() int64
}

type MinMaxer interface {
	Max() int64
	Min() int64
}
