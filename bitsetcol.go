package cof

import (
	"io"

	"github.com/RoaringBitmap/roaring"
)

type BitsetColumn struct {
	rows int
	rbm  *roaring.Bitmap
}

func NewBitsetCol() *BitsetColumn {
	return &BitsetColumn{
		rbm:  roaring.BitmapOf(),
		rows: 0,
	}
}

func (c *BitsetColumn) Type() string {
	return "bitset"
}

func (i *BitsetColumn) Rows() int {
	return i.rows
}

func (i *BitsetColumn) Add(value interface{}) error {
	boolVal, ok := value.(bool)
	if !ok {
		return ErrBadType
	}

	if !boolVal {
		i.rows++
		return nil
	}

	i.rbm.AddInt(i.rows)
	i.rows++
	return nil
}

func (i *BitsetColumn) WriteTo(w io.Writer) (int64, error) {
	return i.rbm.WriteTo(w)
}

func (i *BitsetColumn) Reset() error {
	i.rows = 0
	i.rbm = roaring.BitmapOf()
	return nil
}
