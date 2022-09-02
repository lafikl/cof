package cof

import (
	"github.com/RoaringBitmap/roaring"
)

type bitsetIterator struct {
	size   int
	rows   int
	rbm    *roaring.Bitmap
	vector []ColumnValue
}

func NewBitsetIterator(vectorSize int, start, end int64, buf []byte) (Iterator, error) {
	rbm := roaring.New()
	err := rbm.UnmarshalBinary(buf[start:end])
	if err != nil {
		return nil, err
	}

	vals := make([]ColumnValue, vectorSize)

	return &bitsetIterator{
		rows:   0,
		size:   vectorSize,
		vector: vals,
		rbm:    rbm,
	}, nil
}

func (iter *bitsetIterator) Reset() {
	for idx, cv := range iter.vector {
		cv.Reset()
		iter.vector[idx] = cv
	}
}

func (iter *bitsetIterator) Next() (*[]ColumnValue, error) {
	iter.Reset()
	for i := 0; i < iter.size; i++ {
		val := iter.rbm.ContainsInt(iter.rows)
		iter.vector[i] = ColumnValue{Index: i, Value: val}
		iter.rows++
	}
	return &iter.vector, nil
}

func (iter *bitsetIterator) Err() error {
	return nil
}

func (iter *bitsetIterator) Pos() int64 {
	return 0
}
