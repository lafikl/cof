package cof

import (
	"encoding/binary"
	"io"

	"github.com/golang/snappy"

	"github.com/golang/protobuf/proto"
	"github.com/lafikl/cof/types"
)

type Reader struct {
	buf    []byte
	footer *types.Footer
}

func NewReader(buf []byte) *Reader {
	return &Reader{
		buf:    buf,
		footer: &types.Footer{},
	}
}

type GroupIterator struct {
	iterators   map[string]Iterator
	size        int
	records     []Record
	TotalRows   int64
	fetchedRows int64
}

func (gi *GroupIterator) Next() ([]Record, int, error) {
	gi.records = make([]Record, gi.size)
	rowsWritten := 0
	for name, iter := range gi.iterators {
		vals, err := iter.Next()
		if err != nil && err != io.EOF {
			return nil, rowsWritten + 1, err
		}

		for idx, value := range *vals {
			if value.Value == nil {
				continue
			}
			if gi.records[idx] == nil {
				gi.records[idx] = Record{}
			}
			gi.records[idx][name] = value.Value
			if rowsWritten < idx {
				rowsWritten = idx
			}
		}
		if err == io.EOF {
			return gi.records, rowsWritten + 1, err
		}
	}
	return gi.records, rowsWritten + 1, nil
}

func (r *Reader) DecodeGroup(group *types.Group) ([]byte, error) {
	return snappy.Decode([]byte{}, r.buf[group.Start:group.End])
}

func (r *Reader) ReadGroup(group *types.Group, size int, columns ...string) (*GroupIterator, error) {
	iters := map[string]Iterator{}
	// decode group
	for _, column := range group.Columns {
		// filter unwanted columns
		wanted := false
		for _, wantedCol := range columns {
			if wantedCol == column.Name {
				wanted = true
			}
		}
		if !wanted {
			continue
		}

		switch column.Type {
		case "string":
			iters[column.Name] = NewStringIterator(size,
				column.Start, column.End, r.buf)
		case "int":
			iters[column.Name] = NewIntIterator(size,
				column.Start, column.End, r.buf)
		case "bitset":
			iter, err := NewBitsetIterator(size,
				column.Start, column.End, r.buf)
			if err != nil {
				return nil, err
			}
			iters[column.Name] = iter
		case "delta":
			iters[column.Name] = NewDeltaIterator(size,
				column.Start, column.End, r.buf)

		default:
			// panic(fmt.Sprintf("unknown type: %s", column.Type))
		}
	}

	gi := &GroupIterator{
		iterators:   iters,
		size:        size,
		TotalRows:   group.NumRows,
		fetchedRows: int64(0),
	}
	return gi, nil
}

func (r *Reader) ReadFooter() error {
	offset := binary.LittleEndian.Uint64(r.buf[len(r.buf)-8:])

	// read footer
	err := proto.Unmarshal(r.buf[offset:len(r.buf)-8], r.footer)
	if err != nil {
		return err
	}
	return nil
}

func (r *Reader) Footer() *types.Footer {
	return r.footer
}
