// Package cof only writes to data to files
// it has no idea how you fetch data
// reading and fetching is implemented by
// fetcher
package cof

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"

	"github.com/lafikl/cof/types"
)

var (
	ErrNoTimeColumn = errors.New(`missing "time" column in Table`)
)

type Store struct {
	BlockSize int
	schema    Table
	offset    int64
	BatchSize int
	Footer    *types.Footer
}

type Table map[string]Column

type Record map[string]interface{}

func New(schema Table) (*Store, error) {
	if _, ok := schema["time"]; !ok {
		return nil, ErrNoTimeColumn
	}
	return &Store{
		BlockSize: 1e4,
		schema:    schema,
		Footer:    &types.Footer{},
	}, nil
}

func (s *Store) getMinMax() (min, max int64) {
	mm, ok := s.schema["time"].(MinMaxer)
	if !ok {
		panic("time column is not MinMaxer")
	}
	return mm.Min(), mm.Max()
}

func (s *Store) WriteGroup(w io.Writer) (int64, error) {
	min, max := s.getMinMax()
	group := &types.Group{
		Min:        min,
		Max:        max,
		Start:      s.offset,
		NumColumns: int64(len(s.schema)),
		NumRows:    int64(0),
	}

	for name, column := range s.schema {
		if column.Rows() == 0 {
			continue
		}
		written, err := column.WriteTo(w)
		if err != nil {
			return s.offset, err
		}

		group.NumRows = int64(column.Rows())

		group.Columns = append(group.Columns, &types.Column{
			Name:  name,
			Type:  column.Type(),
			Start: s.offset,
			End:   s.offset + written,
		})
		s.offset += written
	}
	group.End = s.offset
	s.Footer.Groups = append(s.Footer.Groups, group)
	s.BatchSize = 0
	err := s.ResetAll()
	return s.offset, err
}

func (s *Store) ResetAll() error {
	for name, column := range s.schema {
		err := column.Reset()
		if err != nil {
			return fmt.Errorf("%s: %s", name, err)
		}
	}
	return nil
}

func (s *Store) WriteFooter(w io.Writer) (int64, error) {
	out, err := proto.Marshal(s.Footer)
	if err != nil {
		return s.offset, err
	}

	written, err := w.Write(out)
	if err != nil {
		return s.offset, err
	}

	// writing the start of the footer in the last 8 bytes
	foffset := make([]byte, 8)
	binary.LittleEndian.PutUint64(foffset, uint64(s.offset))
	s.offset += int64(written)
	written, err = w.Write(foffset)
	if err != nil {
		return s.offset, err
	}
	s.offset += int64(written)

	// @TODO(KL): put a crc checksum

	return s.offset, nil
}

func (s *Store) Batch(records []Record) error {
	var err error
	for _, record := range records {
		for k, v := range record {
			if _, ok := s.schema[k]; !ok {
				continue
			}
			err = s.schema[k].Add(v)
			if err != nil {
				return fmt.Errorf("%s=%s: %s", k, v, err)
			}
		}
	}
	s.BatchSize += len(records)
	return nil
}

func (s *Store) ShouldFlush() bool {
	if s.BatchSize >= s.BlockSize {
		return true
	}
	return false
}
