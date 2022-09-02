package cof

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Shard struct {
	columns        map[string]Column
	columnsOffsets []ColOffset
}

type BlockHeader struct {
	Min  int64
	Max  int64
	Rows int64
	Size int64
}

type Field struct {
	Name string
	Type string
}

type ColOffset struct {
	Name   string
	Offset int64
}

func newShard(id string) *Shard {
	return &Shard{
		columns:        map[string]Column{},
		columnsOffsets: []ColOffset{},
	}
}

func (s *Shard) AddColumn(name string, col Column) {
	s.columns[name] = col
}

func (s *Shard) Flush(w io.Writer) error {
	// @TODO(KL): write block metadata and simple indexes
	buffer := new(bytes.Buffer)
	for name, col := range s.columns {
		written, err := col.WriteTo(buffer)
		if err != nil {
			return err
		}
		fmt.Println(name, written)
		s.columnsOffsets = append(s.columnsOffsets, ColOffset{
			Name:   name,
			Offset: written,
		})
	}
	block := BlockHeader{
		Min:  0,
		Max:  0,
		Rows: 0,
		Size: 0,
	}
	err := binary.Write(w, binary.LittleEndian, block)
	if err != nil {
		return err
	}

	return nil
}
