package cof

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/lafikl/cof/types"
)

func TestFlush(t *testing.T) {
	table := Table{
		"url":  NewStringCol(),
		"time": NewIntCol(),
	}
	s, err := New(table)
	if err != nil {
		t.Fatal(err)
	}
	records := []Record{}
	r := Record{
		"url":  "https://gooogle.com",
		"time": int64(148222),
	}
	r1 := Record{
		"time": int64(8888),
	}
	records = append(records, r)
	records = append(records, r1)
	err = s.Batch(records)
	if err != nil {
		t.Fatal(err)
	}

	buf := new(bytes.Buffer)

	_, err = s.WriteGroup(buf)
	if err != nil {
		t.Fatal(err)
	}

	err = s.ResetAll()
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.WriteFooter(buf)
	if err != nil {
		t.Fatal(err)
	}

	// reading what we wrote
	rdr := NewReader(buf.Bytes())
	err = rdr.ReadFooter()
	if err != nil {
		t.Fatal(err)
	}

	footer := rdr.Footer()

	iter, err := rdr.ReadGroup(footer.Groups[0], 10, "url", "time")
	if err != nil {
		t.Fatal(err)
	}
	outRecords, _, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("fetched records", len(outRecords))
	fmt.Println(outRecords)

}

func TestFooter(t *testing.T) {
	table := Table{
		"url":  NewStringCol(),
		"time": NewIntCol(),
	}
	s, err := New(table)
	if err != nil {
		t.Fatal(err)
	}

	group := &types.Group{
		NumRows:    10,
		NumColumns: 4,
		Min:        2,
		Max:        50,
		Columns: []*types.Column{
			&types.Column{
				Name:  "url",
				Type:  "string",
				Start: 0,
				End:   200,
			},
		},
	}
	s.Footer.Groups = append(s.Footer.Groups, group)
	buf := new(bytes.Buffer)
	offset, err := s.WriteFooter(buf)
	fmt.Println("written", offset, err, proto.Size(s.Footer))

	rdr := NewReader(buf.Bytes())
	err = rdr.ReadFooter()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(rdr.Footer())
}
