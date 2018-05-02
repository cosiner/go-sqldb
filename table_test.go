package sqldb

import (
	"fmt"
	"testing"
)

func TestSQLCreate(t *testing.T) {
	cfg := Parser{
		Default:         true,
		Notnull:         true,
		TablenamePrefix: "sqldb_",
		NameMapper:      SnakeCase,
	}
	table, err := cfg.StructTable(Column{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := cfg.SQLCreate(table)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(s)
}

func TestSnakeCase(t *testing.T) {
	cases := map[string]string{
		"AbcdEEf":   "abcd_eef",
		"abcdEEf":   "abcd_eef",
		"abcdEEfF":  "abcd_eef_f",
		"abcd_EEfF": "abcd__eef_f",
	}
	for s, expect := range cases {
		if got := SnakeCase(s); got != expect {
			t.Errorf("SnakeCase convert failed: %s, expect %s, got %s", s, expect, got)
		}
	}
}

func TestParse(t *testing.T) {
	type ExportedStr string
	type unexportedStr string
	type ExportedEmbed struct {
		V1  int
		Val uint32 // override
	}
	type unexportedEmbed struct {
		V2 string
	}
	type Stru struct {
		Val   bool
		Bytes []byte
		ExportedStr
		unexportedStr
		ExportedEmbed
		unexportedEmbed
	}

	cfg := Parser{
		Default:    true,
		Notnull:    true,
		NameMapper: SnakeCase,
	}
	table, err := cfg.StructTable(Stru{})
	if err != nil {
		t.Fatal(err)
	}
	expectCols := []string{
		"val", "bytes", "exported_str", "v1", "v2",
	}
	if len(expectCols) != len(table.Cols) {
		t.Fatal("colum count unmatched")
	}
	for i, c := range expectCols {
		if c != table.Cols[i].Name {
			t.Fatalf("colum name unmatched: %s, %s\n", c, table.Cols[i].Name)
		}
	}
}
