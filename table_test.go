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
