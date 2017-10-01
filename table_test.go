package sqldb

import (
	"fmt"
	"testing"
)

func TestSQLCreate(t *testing.T) {
	cfg := Config{
		Default: true,
		Notnull: true,
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
