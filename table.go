package sqldb

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Column struct {
	Name         string
	Type         string
	Precision    string
	DBType       string
	Primary      bool
	AutoIncr     bool
	Notnull      bool
	Default      bool
	DefaultVal   string
	Unique       bool
	UniqueName   string
	ForeignTable string
	ForeignCol   string
}

type Table struct {
	Name string
	Cols []Column
}

type Config struct {
	DBDialect       DBDialect
	FieldTag        string
	Default         bool
	Notnull         bool
	TablenamePrefix string
	NameMapper      NameMapper
}

func (c *Config) initDefault() {
	if c.DBDialect == nil {
		c.DBDialect = Postgres{}
	}
	if c.FieldTag == "" {
		c.FieldTag = "sqldb"
	}
	if c.NameMapper == nil {
		c.NameMapper = SnakeCase
	}
}

func (c *Config) CreateTables(db *sql.DB, models ...interface{}) error {
	for _, mod := range models {
		table, err := c.StructTable(mod)
		if err != nil {
			return err
		}
		s, err := c.SQLCreate(table)
		if err != nil {
			return fmt.Errorf("%s: %s", table.Name, err.Error())
		}
		_, err = db.Exec(s)
		if err != nil {
			return fmt.Errorf("%s: %s", table.Name, err.Error())
		}
	}
	return nil
}

func (c *Config) SQLCreate(table Table) (string, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE IF NOT EXISTS %s (\n", table.Name)
	var (
		uniques   map[string][]string
		primaries []string
		foreigns  []int
		lastQuite string
	)
	for i, col := range table.Cols {
		dbTyp, defaultVal, err := c.DBDialect.Type(col.Type, col.Precision, col.DefaultVal)
		if err != nil {
			return "", err
		}
		if col.Primary {
			primaries = append(primaries, col.Name)
		}
		if col.ForeignTable != "" {
			foreigns = append(foreigns, i)
		}
		if col.DBType != "" {
			dbTyp = col.DBType
		}
		var constraints string
		if col.Unique {
			if col.UniqueName == "" {
				constraints += " UNIQUE"
			} else {
				if uniques == nil {
					uniques = make(map[string][]string)
				}
				uniques[col.UniqueName] = append(uniques[col.UniqueName], col.Name)
			}
		}
		if col.AutoIncr {
			constraints += " AUTO INCREAMENT"
		}
		if !col.Notnull {
			constraints += " NOT NULL"
		}
		if col.Default {
			constraints += " DEFAULT " + defaultVal
		}
		lastQuite = ""
		if i != len(table.Cols)-1 || len(primaries) != 0 || len(uniques) != 0 || len(foreigns) != 0 {
			lastQuite = ","
		}
		fmt.Fprintf(&buf, "    %s %s %s%s\n", col.Name, dbTyp, constraints, lastQuite)
	}
	if len(primaries) > 0 {
		lastQuite = ""
		if len(uniques) != 0 || len(foreigns) != 0 {
			lastQuite = ","
		}
		fmt.Fprintf(&buf, "    PRIMARY KEY (%s)%s\n", strings.Join(primaries, ","), lastQuite)
	}
	for name, keys := range uniques {
		lastQuite = ""
		if len(foreigns) != 0 || len(uniques) != 1 {
			lastQuite = ","
		}
		fmt.Fprintf(&buf, "    CONSTRAINT %s UNIQUE (%s)%s\n", name, strings.Join(keys, ","), lastQuite)
		delete(uniques, name)
	}
	for i, index := range foreigns {
		col := table.Cols[index]
		lastQuite = ""
		if i != len(foreigns)-1 {
			lastQuite = ","
		}
		fmt.Fprintf(&buf, "    FOREIGN KEY(%s) REFERENCES %s(%s)%s\n", col.Name, col.ForeignTable, col.ForeignCol, lastQuite)
	}
	fmt.Fprintf(&buf, ");\n")
	return buf.String(), nil
}

func (c *Config) parseColumn(t *Table, f *reflect.StructField) (Column, error) {
	col := Column{
		Name:    c.NameMapper(f.Name),
		Type:    f.Type.Kind().String(),
		Default: c.Default,
		Notnull: !c.Notnull,
	}

	var conds []string
	tag := strings.TrimSpace(f.Tag.Get(c.FieldTag))
	if tag != "" {
		conds = strings.Split(tag, " ")
	}
	for _, sec := range conds {
		sec = strings.TrimSpace(sec)
		var (
			keyCond  = strings.SplitN(sec, ":", 2)
			condName = keyCond[0]
			condVal  string
		)
		if len(keyCond) > 1 {
			condVal = keyCond[1]
		}

		switch condName {
		case "table":
			t.Name = condVal
		case "col":
			col.Name = condVal
			if condVal == "" {
				return col, fmt.Errorf("invalid column name")
			}
			if condVal == "-" {
				col.Name = ""
				return col, nil
			}
			col.Name = condVal
		case "type":
			if condVal == "" {
				return col, fmt.Errorf("invalid column type: %s", col.Name)
			}
			col.Type = condVal
		case "precision":
			if condVal == "" {
				return col, fmt.Errorf("invalid column precision: %s", col.Name)
			}
			col.Precision = condVal
		case "dbtype":
			if condVal == "" {
				return col, fmt.Errorf("invalid column db type: %s", col.Name)
			}
			col.DBType = condVal
		case "pk":
			col.Primary = condVal == "" || condVal == "true"
		case "autoincr":
			col.AutoIncr = condVal == "" || condVal == "true"
		case "notnull":
			col.Notnull = condVal == "" || condVal == "true"
		case "default":
			col.Default = condVal != "-"
			if c.Default {
				col.DefaultVal = condVal
			}
		case "unique":
			col.Unique = true
			col.UniqueName = condVal
		case "fk":
			fkConds := strings.SplitN(condVal, ".", 2)
			if len(fkConds) != 2 || fkConds[0] == "" || fkConds[1] == "" {
				return col, fmt.Errorf("invalid foreign key: %s", condVal)
			}
			col.ForeignTable = fkConds[0]
			col.ForeignCol = fkConds[1]
		default:
			return col, fmt.Errorf("unsupported tag: %s", condName)
		}
	}
	return col, nil
}

func (c *Config) StructTable(v interface{}) (Table, error) {
	c.initDefault()

	refv := reflect.ValueOf(v)
	if refv.Kind() == reflect.Ptr {
		refv = refv.Elem()
	}
	if refv.Kind() != reflect.Struct {
		return Table{}, fmt.Errorf("invalid artument type, expect (pointer of) structure")
	}
	reft := refv.Type()

	t := Table{
		Name: c.TablenamePrefix + c.NameMapper(reft.Name()),
	}
	n := reft.NumField()
	for i := 0; i < n; i++ {
		f := reft.Field(i)
		col, err := c.parseColumn(&t, &f)
		if err != nil {
			return t, err
		}
		if col.Name != "" {
			t.Cols = append(t.Cols, col)
		}
	}
	return t, nil
}

func SnakeCase(s string) string {
	var (
		hasUpper  bool
		size      = utf8.RuneCountInString(s)
		prevUpper bool
	)
	for i, r := range s {
		if unicode.IsUpper(r) {
			hasUpper = true
			if i != 0 && !prevUpper {
				size++
			}
			prevUpper = true
		} else {
			prevUpper = false
		}
	}
	if !hasUpper {
		return s
	}
	var (
		buf = make([]rune, 0, size)
	)
	prevUpper = false
	for i, r := range s {
		isUpper := unicode.IsUpper(r)
		if isUpper && i != 0 && !prevUpper {
			buf = append(buf, '_')
		}
		if prevUpper = isUpper; isUpper {
			buf = append(buf, unicode.ToLower(r))
		} else {
			buf = append(buf, r)
		}
	}
	return string(buf)
}
