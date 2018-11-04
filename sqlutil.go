package sqldb

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

type ColumnNames []string

type ColumnNameJoinRule interface {
	Separator() string
	Append(buffer *bytes.Buffer, c string)
}

type columnNameJoinAsList struct{}

var _ ColumnNameJoinRule = columnNameJoinAsList{}

func (columnNameJoinAsList) Separator() string { return ", " }

func (columnNameJoinAsList) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString(c)
}

type columnNameJoinAsNamedList struct{}

var _ ColumnNameJoinRule = columnNameJoinAsNamedList{}

func (columnNameJoinAsNamedList) Separator() string { return ", " }

func (columnNameJoinAsNamedList) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString(":")
	buffer.WriteString(c)
}

type columnNameJoinAsUpdate struct{}

var _ ColumnNameJoinRule = columnNameJoinAsUpdate{}

func (columnNameJoinAsUpdate) Separator() string { return ", " }

func (columnNameJoinAsUpdate) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString(c)
	buffer.WriteString(" = ?")
}

type columnNameJoinAsNamedUpdate struct{}

var _ ColumnNameJoinRule = columnNameJoinAsNamedUpdate{}

func (columnNameJoinAsNamedUpdate) Separator() string { return ", " }

func (columnNameJoinAsNamedUpdate) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString(c)
	buffer.WriteString(" = :")
	buffer.WriteString(c)
}

type columnNameJoinAsCond struct{ Cond string }

var _ ColumnNameJoinRule = columnNameJoinAsCond{}

func (c columnNameJoinAsCond) Separator() string { return " " + c.Cond + " " }

func (columnNameJoinAsCond) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString(c)
	buffer.WriteString(" = ?")
}

type columnNameJoinAsNamedCond struct{ Cond string }

var _ ColumnNameJoinRule = columnNameJoinAsNamedCond{}

func (c columnNameJoinAsNamedCond) Separator() string { return " " + c.Cond + " " }

func (columnNameJoinAsNamedCond) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString(c)
	buffer.WriteString(" = :")
	buffer.WriteString(c)
}

func (c ColumnNames) Join(rule ColumnNameJoinRule) string {
	var b bytes.Buffer
	for _, c := range c {
		if b.Len() > 0 {
			b.WriteString(rule.Separator())
		}
		rule.Append(&b, c)
	}
	return b.String()
}

func (c ColumnNames) List() string {
	return c.Join(columnNameJoinAsList{})
}

func (c ColumnNames) NamedList() string {
	return c.Join(columnNameJoinAsNamedList{})
}

func (c ColumnNames) Update() string {
	return c.Join(columnNameJoinAsUpdate{})
}

func (c ColumnNames) NamedUpdate() string {
	return c.Join(columnNameJoinAsNamedUpdate{})
}

func (c ColumnNames) Cond(cond string) string {
	return c.Join(columnNameJoinAsCond{})
}

func (c ColumnNames) NamedCond(cond string) string {
	return c.Join(columnNameJoinAsNamedCond{})
}

func (c ColumnNames) Contains(col string) bool {
	for _, s := range c {
		if s == col {
			return true
		}
	}
	return false
}

func (c ColumnNames) Copy() ColumnNames {
	nc := make(ColumnNames, len(c))
	copy(nc, c)
	return nc
}

func (c ColumnNames) InplaceRemove(cols ...string) ColumnNames {
	var end int
	for i, col := range c {
		if !ColumnNames(cols).Contains(col) {
			if i != end {
				c[end] = col
			}
			end++
		}
	}
	c = c[:end]
	return c
}

type SQLCacheID uint32

type SQLUtil struct {
	dialect DBDialect
	parser  *TableParser

	mu        sync.RWMutex
	currID    SQLCacheID
	sqlCaches map[SQLCacheID]string
}

func NewSQLUtil(parser *TableParser, dialect DBDialect) *SQLUtil {
	return &SQLUtil{
		parser:    parser,
		dialect:   dialect,
		sqlCaches: make(map[SQLCacheID]string),
	}
}
func (s *SQLUtil) TableParser() *TableParser {
	return s.parser
}
func (s *SQLUtil) DBDialect() DBDialect {
	return s.dialect
}

func (s *SQLUtil) TableColumns(v interface{}, excepts ...string) ColumnNames {
	t, err := s.parser.StructTable(v)
	if err != nil {
		return nil
	}
	cols := make(ColumnNames, 0, len(t.Cols))
	for _, c := range t.Cols {
		if !ColumnNames(excepts).Contains(c.Name) {
			cols = append(cols, c.Name)
		}
	}
	return cols
}

func (s *SQLUtil) CachedSQL(idptr *SQLCacheID, fn func(*SQLUtil) string) string {
	var sql string
	id := *idptr
	if id == 0 {
		sql = fn(s)
		s.mu.Lock()
		id = *idptr
		if id == 0 {
			s.currID++
			id = s.currID

			s.sqlCaches[id] = sql
			*idptr = id
		}
		s.mu.Unlock()
	} else {
		s.mu.RLock()
		sql = s.sqlCaches[id]
		s.mu.RUnlock()
	}
	return sql
}

func (s *SQLUtil) CreateTables(db *sql.DB, models ...interface{}) error {
	for _, mod := range models {
		table, err := s.parser.StructTable(mod)
		if err != nil {
			return err
		}
		s, err := s.CreateTableSQL(table)
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

func (s *SQLUtil) escapeName(name string) string {
	return `"` + name + `"`
}

func (s *SQLUtil) CreateTableSQL(table Table) (string, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE IF NOT EXISTS %s (\n", s.escapeName(table.Name))
	var (
		uniques   map[string][]string
		primaries []string
		foreigns  []int
		lastQuite string
	)
	for i, col := range table.Cols {
		dbTyp, defaultVal, err := s.dialect.Type(col.Type, col.Precision, col.DefaultVal)
		if err != nil {
			return "", err
		}
		if col.Primary {
			primaries = append(primaries, s.escapeName(col.Name))
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
				uniques[col.UniqueName] = append(uniques[col.UniqueName], s.escapeName(col.Name))
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
		fmt.Fprintf(&buf, "    %s %s %s%s\n", s.escapeName(col.Name), dbTyp, constraints, lastQuite)
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
		fmt.Fprintf(&buf, "    FOREIGN KEY(%s) REFERENCES %s(%s)%s\n", s.escapeName(col.Name), col.ForeignTable, col.ForeignCol, lastQuite)
	}
	fmt.Fprintf(&buf, ");\n")
	return buf.String(), nil
}
