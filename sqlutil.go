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

type columnNameJoinAsCond struct {
	Cond  string
	Check string
}

var _ ColumnNameJoinRule = columnNameJoinAsCond{}

func (cd columnNameJoinAsCond) Separator() string { return " " + cd.Cond + " " }

func (cd columnNameJoinAsCond) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString(c)
	buffer.WriteString(" " + cd.Check + " ?")
}

type columnNameJoinAsNamedCond struct {
	Cond  string
	Check string
}

var _ ColumnNameJoinRule = columnNameJoinAsNamedCond{}

func (cd columnNameJoinAsNamedCond) Separator() string { return " " + cd.Cond + " " }

func (cd columnNameJoinAsNamedCond) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString(c)
	buffer.WriteString(" " + cd.Check + " :")
	buffer.WriteString(c)
}

type columnNameJoinAsPlaceholders struct{}

func (columnNameJoinAsPlaceholders) Separator() string { return ", " }

func (columnNameJoinAsPlaceholders) Append(buffer *bytes.Buffer, c string) {
	buffer.WriteString("?")
}

var bufferPools = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

func (c ColumnNames) Join(rule ColumnNameJoinRule) string {
	b := bufferPools.Get().(*bytes.Buffer)
	for _, c := range c {
		if b.Len() > 0 {
			b.WriteString(rule.Separator())
		}
		rule.Append(b, c)
	}
	s := b.String()
	b.Reset()
	bufferPools.Put(b)
	return s
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

func (c ColumnNames) Cond(cond, check string) string {
	return c.Join(columnNameJoinAsCond{Cond: cond, Check: check})
}

func (c ColumnNames) NamedCond(cond, check string) string {
	return c.Join(columnNameJoinAsNamedCond{Cond: cond, Check: check})
}

func (c ColumnNames) Placeholders() string {
	return c.Join(columnNameJoinAsPlaceholders{})
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

type SQLUtil struct {
	dialect DBDialect
	parser  *TableParser
}

func NewSQLUtil(parser *TableParser, dialect DBDialect) *SQLUtil {
	return &SQLUtil{
		parser:  parser,
		dialect: dialect,
	}
}
func (s *SQLUtil) TableParser() *TableParser {
	return s.parser
}
func (s *SQLUtil) DBDialect() DBDialect {
	return s.dialect
}

func (s *SQLUtil) TableName(v interface{}) string {
	t, err := s.parser.StructTable(v)
	if err != nil {
		return ""
	}
	return t.Name
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

func (s *SQLUtil) EscapeName(name string) string {
	return `"` + name + `"`
}

func (s *SQLUtil) CreateTableSQL(table Table) (string, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE TABLE IF NOT EXISTS %s (\n", s.EscapeName(table.Name))
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
			primaries = append(primaries, s.EscapeName(col.Name))
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
				uniques[col.UniqueName] = append(uniques[col.UniqueName], s.EscapeName(col.Name))
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
		fmt.Fprintf(&buf, "    %s %s %s%s\n", s.EscapeName(col.Name), dbTyp, constraints, lastQuite)
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
		fmt.Fprintf(&buf, "    FOREIGN KEY(%s) REFERENCES %s(%s)%s\n", s.EscapeName(col.Name), col.ForeignTable, col.ForeignCol, lastQuite)
	}
	fmt.Fprintf(&buf, ");\n")
	return buf.String(), nil
}

type SQLHolder string

func (e *SQLHolder) SQL(sb *SQLBuilder, fn func(sb *SQLBuilder) string) string {
	str := string(*e)
	if str == "" {
		str = fn(sb)
		*e = SQLHolder(str)
	}
	return str
}

type SQLBuilder struct {
	SQLUtil *SQLUtil
}

func NewSQLBuilder(su *SQLUtil) *SQLBuilder {
	return &SQLBuilder{
		SQLUtil: su,
	}
}

func (b *SQLBuilder) whereClause(s string) string {
	if s == "" {
		return s
	}
	return " WHERE " + s
}

func (b *SQLBuilder) Query(model interface{}, columns, where []string) string {
	table := b.SQLUtil.TableName(model)
	if len(columns) == 0 {
		columns = b.SQLUtil.TableColumns(model, where...)
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s%s",
		ColumnNames(columns).List(),
		table,
		b.whereClause(ColumnNames(where).NamedCond("AND", "=")),
	)
}

func (b *SQLBuilder) IsExist(model interface{}, resultName string, where []string) string {
	table := b.SQLUtil.TableName(model)
	return fmt.Sprintf(
		"SELECT EXISTS(SELECT 1 FROM %s%s) AS %s",
		table,
		b.whereClause(ColumnNames(where).NamedCond("AND", "=")),
		resultName,
	)
}

type CheckIsExistGroup struct {
	Model      interface{}
	ResultName string
	Where      []string
}

func (b *SQLBuilder) MultiIsExist(groups ...CheckIsExistGroup) string {
	var buffer bytes.Buffer
	buffer.WriteString("SELECT ")
	var isFirst = true
	for _, g := range groups {
		table := b.SQLUtil.TableName(g.Model)
		if isFirst {
			isFirst = false
		} else {
			buffer.WriteString(", ")
		}
		_, _ = fmt.Fprintf(&buffer,
			"EXISTS(SELECT 1 FROM %s%s) AS %s",
			table,
			b.whereClause(ColumnNames(g.Where).NamedCond("AND", "=")),
			g.ResultName,
		)
	}
	return buffer.String()
}

func (b *SQLBuilder) Delete(model interface{}, wheres []string) string {
	table := b.SQLUtil.TableName(model)

	return fmt.Sprintf("DELETE FROM %s%s",
		table,
		b.whereClause(ColumnNames(wheres).NamedCond("AND", "=")),
	)
}

func (b *SQLBuilder) DeleteByConds(model interface{}, wheres string) string {
	table := b.SQLUtil.TableName(model)

	return fmt.Sprintf("DELETE FROM %s%s", table, b.whereClause(wheres))
}

func (b *SQLBuilder) Insert(model interface{}) string {
	table := b.SQLUtil.TableName(model)
	columns := ColumnNames(b.SQLUtil.TableColumns(model))
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", table, columns.List(), columns.NamedList())
}

func (b *SQLBuilder) InsertUnique(model interface{}, uniqueCols []string) string {
	table := b.SQLUtil.TableName(model)
	columns := ColumnNames(b.SQLUtil.TableColumns(model))
	return fmt.Sprintf("INSERT INTO %s(%s) SELECT %s WHERE NOT EXISTS(SELECT 1 FROM %s WHERE %s)",
		table,
		columns.List(),
		columns.NamedList(),
		table,
		ColumnNames(uniqueCols).NamedCond("AND", "="),
	)
}

func (b *SQLBuilder) Update(model interface{}, columns, wheres []string) string {
	table := b.SQLUtil.TableName(model)
	if len(columns) == 0 {
		columns = b.SQLUtil.TableColumns(model, wheres...)
	}
	return fmt.Sprintf("UPDATE %s SET %s%s",
		table,
		ColumnNames(columns).NamedUpdate(),
		b.whereClause(ColumnNames(wheres).NamedCond("AND", "=")),
	)
}
