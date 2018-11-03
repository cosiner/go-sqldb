package sqldb

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
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

	Field reflect.StructField
}

type Table struct {
	Name string
	Cols []Column
	Type reflect.Type
}

type TableParserOptions struct {
	FieldTag        string
	ColumnNameTag   string
	Default         bool
	Notnull         bool
	TablenamePrefix string
	NameMapper      NameMapper
}

func (o *TableParserOptions) merge(opts ...TableParserOptions) {
	for _, opt := range opts {
		if opt.FieldTag != "" {
			o.FieldTag = opt.FieldTag
		}
		if opt.ColumnNameTag != "" {
			o.ColumnNameTag = opt.ColumnNameTag
		}
		if opt.Default {
			o.Default = opt.Default
		}
		if opt.Notnull {
			o.Notnull = opt.Notnull
		}
		if opt.TablenamePrefix != "" {
			o.TablenamePrefix = opt.TablenamePrefix
		}
		if opt.NameMapper != nil {
			o.NameMapper = opt.NameMapper
		}
	}
}

type TableParser struct {
	opts TableParserOptions

	mu     sync.RWMutex
	tables map[reflect.Type]Table
}

func NewTableParser(options ...TableParserOptions) *TableParser {
	opts := TableParserOptions{
		FieldTag:   "sqldb",
		NameMapper: SnakeCase,
	}
	opts.merge(options...)

	return &TableParser{
		opts: opts,

		tables: make(map[reflect.Type]Table),
	}
}

func (p *TableParser) parseColumn(t *Table, f reflect.StructField) (Column, error) {
	col := Column{
		Name:    p.opts.NameMapper(f.Name),
		Type:    f.Type.String(),
		Default: p.opts.Default,
		Notnull: !p.opts.Notnull,
		Field:   f,
	}
	if p.isBlob(f.Type) {
		col.Type = "blob"
	}
	if p.opts.ColumnNameTag != "" {
		tag := f.Tag.Get(p.opts.ColumnNameTag)
		if tag != "" {
			col.Name = tag
		}
	}
	var conds []string
	tag := strings.TrimSpace(f.Tag.Get(p.opts.FieldTag))
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
			if p.opts.Default {
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

func (p *TableParser) isPrimary(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64,
		reflect.String:
		return true
	}
	return false
}

func (p *TableParser) isBlob(t reflect.Type) bool {
	return t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8
}

func (p *TableParser) shouldIgnore(f *reflect.StructField) bool {
	if f.Tag.Get(p.opts.FieldTag) == "-" {
		return true
	}
	if f.Type.Kind() == reflect.Struct {
		return !f.Anonymous
	}
	if !p.isPrimary(f.Type) && !p.isBlob(f.Type) {
		return true
	}
	return unicode.IsLower([]rune(f.Name)[0])
}

func (p *TableParser) concatIndexes(parent, child []int) []int {
	if len(parent) == 0 {
		return child
	}
	indexes := make([]int, 0, len(parent)+len(child))
	indexes = append(indexes, parent...)
	indexes = append(indexes, child...)
	return indexes
}

func (p *TableParser) structFields(fields []reflect.StructField, parentFieldIndexes []int, t reflect.Type) []reflect.StructField {
	n := t.NumField()

	var anonymousStructs []reflect.StructField
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if p.shouldIgnore(&f) {
			continue
		}
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			anonymousStructs = append(anonymousStructs, f)
		} else {
			var override bool
			for i := range fields {
				if fields[i].Name == f.Name {
					override = true
					break
				}
			}
			if !override {
				f.Index = p.concatIndexes(parentFieldIndexes, f.Index)
				fields = append(fields, f)
			}
		}
	}
	for _, f := range anonymousStructs {
		fields = p.structFields(fields, p.concatIndexes(parentFieldIndexes, f.Index), f.Type)
	}
	return fields
}

func (p *TableParser) indirectReflect(v interface{}) reflect.Value {
	refv := reflect.ValueOf(v)
	for refv.Kind() == reflect.Ptr {
		refv = refv.Elem()
	}
	return refv
}

func (p *TableParser) parseTable(refv reflect.Value) (Table, error) {
	reft := refv.Type()
	if refv.Kind() != reflect.Struct {
		return Table{}, fmt.Errorf("invalid artument type, expect (pointer of) structure")
	}

	t := Table{
		Name: p.opts.TablenamePrefix + p.opts.NameMapper(reft.Name()),
		Type: reft,
	}
	fields := p.structFields(nil, nil, reft)
	for _, f := range fields {
		col, err := p.parseColumn(&t, f)
		if err != nil {
			return t, err
		}
		if col.Name != "" {
			t.Cols = append(t.Cols, col)
		}
	}
	return t, nil
}

func (p *TableParser) StructTable(v interface{}) (Table, error) {
	refv := p.indirectReflect(v)
	reft := refv.Type()

	p.mu.RLock()
	t, has := p.tables[reft]
	p.mu.RUnlock()
	if has {
		return t, nil
	}

	t, err := p.parseTable(refv)
	if err != nil {
		return t, err
	}
	p.mu.Lock()
	if p.tables == nil {
		p.tables = make(map[reflect.Type]Table)
	}
	p.tables[reft] = t
	p.mu.Unlock()
	return t, nil
}

func SnakeCase(s string) string {
	runes := []rune(s)

	var out []rune
	l := len(runes)
	for i := 0; i < l; i++ {
		if i > 0 &&
			unicode.IsUpper(runes[i]) { // curr upper
			if (i+1 < len(runes) && !unicode.IsUpper(runes[i+1])) || // next lower
				!unicode.IsUpper(runes[i-1]) { // prev lower
				out = append(out, '_')
			}
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return string(out)
}
