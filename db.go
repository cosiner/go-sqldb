package sqldb

import "fmt"

type NameMapper func(string) string

type DBDialect interface {
	Type(typ, precision, val string) (dbtyp, defaultVal string, err error)
}

func defaultVal(def, val string, quote bool) string {
	if val == "" {
		val = def
	}
	if quote {
		val = `'` + val + `'`
	}
	return val
}

type Postgres struct{}

func (Postgres) Type(typ, precision, val string) (dbtyp, defval string, err error) {
	switch typ {
	case "bool":
		return "BOOLEAN", defaultVal("false", val, false), nil
	case "int":
		return "INTEGER", defaultVal("0", val, false), nil
	case "int8":
		return "SMALLINT", defaultVal("0", val, false), nil
	case "int16":
		return "SMALLINT", defaultVal("0", val, false), nil
	case "int32":
		return "INTEGER", defaultVal("0", val, false), nil
	case "int64":
		return "BIGINT", defaultVal("0", val, false), nil
	case "uint":
		return "BIGINT", defaultVal("0", val, false), nil
	case "uint8":
		return "SMALLINT", defaultVal("0", val, false), nil
	case "uint16":
		return "INTEGER", defaultVal("0", val, false), nil
	case "uint32":
		return "BIGINT", defaultVal("0", val, false), nil
	case "uint64":
		return "BIGINT", defaultVal("0", val, false), nil
	case "float32", "float64":
		if precision != "" {
			typ = fmt.Sprintf("NUMERIC(%s)", precision)
		} else if typ == "float32" {
			typ = "REAL"
		} else {
			typ = "DOUBLE PRECISION"
		}
		return typ, defaultVal("0", val, false), nil
	case "string":
		if precision == "" {
			precision = "1024"
		}
		return fmt.Sprintf("VARCHAR(%s)", precision), defaultVal("", val, true), nil
	case "char":
		if precision == "" {
			precision = "256"
		}
		return fmt.Sprintf("CHAR(%s)", precision), defaultVal("", val, true), nil
	case "text":
		return "text", defaultVal("", val, true), nil
	default:
		return "", "", fmt.Errorf("postgres: unsupported type: %s", typ)
	}
}
