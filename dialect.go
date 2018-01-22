package sqldb

import (
	"fmt"
	"strings"
)

type Postgres struct{}

func (Postgres) defaultVal(def, val string, quote bool) string {
	if val == "" {
		val = def
	}
	if quote {
		if strings.Contains(val, "'") {
			val = strings.Replace(val, "\\", "\\\\'", -1)
			val = strings.Replace(val, "'", "\\'", -1)
			val = `E'` + val + `'`
		} else {
			val = `'` + val + `'`
		}
	}
	return val
}

func (p Postgres) Type(typ, precision, val string) (dbtyp, defval string, err error) {
	switch typ {
	case "bool":
		return "BOOLEAN", p.defaultVal("false", val, false), nil
	case "int":
		return "INTEGER", p.defaultVal("0", val, false), nil
	case "int8":
		return "SMALLINT", p.defaultVal("0", val, false), nil
	case "int16":
		return "SMALLINT", p.defaultVal("0", val, false), nil
	case "int32":
		return "INTEGER", p.defaultVal("0", val, false), nil
	case "int64":
		return "BIGINT", p.defaultVal("0", val, false), nil
	case "uint":
		return "BIGINT", p.defaultVal("0", val, false), nil
	case "uint8":
		return "SMALLINT", p.defaultVal("0", val, false), nil
	case "uint16":
		return "INTEGER", p.defaultVal("0", val, false), nil
	case "uint32":
		return "BIGINT", p.defaultVal("0", val, false), nil
	case "uint64":
		return "BIGINT", p.defaultVal("0", val, false), nil
	case "float32", "float64", "float":
		if precision != "" {
			typ = fmt.Sprintf("NUMERIC(%s)", precision)
		} else if typ == "float32" {
			typ = "REAL"
		} else {
			typ = "DOUBLE PRECISION"
		}
		return typ, p.defaultVal("0", val, false), nil
	case "string":
		if precision == "" {
			precision = "64"
		}
		return fmt.Sprintf("VARCHAR(%s)", precision), p.defaultVal("", val, true), nil
	case "char":
		if precision == "" {
			precision = "64"
		}
		return fmt.Sprintf("CHAR(%s)", precision), p.defaultVal("", val, true), nil
	case "text":
		return "text", p.defaultVal("", val, true), nil
	default:
		return "", "", fmt.Errorf("postgres: unsupported type: %s", typ)
	}
}

func (Postgres) DSN(config DBConfig) string {
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Port == 0 {
		config.Port = 5432
	}
	userPass := config.User
	if userPass != "" {
		if config.Password != "" {
			userPass += ":" + config.Password
		}
		userPass += "@"
	}
	return fmt.Sprintf(
		"postgres://%s%s:%d/%s?%s",
		userPass,
		config.Host,
		config.Port,
		config.DBName,
		config.JoinOptions("=", "&"),
	)
}

type SQLite3 struct{}

func (SQLite3) DSN(config DBConfig) string {
	if config.Host == "" || config.DBName == "" {
		return ":memory:"
	}
	return fmt.Sprintf("file:%s?%s", config.DBName, config.JoinOptions("=", "&"))
}

func (s SQLite3) defaultVal(def, val string, quote bool) string {
	if val == "" {
		val = def
	}
	if quote {
		if strings.Contains(val, "'") {
			val = strings.Replace(val, "'", "''", -1)
		}
		val = `'` + val + `'`
	}
	return val
}

func (s SQLite3) Type(typ, precision, val string) (dbtyp, defval string, err error) {
	switch typ {
	case "bool",
		"int",
		"int8",
		"int16",
		"int32",
		"int64",
		"uint",
		"uint8",
		"uint16",
		"uint32",
		"uint64":
		return "INTEGER", s.defaultVal("0", val, false), nil
	case "float32",
		"float64",
		"float":
		return "FLOAT", s.defaultVal("0", val, false), nil
	case "string",
		"text",
		"char":
		return "TEXT", s.defaultVal("", val, true), nil
	default:
		return "", "", fmt.Errorf("sqlite3: unsupported type: %s", typ)
	}
}
