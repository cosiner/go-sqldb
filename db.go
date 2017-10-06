package sqldb

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"time"
)

type DBConfig struct {
	Type        string
	Host        string
	Port        int
	DBName      string
	User        string
	Password    string
	MaxIdle     int
	MaxOpen     int
	MaxLifetime int
	Options     map[string]string
}

func (d *DBConfig) JoinOptions(kvSep, optSep string) string {
	var buf bytes.Buffer
	for k, v := range d.Options {
		if buf.Len() > 0 {
			buf.WriteString(optSep)
		}
		buf.WriteString(k)
		buf.WriteString(kvSep)
		buf.WriteString(v)
	}
	return buf.String()
}
func (d *DBConfig) ApplyDefault(def DBConfig) {
	if d.Type == "" && def.Type != "" {
		d.Type = def.Type
	}
	if d.Host == "" && def.Host != "" {
		d.Host = def.Host
	}
	if d.Port == 0 && def.Port != 0 {
		d.Port = def.Port
	}
	if d.DBName == "" && def.DBName != "" {
		d.DBName = def.DBName
	}
	if d.User == "" && def.User != "" {
		d.User = def.User
	}
	if d.Password == "" && def.Password != "" {
		d.Password = def.Password
	}
	if d.MaxIdle == 0 && def.MaxIdle != 0 {
		d.MaxIdle = def.MaxIdle
	}
	if d.MaxOpen == 0 && def.MaxOpen != 0 {
		d.MaxOpen = def.MaxOpen
	}
	if d.MaxLifetime == 0 && def.MaxLifetime != 0 {
		d.MaxLifetime = def.MaxLifetime
	}
	for k, v := range def.Options {
		_, has := d.Options[k]
		if !has {
			if d.Options == nil {
				d.Options = make(map[string]string)
			}
			d.Options[k] = v
		}
	}
}

type NameMapper func(string) string

type DBDialect interface {
	Type(typ, precision, val string) (dbtyp, defaultVal string, err error)
	DSN(config DBConfig) string
}

func Open(dialect DBDialect, config DBConfig) (*sql.DB, error) {
	db, err := sql.Open(config.Type, dialect.DSN(config))
	if err != nil {
		return nil, err
	}
	if config.MaxIdle > 0 {
		db.SetMaxIdleConns(config.MaxIdle)
	}
	if config.MaxOpen > 0 {
		db.SetMaxOpenConns(config.MaxOpen)
	}
	if config.MaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(config.MaxLifetime) * time.Second)
	}
	return db, nil
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

type Tx interface {
	Commit() error
	Rollback() error
}

func TxDone(tx Tx, err *error) error {
	var e error
	if err != nil && *err != nil {
		e = tx.Rollback()
	} else {
		e = tx.Commit()
	}
	return e
}

type TxCloser interface {
	Tx
	io.Closer
}

func TxDoneClose(tx TxCloser, err *error) error {
	e := TxDone(tx, err)
	tx.Close()
	return e
}
