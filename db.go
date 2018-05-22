package sqldb

import (
	"bytes"
	"database/sql"
	"io"
	"time"
)

type DBConfig struct {
	Type        string            `json:"type" yaml:"type" toml:"type"`
	Host        string            `json:"host" yaml:"host" toml:"host"`
	Port        int               `json:"port" yaml:"port" toml:"port"`
	DBName      string            `json:"dbname" yaml:"dbname" toml:"dbname"`
	User        string            `json:"user" yaml:"user" toml:"user"`
	Password    string            `json:"password" yaml:"password" toml:"password"`
	MaxIdle     int               `json:"maxIdle" yaml:"maxIdle" toml:"maxIdle"`
	MaxOpen     int               `json:"maxOpen" yaml:"maxOpen" toml:"maxOpen"`
	MaxLifetime int               `json:"maxLifetime" yaml:"maxLifetime" toml:"maxLifetime"`
	Options     map[string]string `json:"options" yaml:"options" toml:"options"`
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

type Tx interface {
	Commit() error
	Rollback() error
}

func TxDone(tx Tx, err *error) error {
	var e error
	if err != nil {
		e = *err
	}
	if e != nil {
		tx.Rollback()
	} else {
		e = tx.Commit()
		if err != nil {
			*err = e
		}
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
