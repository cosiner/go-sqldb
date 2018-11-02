package sqldb

import (
	"sync"
)

type SQLID uint32

type SQLCache struct {
	mu   sync.RWMutex
	curr SQLID
	sqls map[SQLID]string
}

func NewSQLCache() *SQLCache {
	return &SQLCache{
		sqls: make(map[SQLID]string),
	}
}

func (c *SQLCache) Get(idptr *SQLID, fn func() string) string {
	var sql string
	id := *idptr
	if id == 0 {
		sql = fn()
		c.mu.Lock()
		id = *idptr
		if id == 0 {
			c.curr++
			id = c.curr

			c.sqls[id] = sql
			*idptr = id
		}
		c.mu.Unlock()
	} else {
		c.mu.RLock()
		sql = c.sqls[id]
		c.mu.RUnlock()
	}
	return sql
}
