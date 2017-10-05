package sqldb

import "database/sql"

func ErrOnNoRows(err, newErr error) error {
	if err == sql.ErrNoRows {
		return nil
	}
	return newErr
}

func ErrAllowNoRows(err error) error {
	return ErrOnNoRows(err, nil)
}

func ErrOnNoAffects(n int64, err, newErr error) error {
	if n == 0 && err == nil {
		return newErr
	}
	return nil
}

func ErrOnNoAffectsResult(res sql.Result, err, newErr error) error {
	var n int64
	if err == nil {
		n, err = res.RowsAffected()
	}
	if n == 0 && err == nil {
		return newErr
	}
	return nil
}
