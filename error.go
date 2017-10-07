package sqldb

import "database/sql"

func ErrOnNoRows(err, newErr error) error {
	if err == sql.ErrNoRows {
		return newErr
	}
	return err
}

func ErrAllowNoRows(err error) error {
	return ErrOnNoRows(err, nil)
}

func ErrOnNoAffects(n int64, err, newErr error) error {
	if n == 0 && err == nil {
		return newErr
	}
	return err
}

func ErrOnNoAffectsResult(res sql.Result, err, newErr error) error {
	var n int64
	if err == nil {
		n, err = res.RowsAffected()
	}
	return ErrOnNoAffects(n, err, newErr)
}
