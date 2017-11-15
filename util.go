package sqldb

import "database/sql"

func ResultRowsAffected(result sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func ResultLastInsertId(result sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}
