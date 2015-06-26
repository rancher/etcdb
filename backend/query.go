package backend

import (
	"bytes"
	"database/sql"
)

type Query struct {
	buf     bytes.Buffer
	Params  []interface{}
	dialect dbDialect
}

func (q *Query) Text(text string) *Query {
	q.buf.WriteString(text)
	return q
}

func (q *Query) Param(p interface{}) *Query {
	q.Params = append(q.Params, p)
	q.buf.WriteString(q.dialect.nameParam(q.Params))
	return q
}

func (q *Query) Extend(parts ...interface{}) *Query {
	for i, p := range parts {
		if i%2 == 0 {
			q.Text(p.(string))
		} else {
			q.Param(p)
		}
	}
	return q
}

func (q *Query) Exec(db Querier) (sql.Result, error) {
	sql := q.buf.String()
	return db.Exec(sql, q.Params...)
}

func (q *Query) Query(db Querier) (*sql.Rows, error) {
	sql := q.buf.String()
	return db.Query(sql, q.Params...)
}

func (q *Query) QueryRow(db Querier) *sql.Row {
	sql := q.buf.String()
	return db.QueryRow(sql, q.Params...)
}

type Querier interface {
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
}
