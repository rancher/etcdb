package backend

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
)

type dbDialect interface {
	init(*sql.DB) error
	tableDefinitions() []string
	nameParam([]interface{}) string
	incrementIndex(Querier) (int64, error)
	expiration(*Query, int64)
	isDuplicateKeyError(error) bool
	now() string
	ttl() string
}

type mysqlDialect struct{}

func (d mysqlDialect) init(db *sql.DB) error {
	_, err := db.Exec("SET sql_mode='ANSI_QUOTES'")
	return err
}

func (d mysqlDialect) tableDefinitions() []string {
	return []string{
		`CREATE TABLE "nodes" (
			"key" varchar(255),
			"created" bigint NOT NULL,
			"modified" bigint NOT NULL,
			"value" text NOT NULL DEFAULT '',
			"expiration" timestamp NULL,
			"dir" boolean NOT NULL DEFAULT 0,
			"path_depth" integer,
			PRIMARY KEY ("key")
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`,

		`CREATE INDEX "nodes_expiration" ON "nodes" ("expiration")`,

		`CREATE TABLE "index" (
			"index" bigint,
			PRIMARY KEY ("index")
		) ENGINE=InnoDB`,
	}
}

func (d mysqlDialect) nameParam(params []interface{}) string {
	return "?"
}

func (d mysqlDialect) incrementIndex(db Querier) (index int64, err error) {
	_, err = db.Exec(`
		UPDATE "index" SET "index" = "index" + 1
		`)
	if err != nil {
		return
	}
	err = db.QueryRow(`SELECT "index" FROM "index"`).Scan(&index)
	return
}

func (d mysqlDialect) expiration(q *Query, ttl int64) {
	q.Extend(`DATE_ADD(UTC_TIMESTAMP, INTERVAL `, ttl, ` SECOND)`)
}

func (d mysqlDialect) now() string {
	return "UTC_TIMESTAMP"
}

func (d mysqlDialect) ttl() string {
	return "TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP, expiration)"
}

func (d mysqlDialect) isDuplicateKeyError(err error) bool {
	if err, ok := err.(*mysql.MySQLError); ok {
		return err.Number == 1062
	}
	return false
}

// PostgreSQL

type postgresDialect struct{}

func (d postgresDialect) init(db *sql.DB) error {
	return nil
}

func (d postgresDialect) tableDefinitions() []string {
	return []string{
		`CREATE TABLE "nodes" (
			"key" varchar(2048),
			"created" bigint NOT NULL,
			"modified" bigint NOT NULL,
			"value" text NOT NULL DEFAULT '',
			"expiration" timestamp,
			"dir" boolean NOT NULL DEFAULT 'false',
			"path_depth" integer,
			PRIMARY KEY ("key")
		)`,

		`CREATE INDEX ON "nodes" ("expiration")`,

		`CREATE TABLE "index" (
			"index" bigint,
			PRIMARY KEY ("index")
		)`,
	}
}

func (d postgresDialect) nameParam(params []interface{}) string {
	return fmt.Sprintf("$%d", len(params))
}

func (d postgresDialect) incrementIndex(db Querier) (index int64, err error) {
	err = db.QueryRow(`
		UPDATE index SET index = index + 1 RETURNING index
		`).Scan(&index)
	return
}

func (d postgresDialect) expiration(q *Query, ttl int64) {
	q.Extend(`CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + `,
		strconv.FormatInt(ttl, 10),
		`::INTERVAL`,
	)
}

func (d postgresDialect) now() string {
	return `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'`
}

func (d postgresDialect) ttl() string {
	return "CAST(EXTRACT(EPOCH FROM expiration) - EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS integer)"
}

func (d postgresDialect) isDuplicateKeyError(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		return err.Code == "23505"
	}
	return false
}
