package backend

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/rancherio/etcdb/models"
)

// SqlBackend SQL implementation
type SqlBackend struct {
	db      *sql.DB
	dialect dbDialect
}

// New creates a SqlBackend for the DB
func New(driver, dataSource string) (*SqlBackend, error) {
	var dialect dbDialect
	switch driver {
	case "mysql":
		dialect = mysqlDialect{}
	case "postgres":
		dialect = postgresDialect{}
	default:
		return nil, fmt.Errorf("Unrecognized database driver %s, should be 'mysql' or 'postgres'", driver)
	}

	db, err := sql.Open(driver, dataSource)
	if err != nil {
		return nil, err
	}
	backend := &SqlBackend{db, dialect}
	err = backend.dialect.init(db)
	if err != nil {
		backend.Close()
		return nil, err
	}

	return backend, nil
}

func (b *SqlBackend) Close() error {
	return b.db.Close()
}

func (b *SqlBackend) runQueries(queries ...string) error {
	for _, q := range queries {
		_, err := b.db.Exec(q)
		if err != nil {
			log.Printf("err: %s -- %T %s", err, err, q)
			return err
		}
	}

	return nil
}

func (b *SqlBackend) dropSchema() error {
	return b.runQueries(
		`DROP TABLE IF EXISTS "nodes"`,
		`DROP TABLE IF EXISTS "index"`,
	)
}

// CreateSchema creates the DB schema
func (b *SqlBackend) CreateSchema() error {
	queries := b.dialect.tableDefinitions()
	queries = append(queries, `INSERT INTO "index" ("index") VALUES (0)`)
	return b.runQueries(queries...)
}

func (b *SqlBackend) Query() *Query {
	return &Query{dialect: b.dialect}
}

func (b *SqlBackend) Begin() (tx *sql.Tx, err error) {
	tx, err = b.db.Begin()
	if err != nil {
		return
	}
	_, err = tx.Exec(`DELETE FROM "nodes" WHERE "expiration" < ` + b.dialect.now())
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	return
}

// Get returns a node for the key
func (b *SqlBackend) Get(key string, recursive bool) (node *models.Node, err error) {
	tx, err := b.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	return b.get(tx, key, recursive)
}

func (b *SqlBackend) get(tx *sql.Tx, key string, recursive bool) (*models.Node, error) {
	// TODO compute "depth" field based on the # of directories
	// in the path and can query for immediate descendents based on that
	query := b.queryNode(key).Extend(` OR ("key" LIKE `)
	b.dialect.concat(query, key, "/%")
	if !recursive {
		query.Extend(" AND path_depth = ", strings.Count(key, "/")+1)
	}
	query.Text(")")
	rows, err := query.Query(tx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// FIXME
	currIndex := int64(0)

	nodes := make(map[string]*models.Node)

	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		nodes[node.Key] = node
	}

	if _, ok := nodes[key]; !ok {
		return nil, models.NotFound(key, currIndex)
	}

	for _, node := range nodes {
		if node.Key == key {
			// don't need to compute parent of the requested key
			continue
		}
		parent := nodes[splitKey(node.Key)]
		parent.Nodes = append(parent.Nodes, node)
	}

	return nodes[key], nil
}

type scannable interface {
	Scan(...interface{}) error
}

func scanNode(scanner scannable) (*models.Node, error) {
	var node models.Node
	// mysql.NullTime is more portable and works with the Postgres driver
	var expiration mysql.NullTime
	err := scanner.Scan(&node.Key, &node.CreatedIndex, &node.ModifiedIndex,
		&node.Value, &node.Dir, &expiration, &node.TTL)
	if err != nil {
		return nil, err
	}
	if expiration.Valid {
		node.Expiration = &expiration.Time
	}
	return &node, nil
}

func (b *SqlBackend) queryNode(key string) *Query {
	return b.Query().Text(`
		SELECT "key", "created", "modified", "value", "dir", "expiration",
		`).Text(b.dialect.ttl()).Extend(`
		FROM "nodes"
		WHERE "key" = `, key)
}

func (b *SqlBackend) getOne(tx *sql.Tx, key string) (*models.Node, error) {
	node, err := scanNode(b.queryNode(key).QueryRow(tx))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return node, err
}

// Set sets the value for a key
func (b *SqlBackend) Set(key, value string, condition Condition) (*models.Node, *models.Node, error) {
	return b.set(key, value, false, nil, condition)
}

func (b *SqlBackend) SetTTL(key, value string, ttl int64, condition Condition) (*models.Node, *models.Node, error) {
	return b.set(key, value, false, &ttl, condition)
}

func (b *SqlBackend) MkDir(key string, condition Condition) (*models.Node, *models.Node, error) {
	return b.set(key, "", true, nil, condition)
}

func (b *SqlBackend) set(key, value string, dir bool, ttl *int64, condition Condition) (node *models.Node, prevNode *models.Node, err error) {
	tx, err := b.Begin()
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	// TODO select for update?
	prevNode, err = b.getOne(tx, key)
	if err != nil {
		return nil, nil, err
	}

	currIndex := int64(0)

	if err := condition.Check(key, currIndex, prevNode); err != nil {
		return nil, nil, err
	}

	if prevNode != nil && prevNode.Dir {
		// XXX is this index the new, or previous index?
		return nil, nil, models.NotAFile(key, currIndex)
	}

	index, err := b.incrementIndex(tx)
	if err != nil {
		return nil, nil, err
	}

	err = b.mkdirs(tx, splitKey(key), index)
	if err != nil {
		return nil, nil, err
	}

	pathDepth := strings.Count(key, "/")

	query := b.Query()

	if prevNode == nil {
		query.Text(`INSERT INTO nodes ("key", "value", "dir", "created", "modified", "path_depth"`)
		if ttl != nil {
			query.Text(`, expiration`)
		}
		query.Extend(`) VALUES (`,
			key, `, `, value, `, `, dir, `, `, index, `, `, index, `, `, pathDepth,
		)
		if ttl != nil {
			query.Text(`, `)
			b.dialect.expiration(query, *ttl)
		}
		query.Text(")")
	} else {
		query.Extend(`UPDATE nodes SET "value" = `, value, `, dir = `, dir,
			`, modified = `, index, `, path_depth = `, pathDepth)
		if ttl == nil {
			query.Text(
				`, expiration = null`,
			)
		} else {
			query.Text(`, expiration = `)
			b.dialect.expiration(query, *ttl)
		}
		query.Extend(` WHERE "key" = `, key)
	}
	_, err = query.Exec(tx)
	if err != nil {
		return nil, nil, err
	}

	node, err = b.getOne(tx, key)
	if err != nil {
		return nil, nil, err
	}

	return node, prevNode, nil
}

func (b *SqlBackend) mkdirs(tx *sql.Tx, path string, index int64) error {
	pathDepth := strings.Count(path, "/")
	for ; path != "/" && path != ""; path = splitKey(path) {
		_, err := tx.Exec("SAVEPOINT mkdirs")
		if err != nil {
			return err
		}
		_, err = b.Query().Extend(`
			INSERT INTO nodes ("key", "dir", "created", "modified", "path_depth")
			VALUES (`, path, `, true, `, index, `, `, index, `, `, pathDepth, `)
			`).Exec(tx)
		if err != nil {
			tx.Exec("ROLLBACK TO SAVEPOINT mkdirs")
		}
		if b.dialect.isDuplicateKeyError(err) {
			var existingIsDir bool
			err := b.Query().Extend(`SELECT dir FROM nodes WHERE "key" = `, path).QueryRow(tx).Scan(&existingIsDir)
			if err != nil {
				return err
			}
			if !existingIsDir {
				// FIXME should this be previous index before the update?
				return models.NotADirectory(path, index)
			}
			return nil
		}
		if err != nil {
			return err
		}
		pathDepth--
	}
	return nil
}

func (b *SqlBackend) CreateInOrder(key, value string) (node *models.Node, err error) {
	tx, err := b.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	index, err := b.incrementIndex(tx)
	if err != nil {
		return nil, err
	}

	key = fmt.Sprintf("%s/%d", key, index)

	_, err = b.Query().Extend(`
		INSERT INTO nodes ("key", "value", "created", "modified")
		VALUES (`, key, `, `, value, `, `, index, `, `, `)`).Exec(tx)
	if err != nil {
		return nil, err
	}

	node, err = b.getOne(tx, key)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Delete removes the key
func (b *SqlBackend) Delete(key string, condition Condition) (node *models.Node, index int64, err error) {
	tx, err := b.Begin()
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	node, err = b.getOne(tx, key)
	if err != nil {
		return nil, 0, err
	}

	// XXX
	currIndex := int64(0)

	if node == nil {
		return nil, 0, models.NotFound(key, currIndex)
	}
	if node.Dir {
		return nil, 0, models.NotAFile(key, currIndex)
	}

	if err := condition.Check(key, currIndex, node); err != nil {
		return nil, 0, err
	}

	index, err = b.incrementIndex(tx)
	if err != nil {
		return nil, 0, err
	}

	_, err = b.Query().Extend(`
		DELETE FROM nodes WHERE "key" =
		`, key).Exec(tx)
	if err != nil {
		return nil, 0, err
	}

	return node, index, nil
}

// RmDir removes the key for directories
func (b *SqlBackend) RmDir(key string, recursive bool, condition Condition) (node *models.Node, index int64, err error) {
	tx, err := b.Begin()
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	node, err = b.get(tx, key, false)
	if err != nil {
		return nil, 0, err
	}

	// XXX
	currIndex := int64(0)

	if node == nil {
		return nil, 0, models.NotFound(key, currIndex)
	}
	// TODO can we get the count of deleted nodes instead, and roll back if deleting
	// more than one?
	if !recursive && len(node.Nodes) > 0 {
		return nil, 0, models.DirectoryNotEmpty(key, currIndex)
	}

	if err := condition.Check(key, currIndex, node); err != nil {
		return nil, 0, err
	}

	index, err = b.incrementIndex(tx)
	if err != nil {
		return nil, 0, err
	}

	_, err = b.Query().Extend(`
		DELETE FROM nodes WHERE "key" = `, key, ` OR "key" LIKE `, key, ` || '/%'
		`).Exec(tx)
	if err != nil {
		return nil, 0, err
	}

	return node, index, nil
}

func splitKey(key string) string {
	i := len(key) - 1
	for i >= 0 && key[i] != '/' {
		i--
	}
	return key[:i]
}

func (b *SqlBackend) incrementIndex(tx *sql.Tx) (index int64, err error) {
	return b.dialect.incrementIndex(tx)
}
