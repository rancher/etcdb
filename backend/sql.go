package backend

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/lib/pq"
	"github.com/rancherio/etcdb/models"
)

// SqlBackend SQL implementation
type SqlBackend struct {
	db *sql.DB
}

// New creates a SqlBackend for the DB
func New(driver, dataSource string) (*SqlBackend, error) {
	db, err := sql.Open(driver, dataSource)
	if err != nil {
		return nil, err
	}
	return &SqlBackend{db}, nil
}

func (b *SqlBackend) Close() error {
	return b.db.Close()
}

// CreateSchema creates the DB schema
func (b *SqlBackend) CreateSchema() error {
	return b.createSchema(false)
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

func (b *SqlBackend) createSchema(temp bool) error {
	t := ""
	if temp {
		t = "TEMPORARY "
	}
	return b.runQueries(
		"CREATE "+t+`TABLE "nodes" (
	    "key" varchar(2048),
	    "created" bigint NOT NULL,
	    "modified" bigint NOT NULL,
	    "value" text NOT NULL DEFAULT '',
	    "ttl" integer,
	    "expiration" timestamp,
	    "dir" boolean NOT NULL DEFAULT 'false',
			"path_depth" integer,
	    PRIMARY KEY ("key")
		)`,

		"CREATE "+t+`TABLE "index" (
		    "index" bigint,
		    PRIMARY KEY ("index")
		)`,

		`INSERT INTO "index" ("index") VALUES (0)`,
	)
}

// Get returns a node for the key
func (b *SqlBackend) Get(key string, recursive bool) (node *models.Node, err error) {
	tx, err := b.db.Begin()
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
	query := `SELECT key, created, modified, value, dir, ttl, expiration FROM nodes
						WHERE key = $1 OR (key LIKE $1 || '/%'`
	params := []interface{}{key}
	if !recursive {
		query = query + " AND path_depth = $2"
		params = append(params, strings.Count(key, "/")+1)
	}
	query = query + ")"
	rows, err := tx.Query(query, params...)
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
	var expiration pq.NullTime
	err := scanner.Scan(&node.Key, &node.CreatedIndex, &node.ModifiedIndex,
		&node.Value, &node.Dir, &node.TTL, &expiration)
	if err != nil {
		return nil, err
	}
	if expiration.Valid {
		node.Expiration = &expiration.Time
	}
	return &node, nil
}

func (b *SqlBackend) getOne(tx *sql.Tx, key string) (*models.Node, error) {
	node, err := scanNode(tx.QueryRow(`
		SELECT key, created, modified, value, dir, ttl, expiration FROM nodes
		WHERE key = $1
		`, key))
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
	tx, err := b.db.Begin()
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

	params := []interface{}{key, value, dir, index, pathDepth}
	var query string

	if prevNode == nil {
		columns := `key, value, dir, created, modified, path_depth`
		values := `$1, $2, $3, $4, $4, $5`
		if ttl != nil {
			columns = columns + `, ttl, expiration`
			values = values + `, $6, CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + $7::INTERVAL`
			params = append(params, *ttl, strconv.FormatInt(*ttl, 10))
		}
		query = fmt.Sprintf(`INSERT INTO nodes (%s) VALUES (%s)`, columns, values)
	} else {
		ttlClause := ""
		if ttl != nil {
			ttlClause = `, ttl = $6, expiration = CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + $7::INTERVAL`
			params = append(params, *ttl, strconv.FormatInt(*ttl, 10))
		}
		query = `UPDATE nodes SET value = $2, dir = $3, modified = $4, path_depth = $5` + ttlClause + ` WHERE key = $1`
	}
	_, err = tx.Exec(query, params...)
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
		_, err = tx.Exec(`
			INSERT INTO nodes (key, dir, created, modified, path_depth)
			VALUES ($1, true, $2, $2, $3)
			`, path, index, pathDepth)
		if err != nil {
			tx.Exec("ROLLBACK TO SAVEPOINT mkdirs")
		}
		if err, ok := err.(*pq.Error); ok {
			if err.Code == "23505" { // duplicate key
				var existingIsDir bool
				err := tx.QueryRow(`SELECT dir FROM nodes WHERE key = $1`, path).Scan(&existingIsDir)
				if err != nil {
					return err
				}
				if !existingIsDir {
					// FIXME should this be previous index before the update?
					return models.NotADirectory(path, index)
				}
				return nil
			}
		}
		if err != nil {
			return err
		}
		pathDepth--
	}
	return nil
}

func (b *SqlBackend) CreateInOrder(key, value string) (node *models.Node, err error) {
	tx, err := b.db.Begin()
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

	_, err = tx.Exec(`
		INSERT INTO nodes (key, value, created, modified)
		VALUES ($1, $2, $3, $3)
		`, key, value, index)
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
	tx, err := b.db.Begin()
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

	_, err = tx.Exec(`
		DELETE FROM nodes WHERE key = $1
		`, key)
	if err != nil {
		return nil, 0, err
	}

	return node, index, nil
}

// RmDir removes the key for directories
func (b *SqlBackend) RmDir(key string, recursive bool, condition Condition) (node *models.Node, index int64, err error) {
	tx, err := b.db.Begin()
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

	_, err = tx.Exec(`
		DELETE FROM nodes WHERE key = $1 OR key LIKE $1 || '/%'
		`, key)
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
	err = tx.QueryRow(`
		UPDATE index SET index = index + 1 RETURNING index
		`).Scan(&index)
	return
}
