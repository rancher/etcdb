package backend

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/rancher/etcdb/models"
)

// MaxChanges is the maximum rows to keep in the changes table, and the
// corresponding previous versions of modified or deleted nodes.
const MaxChanges = 1000

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

	db, err := dialect.Open(driver, dataSource)
	if err != nil {
		return nil, err
	}
	backend := &SqlBackend{db, dialect}
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
		`DROP TABLE IF EXISTS "changes"`,
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
	err = b.purgeExpired()
	if err != nil {
		log.Println("error expiring:", err)
		return
	}

	return b.db.Begin()
}

func (b *SqlBackend) purgeExpired() (err error) {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
		if err == sql.ErrNoRows {
			err = nil
		}
	}()

	index, err := b.incrementIndex(tx)
	if err != nil {
		return
	}

	rows, err := tx.Query(`SELECT "key", "modified" FROM "nodes"
		WHERE "deleted" = 0 AND "expiration" < ` + b.dialect.now() + `
		ORDER BY "expiration"`)
	if err != nil {
		return
	}
	defer rows.Close()

	var nodes []*models.Node

	for rows.Next() {
		var node models.Node
		err = rows.Scan(&node.Key, &node.ModifiedIndex)
		if err != nil {
			return err
		}
		nodes = append(nodes, &node)
	}

	if len(nodes) == 0 {
		return sql.ErrNoRows
	}

	expirationIndex := index

	for _, node := range nodes {
		err = b.recordChange(tx, expirationIndex, "expire", node.Key, node)
		if err != nil {
			return err
		}

		query := b.Query().Extend(`UPDATE nodes SET deleted = `, expirationIndex,
			` WHERE deleted = 0 AND ("key" = `, node.Key, ` OR "key" LIKE `, node.Key+"/%", `)`)
		_, err = query.Exec(tx)
		if err != nil {
			return err
		}

		expirationIndex++
	}

	// undo last increment to match the final index value used
	expirationIndex--

	_, err = b.Query().Extend(`UPDATE "index" SET "index" = `, expirationIndex).Exec(tx)

	return err
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

	query := b.queryNode()
	if key == "/" {
		if !recursive {
			query.Text(` AND path_depth = 1`)
		}
	} else {
		query.Extend(` AND ("key" = `, key, ` OR ("key" LIKE `, key+"/%")
		if !recursive {
			query.Extend(" AND path_depth = ", pathDepth(key)+1)
		}
		query.Text("))")
	}
	rows, err := query.Query(tx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nodes := make(map[string]*models.Node)

	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		nodes[node.Key] = node
	}

	if key == "/" {
		nodes["/"] = &models.Node{Dir: true}
	}

	if _, ok := nodes[key]; !ok {
		currIndex, err := b.currIndex(tx)
		if err != nil {
			return nil, err
		}
		return nil, models.NotFound(key, currIndex)
	}

	for _, node := range nodes {
		if node.Key == key || node.Key == "" {
			// don't need to compute parent of the requested key, or root key
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

func (b *SqlBackend) queryNodeWithDeleted() *Query {
	return b.Query().Text(`
		SELECT "key", "created", "modified", "value", "dir", "expiration",
		`).Text(b.dialect.ttl()).Text(`
		FROM "nodes"`)
}

func (b *SqlBackend) queryNode() *Query {
	return b.queryNodeWithDeleted().Text(` WHERE "deleted" = 0`)
}

func (b *SqlBackend) getOne(tx *sql.Tx, key string) (*models.Node, error) {
	node, err := scanNode(b.queryNode().Extend(` AND "key" = `, key).QueryRow(tx))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return node, err
}

// Set sets the value for a key
func (b *SqlBackend) Set(key, value string, condition SetCondition) (*models.Node, *models.Node, error) {
	return b.set(key, value, false, nil, condition)
}

func (b *SqlBackend) SetTTL(key, value string, ttl int64, condition SetCondition) (*models.Node, *models.Node, error) {
	return b.set(key, value, false, &ttl, condition)
}

func (b *SqlBackend) MkDir(key string, ttl *int64, condition SetCondition) (*models.Node, *models.Node, error) {
	return b.set(key, "", true, ttl, condition)
}

func (b *SqlBackend) readOnlyError() error {
	index, err := b.currIndex(b.db)
	if err != nil {
		return err
	}
	return models.RootReadOnly(index)
}

func (b *SqlBackend) set(key, value string, dir bool, ttl *int64, condition SetCondition) (node *models.Node, prevNode *models.Node, err error) {
	if key == "/" {
		return nil, nil, b.readOnlyError()
	}

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

	index, err := b.incrementIndex(tx)
	if err != nil {
		return nil, nil, err
	}

	prevNode, err = b.getOne(tx, key)
	if err != nil {
		return nil, nil, err
	}

	prevIndex := index - 1

	if err := condition.Check(key, prevIndex, prevNode); err != nil {
		return nil, nil, err
	}

	if prevNode != nil && prevNode.Dir {
		return nil, nil, models.NotAFile(key, prevIndex)
	}

	err = b.mkdirs(tx, splitKey(key), index)
	if err != nil {
		return nil, nil, err
	}

	if prevNode != nil {
		_, err = b.Query().Extend(
			`UPDATE nodes SET "deleted" = `, index,
			` WHERE "deleted" = 0 AND "key" = `, key,
		).Exec(tx)
		if err != nil {
			return nil, nil, err
		}
	}

	_, err = b.insertQuery(key, value, dir, index, ttl).Exec(tx)
	if err != nil {
		return nil, nil, err
	}

	node, err = b.getOne(tx, key)
	if err != nil {
		return nil, nil, err
	}

	err = b.recordChange(tx, index, condition.SetActionName(), key, prevNode)
	if err != nil {
		return nil, nil, err
	}

	return node, prevNode, nil
}

func (b *SqlBackend) recordChange(db Querier, index int64, action, key string, prevNode *models.Node) (err error) {
	query := b.Query().Extend(`INSERT INTO changes
		("index", "key", "action", "prev_node_modified") VALUES (`,
		index, `, `, key, `,`, action)
	if prevNode == nil {
		query.Text(`, null)`)
	} else {
		query.Extend(`, `, prevNode.ModifiedIndex, `)`)
	}
	_, err = query.Exec(db)
	if err != nil {
		return
	}

	_, err = b.Query().Extend(`DELETE FROM changes WHERE "index" < `, index-MaxChanges).Exec(db)
	if err != nil {
		return
	}

	_, err = b.Query().Extend(`DELETE FROM "nodes" WHERE "deleted" > 0 AND "deleted" < `, index-MaxChanges).Exec(db)
	return
}

func (b *SqlBackend) insertQuery(key, value string, dir bool, index int64, ttl *int64) *Query {
	pathDepth := pathDepth(key)
	query := b.Query()
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
	return query
}

func (b *SqlBackend) mkdirs(tx *sql.Tx, path string, index int64) error {
	pathDepth := pathDepth(path)
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
			err := b.Query().Extend(`SELECT dir FROM nodes WHERE "deleted" = 0 AND "key" = `, path).QueryRow(tx).Scan(&existingIsDir)
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

func (b *SqlBackend) CreateInOrder(key, value string, ttl *int64) (node *models.Node, err error) {
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

	_, err = b.insertQuery(key, value, false, index, ttl).Exec(tx)
	if err != nil {
		return nil, err
	}

	node, err = b.getOne(tx, key)
	if err != nil {
		return nil, err
	}

	err = b.recordChange(tx, index, "create", key, nil)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Delete removes the key
func (b *SqlBackend) Delete(key string, condition DeleteCondition) (node *models.Node, index int64, err error) {
	if key == "/" {
		return nil, 0, b.readOnlyError()
	}

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

	index, err = b.incrementIndex(tx)
	if err != nil {
		return nil, 0, err
	}

	node, err = b.getOne(tx, key)
	if err != nil {
		return nil, 0, err
	}

	prevIndex := index - 1

	if node == nil {
		return nil, 0, models.NotFound(key, prevIndex)
	}
	if node.Dir {
		return nil, 0, models.NotAFile(key, prevIndex)
	}

	if err := condition.Check(key, prevIndex, node); err != nil {
		return nil, 0, err
	}

	_, err = b.Query().Extend(`
		UPDATE "nodes" SET "deleted" = `, index,
		` WHERE "key" = `, key, ` AND "deleted" = 0`).Exec(tx)
	if err != nil {
		return nil, 0, err
	}

	err = b.recordChange(tx, index, condition.DeleteActionName(), key, node)
	if err != nil {
		return nil, 0, err
	}

	return node, index, nil
}

// RmDir removes the key for directories
func (b *SqlBackend) RmDir(key string, recursive bool, condition DeleteCondition) (node *models.Node, index int64, err error) {
	if key == "/" {
		return nil, 0, b.readOnlyError()
	}

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

	index, err = b.incrementIndex(tx)
	if err != nil {
		return nil, 0, err
	}

	// use the previous index in any errors
	prevIndex := index - 1

	node, err = b.getOne(tx, key)
	if err != nil {
		return nil, 0, err
	}

	if node == nil {
		return nil, 0, models.NotFound(key, prevIndex)
	}

	if err := condition.Check(key, prevIndex, node); err != nil {
		return nil, 0, err
	}

	query := b.Query().Extend(`
		UPDATE nodes SET deleted = `, index,
		` WHERE deleted = 0 AND ("key" = `, key, ` OR "key" LIKE `, key+"/%", `)`)
	res, err := query.Exec(tx)
	if err != nil {
		return nil, 0, err
	}

	if !recursive {
		rowsDeleted, err := res.RowsAffected()
		if err != nil {
			return nil, 0, err
		}
		if rowsDeleted > 1 {
			return nil, 0, models.DirectoryNotEmpty(key, prevIndex)
		}
	}

	err = b.recordChange(tx, index, condition.DeleteActionName(), key, node)
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
	if i < 0 {
		return ""
	}
	if i == 0 {
		return "/"
	}
	return key[:i]
}

func (b *SqlBackend) currIndex(db Querier) (index int64, err error) {
	err = db.QueryRow(`SELECT "index" FROM "index"`).Scan(&index)
	return
}

func (b *SqlBackend) incrementIndex(db Querier) (index int64, err error) {
	return b.dialect.incrementIndex(db)
}

func pathDepth(key string) int {
	if key == "/" {
		return 0
	}
	return strings.Count(key, "/")
}
