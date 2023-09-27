package replication

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
)

type Wal2JsonChanges struct {
	Changes []Wal2JsonChange `json:"change"`
}

type Wal2JsonChange struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnTypes  []string      `json:"columntypes"`
	ColumnValues []interface{} `json:"columnvalues"`
}

type Snapshotter struct {
	pgConnection *sql.DB
	snapshotName string
}

func NewSnapshotter(dbConf pgconn.Config, snapshotName string) (*Snapshotter, error) {
	var sslMode = "none"
	if dbConf.TLSConfig != nil {
		sslMode = "require"
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", dbConf.User,
		dbConf.Password, dbConf.Host, dbConf.Port, dbConf.Database, sslMode,
	)

	pgConn, err := sql.Open("postgres", connStr)

	return &Snapshotter{
		pgConnection: pgConn,
		snapshotName: snapshotName,
	}, err
}

func (s *Snapshotter) Prepare() error {
	if _, err := s.pgConnection.Exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"); err != nil {
		return err
	}
	if _, err := s.pgConnection.Exec(fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", s.snapshotName)); err != nil {
		return err
	}

	return nil
}

func (s *Snapshotter) QuerySnapshotData(schema, table string) (rows *sql.Rows, err error) {
	fmt.Println(fmt.Sprintf("SELECT * FROM %s.%s;", schema, table), "Snapshot query")
	return s.pgConnection.Query(fmt.Sprintf("SELECT * FROM %s.%s;", schema, table))
}

func (s *Snapshotter) ReleaseSnapshot() error {
	_, err := s.pgConnection.Exec("COMMIT;")
	return err
}

func (s *Snapshotter) CloseConn() error {
	if s.pgConnection != nil {
		return s.pgConnection.Close()
	}

	return nil
}
