package replication

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
	"log"
	"strings"
)

type Wal2JsonChanges struct {
	Lsn     string
	Changes []Wal2JsonChange `json:"change"`
}

type Wal2JsonChange struct {
	Kind   string       `json:"action"`
	Schema string       `json:"schema"`
	Table  string       `json:"table"`
	Row    arrow.Record `json:"data"`
}

type Snapshotter struct {
	pgConnection *pgx.Conn
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

	pgConn, err := pgx.Connect(context.Background(), connStr)

	return &Snapshotter{
		pgConnection: pgConn,
		snapshotName: snapshotName,
	}, err
}

func (s *Snapshotter) Prepare() error {
	if res, err := s.pgConnection.Exec(context.TODO(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"); err != nil {
		return err
	} else {
		fmt.Println(res.String())
	}
	if res, err := s.pgConnection.Exec(context.TODO(), fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", s.snapshotName)); err != nil {
		return err
	} else {
		fmt.Println(res.RowsAffected())
	}

	return nil
}

func (s *Snapshotter) FindAvgRowSize(table string) sql.NullInt64 {
	var avgRowSize sql.NullInt64

	if rows, err := s.pgConnection.Query(context.TODO(), fmt.Sprintf(`SELECT SUM(pg_column_size('%s.*')) / COUNT(*) FROM %s;`, table, table)); err != nil {
		log.Fatal("Can get avg row size", err)
	} else {
		if rows.Next() {
			if err = rows.Scan(&avgRowSize); err != nil {
				log.Fatal("Can get avg row size", err)
			}
		} else {
			log.Fatal("Can get avg row size; 0 rows returned")
		}
	}

	return avgRowSize
}

func (s *Snapshotter) CalculateBatchSize(safetyFactor float64, availableMemory uint64, estimatedRowSize uint64) int {
	// Adjust this factor based on your system's memory constraints.
	// This example uses a safety factor of 0.8 to leave some memory headroom.
	batchSize := int(float64(availableMemory) * safetyFactor / float64(estimatedRowSize))
	if batchSize < 1 {
		batchSize = 1
	}
	return batchSize
}

func (s *Snapshotter) QuerySnapshotData(table string, columns []string, pk string, limit, offset int) (rows pgx.Rows, err error) {
	joinedColumns := strings.Join(columns, ", ")
	return s.pgConnection.Query(context.TODO(), fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d OFFSET %d;", joinedColumns, table, pk, limit, offset))
}

func (s *Snapshotter) ReleaseSnapshot() error {
	_, err := s.pgConnection.Exec(context.TODO(), "COMMIT;")
	return err
}

func (s *Snapshotter) CloseConn() error {
	if s.pgConnection != nil {
		return s.pgConnection.Close(context.TODO())
	}

	return nil
}
