package pglogicalstream

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
)

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

func (s *Snapshotter) FindAvgRowSize(table string) sql.NullInt64 {
	var avgRowSize sql.NullInt64

	if rows, err := s.pgConnection.Query(fmt.Sprintf(`SELECT SUM(pg_column_size('%s.*')) / COUNT(*) FROM %s;`, table, table)); err != nil {
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

func (s *Snapshotter) CalculateBatchSize(availableMemory uint64, estimatedRowSize uint64) int {
	// Adjust this factor based on your system's memory constraints.
	// This example uses a safety factor of 0.8 to leave some memory headroom.
	safetyFactor := 0.6
	batchSize := int(float64(availableMemory) * safetyFactor / float64(estimatedRowSize))
	if batchSize < 1 {
		batchSize = 1
	}
	return batchSize
}

func (s *Snapshotter) QuerySnapshotData(table string, pk string, limit, offset int) (rows *sql.Rows, err error) {
	fmt.Println("Query snapshot: ", fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d OFFSET %d;", table, pk, limit, offset))
	return s.pgConnection.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d OFFSET %d;", table, pk, limit, offset))
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
