package pglogicalstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/charmbracelet/log"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/usedatabrew/pglogicalstream/internal/helpers"
	"github.com/usedatabrew/pglogicalstream/internal/replication"
	"github.com/usedatabrew/pglogicalstream/internal/schemas"
	"os"
	"strings"
	"time"
)

const outputPlugin = "wal2json"

var pluginArguments = []string{"\"pretty-print\" 'true'"}

type Stream struct {
	pgConn *pgconn.PgConn
	// extra copy of db config is required to establish a new db connection
	// which is required to take snapshot data
	dbConfig                   pgconn.Config
	ctx                        context.Context
	cancel                     context.CancelFunc
	clientXLogPos              pglogrepl.LSN
	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan replication.Wal2JsonChanges
	snapshotMessages           chan replication.Wal2JsonChanges
	snapshotName               string
	changeFilter               replication.ChangeFilter
	lsnrestart                 pglogrepl.LSN
	slotName                   string
	schema                     string
	tableSchemas               []schemas.DataTableSchema
	tableNames                 []string
	separateChanges            bool
	snapshotBatchSize          int
	snapshotMemorySafetyFactor float64
	logger                     *log.Logger
}

func NewPgStream(config Config, logger *log.Logger) (*Stream, error) {
	var (
		cfg *pgconn.Config
		err error
	)

	sslVerifyFull := ""
	if config.TlsVerify == TlsRequireVerify {
		sslVerifyFull = "&sslmode=verify-full"
	}

	link := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database%s",
		config.DbUser,
		config.DbPassword,
		config.DbHost,
		config.DbPort,
		config.DbName,
		sslVerifyFull,
	)
	fmt.Println(link)
	if cfg, err = pgconn.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database%s",
		config.DbUser,
		config.DbPassword,
		config.DbHost,
		config.DbPort,
		config.DbName,
		sslVerifyFull,
	)); err != nil {
		return nil, err
	}

	if config.TlsVerify == TlsRequireVerify {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         config.DbHost,
		}
	}

	dbConn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	var tableNames []string
	var dataSchemas []schemas.DataTableSchema
	for _, table := range config.DbTablesSchema {
		tableNames = append(tableNames, strings.Split(table.Table, ".")[1])
		var dts schemas.DataTableSchema
		dts.TableName = table.Table
		var arrowSchemaFields []arrow.Field
		for _, col := range table.Columns {
			arrowSchemaFields = append(arrowSchemaFields, arrow.Field{
				Name:     col.Name,
				Type:     helpers.MapPlainTypeToArrow(col.DatabrewType),
				Nullable: col.Nullable,
				Metadata: arrow.Metadata{},
			})
		}
		dts.Schema = arrow.NewSchema(arrowSchemaFields, nil)
		dataSchemas = append(dataSchemas, dts)
	}

	stream := &Stream{
		pgConn:                     dbConn,
		dbConfig:                   *cfg,
		messages:                   make(chan replication.Wal2JsonChanges),
		snapshotMessages:           make(chan replication.Wal2JsonChanges, 100),
		slotName:                   config.ReplicationSlotName,
		schema:                     config.DbSchema,
		tableSchemas:               dataSchemas,
		snapshotMemorySafetyFactor: config.SnapshotMemorySafetyFactor,
		separateChanges:            config.SeparateChanges,
		snapshotBatchSize:          config.BatchSize,
		tableNames:                 tableNames,
		changeFilter:               replication.NewChangeFilter(dataSchemas, config.DbSchema),
		logger:                     logger,
	}

	result := stream.pgConn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS pglog_stream_%s;", config.ReplicationSlotName))
	_, err = result.ReadAll()
	if err != nil {
		stream.logger.Errorf("drop publication if exists error %s", err.Error())
	}

	// TODO:: ADD Tables filter
	for i, table := range tableNames {
		tableNames[i] = fmt.Sprintf("%s.%s", config.DbSchema, table)
	}

	tablesSchemaFilter := fmt.Sprintf("FOR TABLE %s", strings.Join(tableNames, ","))
	stream.logger.Infof("Create publication for table schemas with query %s", fmt.Sprintf("CREATE PUBLICATION pglog_stream_%s %s;", config.ReplicationSlotName, tablesSchemaFilter))
	result = stream.pgConn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION pglog_stream_%s %s;", config.ReplicationSlotName, tablesSchemaFilter))
	_, err = result.ReadAll()
	if err != nil {
		stream.logger.Fatalf("create publication error %s", err.Error())
	}
	stream.logger.Info("Created Postgresql publication", "publication_name", config.ReplicationSlotName)

	sysident, err := pglogrepl.IdentifySystem(context.Background(), stream.pgConn)
	if err != nil {
		stream.logger.Fatalf("Failed to identify the system %s", err.Error())
	}

	stream.logger.Info("System identification result", "SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	var freshlyCreatedSlot = false
	var confirmedLSNFromDB string
	// check is replication slot exist to get last restart SLN
	connExecResult := stream.pgConn.Exec(context.TODO(), fmt.Sprintf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'", config.ReplicationSlotName))
	if slotCheckResults, err := connExecResult.ReadAll(); err != nil {
		stream.logger.Fatal(err)
	} else {
		if len(slotCheckResults) == 0 || len(slotCheckResults[0].Rows) == 0 {
			// here we create a new replication slot because there is no slot found
			var createSlotResult replication.CreateReplicationSlotResult
			createSlotResult, err = replication.CreateReplicationSlot(context.Background(), stream.pgConn, stream.slotName, outputPlugin,
				replication.CreateReplicationSlotOptions{Temporary: false,
					SnapshotAction: "export",
				})
			if err != nil {
				stream.logger.Fatalf("Failed to create replication slot for the database: %s", err.Error())
			}
			stream.snapshotName = createSlotResult.SnapshotName
			freshlyCreatedSlot = true
		} else {
			slotCheckRow := slotCheckResults[0].Rows[0]
			confirmedLSNFromDB = string(slotCheckRow[0])
			stream.logger.Info("Replication slot restart LSN extracted from DB", "LSN", confirmedLSNFromDB)
		}
	}

	var lsnrestart pglogrepl.LSN
	if freshlyCreatedSlot {
		lsnrestart = sysident.XLogPos
	} else {
		lsnrestart, _ = pglogrepl.ParseLSN(confirmedLSNFromDB)
	}

	stream.lsnrestart = lsnrestart

	if freshlyCreatedSlot {
		stream.clientXLogPos = sysident.XLogPos
	} else {
		stream.clientXLogPos = lsnrestart
	}

	stream.standbyMessageTimeout = time.Second * 10
	stream.nextStandbyMessageDeadline = time.Now().Add(stream.standbyMessageTimeout)
	stream.ctx, stream.cancel = context.WithCancel(context.Background())

	if !freshlyCreatedSlot || config.StreamOldData == false {
		stream.startLr()
		go stream.streamMessagesAsync()
	} else {
		// New messages will be streamed after the snapshot has been processed.
		go stream.processSnapshot()
	}

	return stream, err
}

func (s *Stream) startLr() {
	var err error
	err = pglogrepl.StartReplication(context.Background(), s.pgConn, s.slotName, s.lsnrestart, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		s.logger.Fatalf("Starting replication slot failed: %s", err.Error())
	}
	s.logger.Info("Started logical replication on slot", "slot-name", s.slotName)
}

func (s *Stream) AckLSN(lsn string) {
	var err error
	s.clientXLogPos, err = pglogrepl.ParseLSN(lsn)
	if err != nil {
		s.logger.Fatalf("Failed to parse LSN for Acknowledge %s", err.Error())
	}

	err = pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.clientXLogPos,
		WALFlushPosition: s.clientXLogPos,
	})

	if err != nil {
		s.logger.Fatalf("SendStandbyStatusUpdate failed: %s", err.Error())
	}
	s.logger.Debugf("Sent Standby status message at LSN#%s", s.clientXLogPos.String())
	s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
}

func (s *Stream) streamMessagesAsync() {
	for {
		select {
		case <-s.ctx.Done():
			s.cancel()
			return
		default:
			if time.Now().After(s.nextStandbyMessageDeadline) {
				var err error
				err = pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: s.clientXLogPos,
				})

				if err != nil {
					s.logger.Fatalf("SendStandbyStatusUpdate failed: %s", err.Error())
				}
				s.logger.Debugf("Sent Standby status message at LSN#%s", s.clientXLogPos.String())
				s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
			}

			ctx, cancel := context.WithDeadline(context.Background(), s.nextStandbyMessageDeadline)
			rawMsg, err := s.pgConn.ReceiveMessage(ctx)
			s.cancel = cancel
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				s.logger.Fatalf("Failed to receive messages from PostgreSQL %s", err.Error())
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				s.logger.Fatalf("Received broken Postgres WAL. Error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				s.logger.Warnf("Received unexpected message: %T\n", rawMsg)
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					s.logger.Fatalf("ParsePrimaryKeepaliveMessage failed: %s", err.Error())
				}

				if pkm.ReplyRequested {
					s.nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					s.logger.Fatalf("ParseXLogData failed: %s", err.Error())
				}
				clientXLogPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				s.changeFilter.FilterChange(clientXLogPos.String(), xld.WALData, func(change replication.Wal2JsonChanges) {
					s.messages <- change
				})
			}
		}
	}
}
func (s *Stream) processSnapshot() {
	snapshotter, err := replication.NewSnapshotter(s.dbConfig, s.snapshotName)
	if err != nil {
		s.logger.Errorf("Failed to create database snapshot: %", err.Error())
		s.cleanUpOnFailure()
		os.Exit(1)
	}
	if err = snapshotter.Prepare(); err != nil {
		s.logger.Errorf("Failed to prepare database snapshot: %", err.Error())
		s.cleanUpOnFailure()
		os.Exit(1)
	}
	defer func() {
		snapshotter.ReleaseSnapshot()
		snapshotter.CloseConn()
	}()

	for _, table := range s.tableSchemas {
		s.logger.Info("Processing database snapshot", "schema", s.schema, "table", table)

		var offset = 0

		pk, err := s.getPrimaryKeyColumn(table.TableName)
		if err != nil {
			s.logger.Fatalf("Failed to resolve pk %s", err.Error())
		}

		s.logger.Info("Query snapshot", "batch-size", s.snapshotBatchSize)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, table.Schema)

		colNames := make([]string, 0, len(table.Schema.Fields()))

		for _, col := range table.Schema.Fields() {
			colNames = append(colNames, pgx.Identifier{col.Name}.Sanitize())
		}

		for {
			var snapshotRows pgx.Rows
			s.logger.Info("Query snapshot: ", "table", table.TableName, "columns", colNames, "batch-size", s.snapshotBatchSize, "offset", offset)
			if snapshotRows, err = snapshotter.QuerySnapshotData(table.TableName, colNames, pk, s.snapshotBatchSize, offset); err != nil {
				s.logger.Errorf("Failed to query snapshot data %s", err.Error())
				s.cleanUpOnFailure()
				os.Exit(1)
			}

			var rowsCount = 0
			for snapshotRows.Next() {
				rowsCount += 1

				values, err := snapshotRows.Values()
				if err != nil {
					panic(err)
				}

				for i, v := range values {
					s := scalar.NewScalar(table.Schema.Field(i).Type)
					if err := s.Set(v); err != nil {
						panic(err)
					}

					scalar.AppendToBuilder(builder.Field(i), s)
				}
				var snapshotChanges = replication.Wal2JsonChanges{
					Lsn: "",
					Changes: []replication.Wal2JsonChange{
						{
							Kind:   "insert",
							Schema: s.schema,
							Table:  strings.Split(table.TableName, ".")[1],
							Row:    builder.NewRecord(),
						},
					},
				}

				s.snapshotMessages <- snapshotChanges
			}

			snapshotRows.Close()

			offset += s.snapshotBatchSize

			if s.snapshotBatchSize != rowsCount {
				break
			}
		}

	}

	s.startLr()
	go s.streamMessagesAsync()
}

func (s *Stream) OnMessage(callback OnMessage) {
	for {
		select {
		case snapshotMessage := <-s.snapshotMessages:
			callback(snapshotMessage)
		case message := <-s.messages:
			callback(message)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Stream) SnapshotMessageC() chan replication.Wal2JsonChanges {
	return s.snapshotMessages
}

func (s *Stream) LrMessageC() chan replication.Wal2JsonChanges {
	return s.messages
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Stream) cleanUpOnFailure() {
	s.logger.Warn("Cleaning up resources on accident.", "replication-slot", s.slotName)
	err := replication.DropReplicationSlot(context.Background(), s.pgConn, s.slotName, replication.DropReplicationSlotOptions{Wait: true})
	if err != nil {
		s.logger.Errorf("Failed to drop replication slot: %s", err.Error())
	}
	s.pgConn.Close(context.TODO())
}

func (s *Stream) getPrimaryKeyColumn(tableName string) (string, error) {
	q := fmt.Sprintf(`
		SELECT a.attname
		FROM   pg_index i
		JOIN   pg_attribute a ON a.attrelid = i.indrelid
							 AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = '%s'::regclass
		AND    i.indisprimary;	
	`, strings.Split(tableName, ".")[1])

	reader := s.pgConn.Exec(context.Background(), q)
	data, err := reader.ReadAll()
	if err != nil {
		return "", err
	}

	pkResultRow := data[0].Rows[0]
	pkColName := string(pkResultRow[0])
	return pkColName, nil
}

func (s *Stream) Stop() error {
	if s.pgConn != nil {
		if s.ctx != nil {
			s.cancel()
		}

		return s.pgConn.Close(context.TODO())
	}

	return nil
}
