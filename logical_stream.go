package pglogicalstream

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/usedatabrew/pglogicalstream/internal/helpers"
	"github.com/usedatabrew/pglogicalstream/internal/replication"
	"log"
	"strings"
	"time"
)

const outputPlugin = "wal2json"

var pluginArguments = []string{"\"pretty-print\" 'true'"}

type Stream struct {
	pgConn       *pgconn.PgConn
	checkPointer CheckPointer
	// extra copy of db config is required to establish a new db connection
	// which is required to take snapshot data
	dbConfig                   pgconn.Config
	ctx                        context.Context
	cancel                     context.CancelFunc
	clientXLogPos              pglogrepl.LSN
	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan []byte
	snapshotMessages           chan []byte
	snapshotName               string
	changeFilter               replication.ChangeFilter
	lsnrestart                 pglogrepl.LSN
	slotName                   string
	schema                     string
	tables                     []string
	separateChanges            bool
	snapshotBatchSize          int
}

func NewPgStream(config Config, checkpointer CheckPointer) (*Stream, error) {
	var (
		cfg *pgconn.Config
		err error
	)
	if cfg, err = pgconn.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database",
		config.DbUser,
		config.DbPassword,
		config.DbHost,
		config.DbPort,
		config.DbName,
	)); err != nil {
		return nil, err
	}
	if config.TlsVerify == TlsRequireVerify {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	dbConn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	stream := &Stream{
		pgConn:           dbConn,
		dbConfig:         *cfg,
		messages:         make(chan []byte),
		snapshotMessages: make(chan []byte, 100),
		slotName:         config.ReplicationSlotName,
		schema:           config.DbSchema,
		tables:           config.DbTables,
		checkPointer:     checkpointer,
		separateChanges:  config.SeparateChanges,
		changeFilter:     replication.NewChangeFilter(config.DbTables, config.DbSchema),
	}

	result := stream.pgConn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS pglog_stream_%s;", config.ReplicationSlotName))
	_, err = result.ReadAll()
	if err != nil {
		log.Fatalln("drop publication if exists error", err)
	}

	// TODO:: ADD Tables filter
	for i, table := range config.DbTables {
		config.DbTables[i] = fmt.Sprintf("%s.%s", config.DbSchema, table)
	}

	tablesSchemaFilter := fmt.Sprintf("FOR TABLE %s", strings.Join(config.DbTables, ","))
	fmt.Println("Create publication for tables", fmt.Sprintf("CREATE PUBLICATION pglog_stream_%s %s;", config.ReplicationSlotName, tablesSchemaFilter))
	result = stream.pgConn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION pglog_stream_%s %s;", config.ReplicationSlotName, tablesSchemaFilter))
	_, err = result.ReadAll()
	if err != nil {
		log.Fatalln("create publication error", err)
	}
	log.Println("created publication", config.ReplicationSlotName)

	sysident, err := pglogrepl.IdentifySystem(context.Background(), stream.pgConn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	var freshlyCreatedSlot = false
	var confirmedLSNFromDB string
	// check is replication slot exist to get last restart SLN
	connExecResult := stream.pgConn.Exec(context.TODO(), fmt.Sprintf("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = '%s'", config.ReplicationSlotName))
	if slotCheckResults, err := connExecResult.ReadAll(); err != nil {
		log.Fatalln(err)
	} else {
		if len(slotCheckResults) == 0 || len(slotCheckResults[0].Rows) == 0 {
			fmt.Println(stream.slotName)
			// here we create a new replication slot because there is no slot found
			var createSlotResult replication.CreateReplicationSlotResult
			createSlotResult, err = replication.CreateReplicationSlot(context.Background(), stream.pgConn, stream.slotName, outputPlugin,
				replication.CreateReplicationSlotOptions{Temporary: false,
					SnapshotAction: "export",
				})
			if err != nil {
				log.Fatalln("CreateReplicationSlot failed:", err)
			}
			stream.snapshotName = createSlotResult.SnapshotName
			freshlyCreatedSlot = true
		} else {
			slotCheckRow := slotCheckResults[0].Rows[0]
			confirmedLSNFromDB = string(slotCheckRow[0])

			if stream.checkPointer != nil {
				lsnFromStorage := stream.checkPointer.GetCheckPoint(config.ReplicationSlotName)
				if lsnFromStorage != "" {
					fmt.Printf("Extracted checkpoint from storage %s", lsnFromStorage)
					confirmedLSNFromDB = lsnFromStorage
				} else {
					log.Printf("Extracted restart lsn from db %s", confirmedLSNFromDB)
				}
			}
		}
	}

	var lsnrestart pglogrepl.LSN
	if freshlyCreatedSlot {
		lsnrestart = sysident.XLogPos
	} else {
		lsnrestart, _ = pglogrepl.ParseLSN(confirmedLSNFromDB)
	}

	stream.lsnrestart = lsnrestart
	stream.clientXLogPos = sysident.XLogPos
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
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", s.slotName)
}

func (s *Stream) streamMessagesAsync() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			if time.Now().After(s.nextStandbyMessageDeadline) {
				var err error
				err = pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: s.clientXLogPos})
				if err != nil {
					log.Fatalf("SendStandbyStatusUpdate failed: %s", err.Error())
				}
				log.Printf("Sent Standby status message at %s\n", s.clientXLogPos.String())
				s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
			}

			ctx, cancel := context.WithDeadline(context.Background(), s.nextStandbyMessageDeadline)
			rawMsg, err := s.pgConn.ReceiveMessage(ctx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				log.Fatalln("ReceiveMessage failed:", err)
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				log.Fatalf("received Postgres WAL error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				log.Printf("Received unexpected message: %T\n", rawMsg)
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}

				if pkm.ReplyRequested {
					s.nextStandbyMessageDeadline = time.Time{}
				}

				if s.checkPointer != nil {
					err := s.checkPointer.SetCheckPoint(pkm.ServerWALEnd.String(), s.slotName)
					if err != nil {
						fmt.Println("Failed to store checkpoint", s.clientXLogPos.String())
					}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}

				s.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				if s.checkPointer != nil {
					err := s.checkPointer.SetCheckPoint(s.clientXLogPos.String(), s.slotName)
					if err != nil {
						fmt.Println("Failed to store checkpoint", s.clientXLogPos.String())
					}
				}

				s.changeFilter.FilterChange(xld.WALData, s.separateChanges, func(change []byte) {
					s.messages <- change
				})
			}
		}
	}
}
func (s *Stream) processSnapshot() {
	snapshotter, err := replication.NewSnapshotter(s.dbConfig, s.snapshotName)
	if err != nil {
		log.Fatalln("Can't init database snapshot", err)
	}
	if err = snapshotter.Prepare(); err != nil {
		log.Fatalln("Can't prepare database snapshot", err)
	}

	// After we finished. We must release snapshot connection
	// We have to commit transaction and close DB connection
	defer func() {
		snapshotter.ReleaseSnapshot()
		snapshotter.CloseConn()
	}()

	for _, table := range s.tables {
		log.Printf("Processing snapshot for a table %s.%s", s.schema, table)

		var (
			avgRowSizeBytes sql.NullInt64
			offset          = 0
		)
		// extract only the name of the table
		rawTableName := strings.Split(table, ".")[1]
		avgRowSizeBytes = snapshotter.FindAvgRowSize(rawTableName)
		fmt.Println(avgRowSizeBytes, offset, "AVG SIZES")

		batchSize := snapshotter.CalculateBatchSize(helpers.GetAvailableMemory(), uint64(avgRowSizeBytes.Int64))
		fmt.Println("Query with batch size", batchSize, "Available memory: ", helpers.GetAvailableMemory(), "Avg row size: ", avgRowSizeBytes.Int64)

		for {
			var snapshotRows *sql.Rows
			if snapshotRows, err = snapshotter.QuerySnapshotData(table, batchSize, offset); err != nil {
				log.Fatalln("Can't query snapshot data", err)
			}

			columnTypes, err := snapshotRows.ColumnTypes()
			var columnTypesString = make([]string, len(columnTypes))
			columnNames, err := snapshotRows.Columns()
			for i, _ := range columnNames {
				columnTypesString[i] = columnTypes[i].DatabaseTypeName()
			}

			if err != nil {
				panic(err)
			}

			count := len(columnTypes)
			var rowsCount = 0
			for snapshotRows.Next() {
				rowsCount += 1
				scanArgs := make([]interface{}, count)
				for i, v := range columnTypes {
					switch v.DatabaseTypeName() {
					case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
						scanArgs[i] = new(sql.NullString)
						break
					case "BOOL":
						scanArgs[i] = new(sql.NullBool)
						break
					case "INT4":
						scanArgs[i] = new(sql.NullInt64)
						break
					default:
						scanArgs[i] = new(sql.NullString)
					}
				}

				err := snapshotRows.Scan(scanArgs...)

				if err != nil {
					panic(err)
				}

				var columnValues = make([]interface{}, len(columnTypes))
				for i, _ := range columnTypes {
					if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
						columnValues[i] = z.Bool
						continue
					}
					if z, ok := (scanArgs[i]).(*sql.NullString); ok {
						columnValues[i] = z.String
						continue
					}
					if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
						columnValues[i] = z.Int64
						continue
					}
					if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
						columnValues[i] = z.Float64
						continue
					}
					if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
						columnValues[i] = z.Int32
						continue
					}
					columnValues[i] = scanArgs[i]
				}

				var snapshotChanges []replication.Wal2JsonChange
				snapshotChanges = append(snapshotChanges, replication.Wal2JsonChange{
					Kind:         "insert",
					Schema:       s.schema,
					Table:        table,
					ColumnNames:  columnNames,
					ColumnTypes:  columnTypesString,
					ColumnValues: columnValues,
				})
				snapshotChangePacket := replication.Wal2JsonChanges{
					Changes: snapshotChanges,
				}
				changePacket, _ := json.Marshal(&snapshotChangePacket)
				s.snapshotMessages <- changePacket
			}

			offset += batchSize

			if batchSize != rowsCount {
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

func (s *Stream) SnapshotMessageC() chan []byte {
	return s.snapshotMessages
}

func (s *Stream) LrMessageC() chan []byte {
	return s.messages
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
