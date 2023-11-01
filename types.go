package pglogicalstream

import "github.com/usedatabrew/pglogicalstream/internal/replication"

type OnMessage = func(message replication.Wal2JsonChanges)
