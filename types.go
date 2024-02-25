package pglogicalstream

import (
	"github.com/usedatabrew/pglogicalstream/messages"
)

type OnMessage = func(message messages.Wal2JsonChanges)
