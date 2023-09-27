package pglogicalstream

type OnMessage = func(message []byte)
