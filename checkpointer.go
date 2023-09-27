package pglogicalstream

type CheckPointer interface {
	SetCheckPoint(lsn, slot string) error
	GetCheckPoint(slot string) string
}
