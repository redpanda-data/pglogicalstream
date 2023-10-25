package pglogicalstream

type TlsVerify string

const TlsNoVerify TlsVerify = "none"
const TlsRequireVerify TlsVerify = "require"

type Config struct {
	DbHost                     string    `yaml:"db_host"`
	DbPassword                 string    `yaml:"db_password"`
	DbUser                     string    `yaml:"db_user"`
	DbPort                     int       `yaml:"db_port"`
	DbName                     string    `yaml:"db_name"`
	DbSchema                   string    `yaml:"db_schema"`
	DbTables                   []string  `yaml:"db_tables"`
	ReplicationSlotName        string    `yaml:"replication_slot_name"`
	TlsVerify                  TlsVerify `yaml:"tls_verify"`
	StreamOldData              bool      `yaml:"stream_old_data"`
	SeparateChanges            bool      `yaml:"separate_changes"`
	SnapshotMemorySafetyFactor float64   `yaml:"snapshot_memory_safety_factor"`
	BatchSize                  int       `yaml:"batch_size"`
}
