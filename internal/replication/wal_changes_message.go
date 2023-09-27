package replication

type WallMessage struct {
	Change []struct {
		Kind         string        `json:"kind"`
		Schema       string        `json:"schema"`
		Table        string        `json:"table"`
		Columnnames  []string      `json:"columnnames"`
		Columntypes  []string      `json:"columntypes"`
		Columnvalues []interface{} `json:"columnvalues"`
		Oldkeys      struct {
			Keynames  []string      `json:"keynames"`
			Keytypes  []string      `json:"keytypes"`
			Keyvalues []interface{} `json:"keyvalues"`
		} `json:"oldkeys"`
	} `json:"change"`
}
