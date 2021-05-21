package drum

type keyVal struct {
	Key      uint64
	Value    []byte
	Op       byte
	Position int
	Result   byte
}
