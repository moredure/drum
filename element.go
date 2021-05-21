package drum

type element struct {
	Key      uint64
	Value    []byte
	Op       byte
	Position int
	Result   byte
}
