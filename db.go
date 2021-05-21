package drum

type DB interface {
	Has(uint64) bool
	Put(uint64, []byte)
	Get(uint64) []byte
	Sync()
}
