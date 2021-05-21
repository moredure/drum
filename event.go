package drum

type Event struct {
	Key        uint64
	Value, Aux []byte
}
