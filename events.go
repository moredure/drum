package drum

type (
	UniqueKeyCheckEvent struct {
		Key uint64
		Aux []byte
	}
	baseEvent struct {
		Key   uint64
		Value []byte
		Aux   []byte
	}
	DuplicateKeyCheckEvent  baseEvent
	UniqueKeyUpdateEvent    baseEvent
	DuplicateKeyUpdateEvent baseEvent
	UpdateEvent             baseEvent
)
