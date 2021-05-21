package drum

type (
	Event struct {
		Key        uint64
		Value, Aux []byte
	}
	UpdateEvent Event
	DuplicateKeyUpdateEvent Event
	UniqueKeyUpdateEvent Event
	DuplicateKeyCheckEvent Event
	UniqueKeyCheckEvent Event
)
