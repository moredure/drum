package drum

type Dispatcher interface {
	UniqueKeyCheckEvent(*UniqueKeyCheckEvent)
	DuplicateKeyCheckEvent(*DuplicateKeyCheckEvent)
	UniqueKeyUpdateEvent(*UniqueKeyUpdateEvent)
	DuplicateKeyUpdateEvent(*DuplicateKeyUpdateEvent)
	UpdateEvent(*UpdateEvent)
}
