package drum

type Dispatcher interface {
	UniqueKeyCheckEvent(*Event)
	DuplicateKeyCheckEvent(*Event)
	UniqueKeyUpdateEvent(*Event)
	DuplicateKeyUpdateEvent(*Event)
	UpdateEvent(*Event)
}
