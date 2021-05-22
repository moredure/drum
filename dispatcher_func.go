package drum

type DispatcherFunc func(interface{})

func (d DispatcherFunc) UniqueKeyCheckEvent(event *UniqueKeyCheckEvent) {
	d(event)
}

func (d DispatcherFunc) DuplicateKeyCheckEvent(event *DuplicateKeyCheckEvent) {
	d(event)
}

func (d DispatcherFunc) UniqueKeyUpdateEvent(event *UniqueKeyUpdateEvent) {
	d(event)
}

func (d DispatcherFunc) DuplicateKeyUpdateEvent(event *DuplicateKeyUpdateEvent) {
	d(event)
}

func (d DispatcherFunc) UpdateEvent(event *UpdateEvent) {
	d(event)
}
