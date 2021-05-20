package drum

import (
	"encoding/binary"
	"io"
	"os"
	"sort"
)

type DRUM interface {
	Check(key uint64, aux []byte)
	Update(key uint64, value, aux []byte)
	CheckAndUpdate(key uint64, value, aux []byte)
}

type Event struct {
	Key        uint64
	Value, Aux []byte
}

type UpdateEvent Event
type DuplicateKeyUpdateEvent Event
type UniqueKeyUpdateEvent Event
type DuplicateKeyCheckEvent Event
type UniqueKeyCheckEvent Event

type keyVal struct {
	Key      uint64
	Value    []byte
	Op       byte
	Position int
	Result   byte
}

type drum struct {
	merge      bool
	feed       bool
	dispatcher chan interface{}
	buckets    int
	elements   int

	auxBuffers          [][][]byte
	kvBuffers           [][]*keyVal
	fileNames           [][2]string
	currentPointers     [][2]int64
	nextBufferPosisions []int
	size                int64

	buf [8]byte

	db DB

	sortedMergeBuffer []*keyVal
	unsortingHelper   []int
	unsortedAuxBuffer [][]byte
}

type DB interface {
	Has(uint64) bool
	Put(uint64, []byte)
	Get(uint64) []byte
	Sync()
}

func (d *drum) getBucketIdentififer(key uint64) int {
	return int(key % uint64(d.buckets))
}

func (d *drum) readInfoBucketIntoMergeBuffer(bucket int) {
	kv, err := os.Open(d.fileNames[bucket][0])
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	written := d.currentPointers[bucket][0]

	for pos, _ := kv.Seek(0, io.SeekCurrent); pos < written; pos, _ = kv.Seek(0, io.SeekCurrent) {
		d.sortedMergeBuffer = append(d.sortedMergeBuffer, new(keyVal))
		element := d.sortedMergeBuffer[len(d.sortedMergeBuffer)-1]
		element.Position = len(d.sortedMergeBuffer) - 1

		kv.Read(d.buf[0:1])
		element.Op = d.buf[0]

		kv.Read(d.buf[:])
		element.Key = binary.BigEndian.Uint64(d.buf[:])

		kv.Read(d.buf[:])

		element.Value = make([]byte, binary.BigEndian.Uint64(d.buf[:]))
		kv.Read(element.Value)
	}
}

func (d *drum) sortMergeBuffer() {
	sort.Slice(d.sortedMergeBuffer, func(i, j int) bool {
		return d.sortedMergeBuffer[i].Key < d.sortedMergeBuffer[j].Key
	})
}

func (d *drum) synchronizeWithDisk() {
	for _, element := range d.sortedMergeBuffer {
		if Check == element.Op || CheckUpdate == element.Op {
			if !d.db.Has(element.Key) {
				element.Result = UniqueKey
			} else {
				element.Result = DuplicateKey
				if Check == element.Op {
					element.Value = d.db.Get(element.Key)
				}
			}
		}
		if Update == element.Op || CheckUpdate == element.Op {
			d.db.Put(element.Key, element.Value)
		}
	}
	d.db.Sync()
}

func (d *drum) unsortMergeBuffer() {
	if cap(d.unsortingHelper) >= len(d.sortedMergeBuffer) {
		d.unsortingHelper = d.unsortingHelper[:len(d.sortedMergeBuffer)]
	} else {
		d.unsortingHelper = append(d.unsortingHelper, make([]int, len(d.sortedMergeBuffer) - len(d.unsortingHelper))...)
	}
	for i := 0; i < len(d.sortedMergeBuffer); i += 1 {
		d.unsortingHelper[d.sortedMergeBuffer[i].Position] = i
	}
}

func (d *drum) readAuxBucketForDispatching(bucket int) {
	aux, err := os.Open(d.fileNames[bucket][1])
	if err != nil {
		panic(err)
	}
	defer aux.Close()

	auxWritten := d.currentPointers[bucket][1]

	for pos, _ := aux.Seek(0, io.SeekCurrent); pos < auxWritten; pos, _ = aux.Seek(0, io.SeekCurrent) {
		d.unsortedAuxBuffer = append(d.unsortedAuxBuffer, nil)
		aux.Read(d.buf[:])
		serial := make([]byte, binary.BigEndian.Uint64(d.buf[:]))
		aux.Read(serial)
		d.unsortedAuxBuffer[len(d.unsortedAuxBuffer)-1] = serial
	}
}

const (
	UniqueKey    byte = iota
	DuplicateKey
)

func (d *drum) dispatch() {
	for i := 0; i < len(d.unsortingHelper); i += 1 {
		idx := d.unsortingHelper[i]
		e := d.sortedMergeBuffer[idx]
		aux := d.unsortedAuxBuffer[i]

		if Check == e.Op && UniqueKey == e.Result {
			d.dispatcher <- UniqueKeyCheckEvent{
				Key: e.Key,
				Aux: aux,
			}
		} else if Check == e.Op && DuplicateKey == e.Result {
			d.dispatcher <- DuplicateKeyCheckEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			}
		} else if CheckUpdate == e.Op && UniqueKey == e.Result {
			d.dispatcher <- UniqueKeyUpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			}
		} else if CheckUpdate == e.Op && DuplicateKey == e.Result {
			d.dispatcher <- DuplicateKeyUpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			}
		} else if Update == e.Op {
			d.dispatcher <- UpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			}
		} else {
			panic("not implemented")
		}
	}

}

func (d *drum) resetSynchronizationBuffers() {
	d.sortedMergeBuffer = make([]*keyVal, 0, d.elements)
	d.unsortingHelper = make([]int, 0, d.elements)
	d.unsortedAuxBuffer = make([][]byte, 0, d.elements)
}

func (d *drum) resetFilePointers() {
	for bucket := 0; bucket < d.buckets; bucket += 1 {
		d.currentPointers[bucket] = [2]int64{0, 0}
	}
}

func (d *drum) mergeBuckets() {
	for bucket := 0; bucket < d.buckets; bucket += 1 {
		d.readInfoBucketIntoMergeBuffer(bucket)
		d.sortMergeBuffer()
		d.synchronizeWithDisk()
		d.unsortMergeBuffer()
		d.readAuxBucketForDispatching(bucket)
		d.dispatch()
		d.resetSynchronizationBuffers()
	}

	d.resetFilePointers()
	d.merge = false
}

func (d *drum) getBucketAndBufferPos(key uint64) (int, int) {
	bucket := d.getBucketIdentififer(key)
	d.nextBufferPosisions[bucket] += 1

	if d.nextBufferPosisions[bucket] == d.elements {
		d.feed = true
	}

	return bucket, d.nextBufferPosisions[bucket]
}

func (d *drum) add(key uint64, value []byte, op byte) (int, int) {
	bucket, position := d.getBucketAndBufferPos(key)
	d.kvBuffers[bucket][position] = &keyVal{
		Key:   key,
		Value: value,
		Op:    op,
	}
	return bucket, position
}

const (
	Check byte = iota
	Update
	CheckUpdate
)

func (d *drum) Check(key uint64, aux []byte) {
	bucket, position := d.add(key, nil, Check)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *drum) Update(key uint64, value, aux []byte) {
	bucket, position := d.add(key, value, Update)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *drum) CheckAndUpdate(key uint64, value, aux []byte) {
	bucket, position := d.add(key, value, CheckUpdate)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *drum) checkTimeToFeed() {
	if d.feed {
		d.feedBuckets()
	}
	d.checkTimeToMerge()
}

func (d *drum) resetNextBufferPositions() {
	for bucket := 0; bucket < d.buckets; bucket += 1 {
		d.nextBufferPosisions[bucket] = 0
	}
}

func (d *drum) feedBuckets() {
	for bucket := 0; bucket < d.buckets; bucket += 1 {
		d.feedBucket(bucket)
	}
	d.resetNextBufferPositions()
	d.feed = false
}

func (d *drum) feedBucket(bucket int) {
	current := d.nextBufferPosisions[bucket]
	if current == 0 {
		return
	}

	kv, err := os.Open(d.fileNames[bucket][0])
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	aux, err := os.Open(d.fileNames[bucket][1])
	if err != nil {
		panic(err)
	}
	defer aux.Close()

	kvBegin, err := kv.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	auxBegin, err := aux.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	d.currentPointers[bucket][0], _ = kv.Seek(0, io.SeekCurrent)
	d.currentPointers[bucket][1], _ = aux.Seek(0, io.SeekCurrent)

	for i := 0; i < current; i += 1 {
		element := d.kvBuffers[bucket][i]

		d.buf[0] = element.Op
		kv.Write(d.buf[0:1])
		binary.BigEndian.PutUint64(d.buf[:], element.Key)
		kv.Write(d.buf[:])
		binary.BigEndian.PutUint64(d.buf[:], uint64(len(element.Value)))
		kv.Write(d.buf[:])
		kv.Write(element.Value)
		a := d.auxBuffers[bucket][i]
		binary.BigEndian.PutUint64(d.buf[:], uint64(len(a)))
		kv.Write(d.buf[:])
		aux.Write(a)
	}

	d.currentPointers[bucket][0], err = kv.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	d.currentPointers[bucket][1], err = aux.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}

	if d.currentPointers[bucket][0]-kvBegin > d.size || d.currentPointers[bucket][1]-auxBegin > d.size {
		d.merge = true
	}
}

func (d *drum) checkTimeToMerge() {
	if d.merge {
		d.mergeBuckets()
	}
}

func NewDrum(buckets int, elements int, size int64, db DB, dispatcher chan interface{}) DRUM {
	auxBuffers := make([][][]byte, buckets)
	for i := range auxBuffers {
		auxBuffers[i] = make([][]byte, elements)
	}
	kvBuffers := make([][]*keyVal, buckets)
	for i := range kvBuffers {
		kvBuffers[i] = make([]*keyVal, elements)
	}
	d := &drum{
		dispatcher:          dispatcher,
		buckets:             buckets,
		elements:            elements,
		size:                size,
		db:                  db,
		auxBuffers:          auxBuffers,
		kvBuffers:           kvBuffers,
		fileNames:           make([][2]string, buckets),
		currentPointers:     make([][2]int64, buckets),
		nextBufferPosisions: make([]int, buckets),
	}
	d.resetSynchronizationBuffers()
	return d
}
