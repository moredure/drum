package drum

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
)

type DRUM struct {
	bucketsPath string

	shift uint64

	dispatcher Dispatcher

	merge, feed bool

	buckets, elements int

	size int64

	auxBuffers [][][]byte

	kvBuffers [][]*element

	fileNames []names

	currentPointers []pointers

	nextBufferPosisions []int

	sortedMergeBuffer []*element
	unsortingHelper   []int
	unsortedAuxBuffer [][]byte

	buf [8]byte

	db DB
}

const (
	check byte = iota
	update
	checkUpdate
	uniqueKey
	duplicateKey
)

func (d *DRUM) Check(key uint64, aux []byte) {
	bucket, position := d.add(key, nil, check)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *DRUM) Update(key uint64, value, aux []byte) {
	bucket, position := d.add(key, value, update)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *DRUM) CheckUpdate(key uint64, value, aux []byte) {
	bucket, position := d.add(key, value, checkUpdate)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *DRUM) Sync() {
	d.feedBuckets()
	d.mergeBuckets()
}

func (d *DRUM) readInfoBucketIntoMergeBuffer(bucket int) {
	kv, err := os.OpenFile(d.fileNames[bucket].Kv, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			panic(err)
		}
	}()

	for {
		position, err := kv.Seek(0, io.SeekCurrent)
		if err != nil {
			panic(err)
		}
		if position >= d.currentPointers[bucket].Kv {
			break
		}

		e := &element{
			Position: len(d.sortedMergeBuffer),
		}
		d.sortedMergeBuffer = append(d.sortedMergeBuffer, e)

		if _, err := kv.Read(d.buf[0:1]); err != nil {
			panic(err)
		}
		e.Op = d.buf[0]

		if _, err := kv.Read(d.buf[:]); err != nil {
			panic(err)
		}
		e.Key = binary.BigEndian.Uint64(d.buf[:])

		if _, err := kv.Read(d.buf[:4]); err != nil {
			panic(err)
		}
		e.Value = make([]byte, binary.BigEndian.Uint32(d.buf[:4]))
		if _, err := kv.Read(e.Value); err != nil {
			panic(err)
		}
	}
}

func (d *DRUM) sortMergeBuffer() {
	sort.Slice(d.sortedMergeBuffer, func(i, j int) bool {
		return d.sortedMergeBuffer[i].Key < d.sortedMergeBuffer[j].Key
	})
}

func (d *DRUM) synchronizeWithDisk() {
	for _, e := range d.sortedMergeBuffer {
		if check == e.Op || checkUpdate == e.Op {
			if d.db.Has(e.Key) {
				e.Result = duplicateKey
				old := d.db.Get(e.Key)
				if check == e.Op {
					e.Value = old
				} else if bytes.Equal(old, e.Value) {
					continue
				}
			} else {
				e.Result = uniqueKey
			}
		}
		if update == e.Op || checkUpdate == e.Op {
			d.db.Put(e.Key, e.Value)
		}
	}
	d.db.Sync()
}



func (d *DRUM) unsortMergeBuffer() {
	if cap(d.unsortingHelper) < len(d.sortedMergeBuffer) {
		d.unsortingHelper = append(d.unsortingHelper, make([]int, len(d.sortedMergeBuffer)-len(d.unsortingHelper))...)
	} else {
		d.unsortingHelper = d.unsortingHelper[:len(d.sortedMergeBuffer)]
	}
	for i := range d.sortedMergeBuffer {
		d.unsortingHelper[d.sortedMergeBuffer[i].Position] = i
	}
}

func (d *DRUM) readAuxBucketForDispatching(bucket int) {
	aux, err := os.OpenFile(d.fileNames[bucket].Aux, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := aux.Close(); err != nil {
			panic(err)
		}
	}()

	for {
		position, err := aux.Seek(0, io.SeekCurrent)
		if err != nil {
			panic(err)
		}
		if position >= d.currentPointers[bucket].Aux {
			break
		}

		if _, err := aux.Read(d.buf[:4]); err != nil {
			panic(err)
		}
		a := make([]byte, binary.BigEndian.Uint32(d.buf[:4]))
		if _, err := aux.Read(a); err != nil {
			panic(err)
		}
		d.unsortedAuxBuffer = append(d.unsortedAuxBuffer, a)
	}
}

func (d *DRUM) dispatch() {
	for i, idx := range d.unsortingHelper {
		if aux, e := d.unsortedAuxBuffer[i], d.sortedMergeBuffer[idx]; check == e.Op && uniqueKey == e.Result {
			d.dispatcher.UniqueKeyCheckEvent(&UniqueKeyCheckEvent{
				Key: e.Key,
				Aux: aux,
			})
		} else if check == e.Op && duplicateKey == e.Result {
			d.dispatcher.DuplicateKeyCheckEvent(&DuplicateKeyCheckEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			})
		} else if checkUpdate == e.Op && uniqueKey == e.Result {
			d.dispatcher.UniqueKeyUpdateEvent(&UniqueKeyUpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			})
		} else if checkUpdate == e.Op && duplicateKey == e.Result {
			d.dispatcher.DuplicateKeyUpdateEvent(&DuplicateKeyUpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			})
		} else if update == e.Op {
			d.dispatcher.UpdateEvent(&UpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			})
		} else {
			panic("not implemented")
		}
	}

}

func (d *DRUM) assignFileNames() {
	for bucket := range d.fileNames {
		d.fileNames[bucket] = names{
			Kv:  d.bucketsPath + "/" + strconv.Itoa(bucket) + "_bucket.kv",
			Aux: d.bucketsPath + "/" + strconv.Itoa(bucket) + "_bucket.aux",
		}
	}
}

func (d *DRUM) resetSynchronizationBuffers() {
	d.sortedMergeBuffer = make([]*element, 0, d.elements)
	d.unsortingHelper = d.unsortingHelper[:0]
	d.unsortedAuxBuffer = make([][]byte, 0, d.elements)
}

func (d *DRUM) resetFilePointers() {
	for bucket := range d.currentPointers {
		d.currentPointers[bucket] = pointers{}
	}
}

func (d *DRUM) mergeBuckets() {
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

func (d *DRUM) getBucketOfKey(key uint64) uint64 {
	return key >> d.shift
}

func (d *DRUM) getBucketAndBufferPosition(key uint64) (bucket uint64, position int) {
	bucket = d.getBucketOfKey(key)
	position = d.nextBufferPosisions[bucket]
	d.nextBufferPosisions[bucket] = position + 1
	if d.nextBufferPosisions[bucket] == d.elements {
		d.feed = true
	}
	return
}

func (d *DRUM) add(key uint64, value []byte, op byte) (bucket uint64, position int) {
	bucket, position = d.getBucketAndBufferPosition(key)
	d.kvBuffers[bucket][position] = &element{
		Key:   key,
		Value: value,
		Op:    op,
	}
	return
}

func (d *DRUM) checkTimeToFeed() {
	if d.feed {
		d.feedBuckets()
	}
	d.checkTimeToMerge()
}

func (d *DRUM) resetNextBufferPositions() {
	for bucket := range d.nextBufferPosisions {
		d.nextBufferPosisions[bucket] = 0
	}
}

func (d *DRUM) feedBuckets() {
	for bucket := 0; bucket < d.buckets; bucket += 1 {
		d.feedBucket(bucket)
	}
	d.resetNextBufferPositions()
	d.feed = false
}

func (d *DRUM) feedBucket(bucket int) {
	current := d.nextBufferPosisions[bucket]
	if current == 0 {
		return
	}

	kv, err := os.OpenFile(d.fileNames[bucket].Kv, os.O_WRONLY, 0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			panic(err)
		}
	}()

	aux, err := os.OpenFile(d.fileNames[bucket].Aux, os.O_WRONLY, 0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := aux.Close(); err != nil {
			panic(err)
		}
	}()

	kvBegin, err := kv.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	auxBegin, err := aux.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}

	if _, err := kv.Seek(d.currentPointers[bucket].Kv, io.SeekCurrent); err != nil {
		panic(err)
	}
	if _, err := aux.Seek(d.currentPointers[bucket].Aux, io.SeekCurrent); err != nil {
		panic(err)
	}

	for position := 0; position < current; position += 1 {
		e := d.kvBuffers[bucket][position]

		d.buf[0] = e.Op
		if _, err := kv.Write(d.buf[0:1]); err != nil {
			panic(err)
		}

		binary.BigEndian.PutUint64(d.buf[:], e.Key)
		if _, err := kv.Write(d.buf[:]); err != nil {
			panic(err)
		}

		binary.BigEndian.PutUint32(d.buf[:4], uint32(len(e.Value)))
		if _, err := kv.Write(d.buf[:4]); err != nil {
			panic(err)
		}
		if _, err := kv.Write(e.Value); err != nil {
			panic(err)
		}

		a := d.auxBuffers[bucket][position]
		binary.BigEndian.PutUint32(d.buf[:4], uint32(len(a)))
		if _, err := aux.Write(d.buf[:4]); err != nil {
			panic(err)
		}
		if _, err := aux.Write(a); err != nil {
			panic(err)
		}
	}

	d.currentPointers[bucket].Kv, err = kv.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	d.currentPointers[bucket].Aux, err = aux.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	if d.currentPointers[bucket].Kv-kvBegin > d.size || d.currentPointers[bucket].Aux-auxBegin > d.size {
		d.merge = true
	}
}

func (d *DRUM) checkTimeToMerge() {
	if d.merge {
		d.mergeBuckets()
	}
}

func Open(bucketsPath string, buckets, elements int, size int64, db DB, dispatcher Dispatcher) *DRUM {
	if math.Pow(math.Logb(float64(buckets)), 2) != float64(buckets) {
		panic("buckets count should be a pow of 2")
	}
	d := &DRUM{
		bucketsPath:         path.Clean(bucketsPath),
		dispatcher:          dispatcher,
		buckets:             buckets,
		shift:               uint64(64 - math.Ilogb(float64(buckets))),
		elements:            elements,
		size:                size,
		db:                  db,
		fileNames:           make([]names, buckets),
		currentPointers:     make([]pointers, buckets),
		nextBufferPosisions: make([]int, buckets),
	}
	d.auxBuffers, d.kvBuffers = make([][][]byte, buckets), make([][]*element, buckets)
	for i := range d.auxBuffers {
		d.auxBuffers[i], d.kvBuffers[i] = make([][]byte, elements), make([]*element, elements)
	}
	d.resetSynchronizationBuffers()
	d.assignFileNames()
	return d
}
