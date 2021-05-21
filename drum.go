package drum

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
)

type DRUM struct {
	filesPath string

	dispatcher chan interface{}

	merge, feed bool

	buckets, elements int

	size int64

	auxBuffers          [][][]byte

	kvBuffers           [][]*element

	fileNames           []names

	currentPointers     []pointers

	nextBufferPosisions []int

	sortedMergeBuffer []*element
	unsortingHelper   []int
	unsortedAuxBuffer [][]byte

	buf [8]byte

	db DB
}

func (d *DRUM) Check(key uint64, aux []byte) {
	bucket, position := d.add(key, nil, Check)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *DRUM) Update(key uint64, value, aux []byte) {
	bucket, position := d.add(key, value, Update)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *DRUM) CheckAndUpdate(key uint64, value, aux []byte) {
	bucket, position := d.add(key, value, CheckUpdate)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *DRUM) Sync() {
	d.feedBuckets()
	d.mergeBuckets()
}

func (d *DRUM) getBucket(key uint64) int {
	return int((key >> (64 - uint64(math.Exp(float64(d.buckets))))) + uint64(d.buckets) / 2)
	// return int(key % uint64(d.buckets))
}

func (d *DRUM) readInfoBucketIntoMergeBuffer(bucket int) {
	kv, err := os.OpenFile(d.fileNames[bucket].Kv, os.O_RDWR, 0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			panic(err)
		}
	}()

	for {
		pos, err := kv.Seek(0, io.SeekCurrent)
		if err != nil {
			panic(err)
		}
		if pos >= d.currentPointers[bucket].Kv {
			break
		}

		d.sortedMergeBuffer = append(d.sortedMergeBuffer, new(element))
		e := d.sortedMergeBuffer[len(d.sortedMergeBuffer)-1]
		e.Position = len(d.sortedMergeBuffer) - 1

		if _, err := kv.Read(d.buf[0:1]); err != nil {
			panic(err)
		}
		e.Op = d.buf[0]

		if _, err := kv.Read(d.buf[:]); err != nil {
			panic(err)
		}
		e.Key = binary.BigEndian.Uint64(d.buf[:])

		if _, err := kv.Read(d.buf[:]); err != nil {
			panic(err)
		}
		e.Value = make([]byte, binary.BigEndian.Uint64(d.buf[:]))

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
		if Check == e.Op || CheckUpdate == e.Op {
			if !d.db.Has(e.Key) {
				e.Result = UniqueKey
			} else {
				e.Result = DuplicateKey
				if Check == e.Op {
					e.Value = d.db.Get(e.Key)
				}
			}
		}
		if Update == e.Op || CheckUpdate == e.Op {
			d.db.Put(e.Key, e.Value)
		}
	}
	d.db.Sync()
}

func (d *DRUM) unsortMergeBuffer() {
	if cap(d.unsortingHelper) >= len(d.sortedMergeBuffer) {
		d.unsortingHelper = d.unsortingHelper[:len(d.sortedMergeBuffer)]
	} else {
		d.unsortingHelper = append(d.unsortingHelper, make([]int, len(d.sortedMergeBuffer) - len(d.unsortingHelper))...)
	}
	for i := 0; i < len(d.sortedMergeBuffer); i += 1 {
		d.unsortingHelper[d.sortedMergeBuffer[i].Position] = i
	}
}

func (d *DRUM) readAuxBucketForDispatching(bucket int) {
	aux, err := os.OpenFile(d.fileNames[bucket].Aux, os.O_RDWR, 0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := aux.Close(); err != nil {
			panic(err)
		}
	}()

	for {
		pos, err := aux.Seek(0, io.SeekCurrent)
		if err != nil {
			panic(err)
		}
		if pos >= d.currentPointers[bucket].Aux {
			break
		}
		d.unsortedAuxBuffer = append(d.unsortedAuxBuffer, nil)
		if _, err := aux.Read(d.buf[:]); err != nil {
			panic(err)
		}
		size := binary.BigEndian.Uint64(d.buf[:])
		a := make([]byte, size)
		if _, err := aux.Read(a); err != nil {
			panic(err)
		}
		d.unsortedAuxBuffer[len(d.unsortedAuxBuffer)-1] = a
	}
}

func (d *DRUM) dispatch() {
	for i := 0; i < len(d.unsortingHelper); i += 1 {
		idx := d.unsortingHelper[i]
		e := d.sortedMergeBuffer[idx]
		aux := d.unsortedAuxBuffer[i]

		if Check == e.Op && UniqueKey == e.Result {
			d.dispatcher <- &UniqueKeyCheckEvent{
				Key: e.Key,
				Aux: aux,
			}
		} else if Check == e.Op && DuplicateKey == e.Result {
			d.dispatcher <- &DuplicateKeyCheckEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			}
		} else if CheckUpdate == e.Op && UniqueKey == e.Result {
			d.dispatcher <- &UniqueKeyUpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			}
		} else if CheckUpdate == e.Op && DuplicateKey == e.Result {
			d.dispatcher <- &DuplicateKeyUpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			}
		} else if Update == e.Op {
			d.dispatcher <- &UpdateEvent{
				Key:   e.Key,
				Value: e.Value,
				Aux:   aux,
			}
		} else {
			panic("not implemented")
		}
	}

}

func (d *DRUM) assignFileNames() {
	for bucket := 0; bucket < d.buckets; bucket += 1 {
		d.fileNames[bucket] = names{
			Kv: d.filesPath + "/" + strconv.Itoa(bucket) + "_bucket.kv",
			Aux: d.filesPath + "/" + strconv.Itoa(bucket) + "_bucket.aux",
		}
	}
}

func (d *DRUM) resetSynchronizationBuffers() {
	d.sortedMergeBuffer = make([]*element, 0, d.elements)
	d.unsortingHelper = make([]int, 0, d.elements)
	d.unsortedAuxBuffer = make([][]byte, 0, d.elements)
}

func (d *DRUM) resetFilePointers() {
	for bucket := 0; bucket < d.buckets; bucket += 1 {
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

func (d *DRUM) getBucketAndBufferPos(key uint64) (bucket, position int) {
	bucket = d.getBucket(key)
	position = d.nextBufferPosisions[bucket]
	d.nextBufferPosisions[bucket] += 1
	if d.nextBufferPosisions[bucket] == d.elements {
		d.feed = true
	}
	return
}

func (d *DRUM) add(key uint64, value []byte, op byte) (int, int) {
	bucket, position := d.getBucketAndBufferPos(key)
	d.kvBuffers[bucket][position] = &element{
		Key:   key,
		Value: value,
		Op:    op,
	}
	return bucket, position
}


func (d *DRUM) checkTimeToFeed() {
	if d.feed {
		d.feedBuckets()
	}
	d.checkTimeToMerge()
}

func (d *DRUM) resetNextBufferPositions() {
	for bucket := 0; bucket < d.buckets; bucket += 1 {
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

	kv, err := os.OpenFile(d.fileNames[bucket].Kv, os.O_RDWR, 0)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			panic(err)
		}
	}()

	aux, err := os.OpenFile(d.fileNames[bucket].Aux, os.O_RDWR, 0)
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
	_, err = kv.Seek(d.currentPointers[bucket].Kv, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	_, err = aux.Seek(d.currentPointers[bucket].Aux, io.SeekCurrent)
	if err != nil {
		panic(err)
	}

	for i := 0; i < current; i += 1 {
		e := d.kvBuffers[bucket][i]

		d.buf[0] = e.Op
		if _, err := kv.Write(d.buf[0:1]); err != nil {
			panic(err)
		}
		binary.BigEndian.PutUint64(d.buf[:], e.Key)
		if _, err := kv.Write(d.buf[:]); err != nil {
			panic(err)
		}
		binary.BigEndian.PutUint64(d.buf[:], uint64(len(e.Value)))
		if _, err := kv.Write(d.buf[:]); err != nil {
			panic(err)
		}
		if _, err := kv.Write(e.Value); err != nil {
			panic(err)
		}

		a := d.auxBuffers[bucket][i]
		binary.BigEndian.PutUint64(d.buf[:], uint64(len(a)))
		if _, err := aux.Write(d.buf[:]); err != nil {
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

func NewDrum(buckets int, elements int, size int64, db DB, dispatcher chan interface{}, filesPath string) *DRUM {
	auxBuffers := make([][][]byte, buckets)
	for i := range auxBuffers {
		auxBuffers[i] = make([][]byte, elements)
	}
	kvBuffers := make([][]*element, buckets)
	for i := range kvBuffers {
		kvBuffers[i] = make([]*element, elements)
	}
	d := &DRUM{
		filesPath:           path.Clean(filesPath),
		dispatcher:          dispatcher,
		buckets:             buckets,
		elements:            elements,
		size:                size,
		db:                  db,
		auxBuffers:          auxBuffers,
		kvBuffers:           kvBuffers,
		fileNames:           make([]names, buckets),
		currentPointers:     make([]pointers, buckets),
		nextBufferPosisions: make([]int, buckets),
	}
	d.resetSynchronizationBuffers()
	d.assignFileNames()
	return d
}
