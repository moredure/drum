package drum

import (
	"encoding/binary"
	"io"
	"os"
	"sort"
)

type DRUM interface {
	Check(key uint64)
	Update(key uint64, value, aux string)
	CheckAndUpdate(key uint64, value, aux string)
}

type Event struct {
	Key uint64
	Value, Aux string
}

type Update Event
type DuplicateKeyUpdate Event
type UniqueKeyUpdate Event
type DuplicateKeyCheck Event
type UniqueKeyCheck Event

type Compound struct {
	Key uint64
	Value string
	Op byte
	Position int
	Result byte
}

type drum struct {
	merge  bool
	feed bool
	ch chan interface{}
	numBuckets int
	bucketBufferElementsSize int
	auxBuffers [][]string
	kvBuffers  [][]*Compound
	fileNames  [][2]string
	currentPointers [][2]int64
	bucketByteSize int64
	nextBufferPosisions []int

	db DB

	sortedMergeBuffer []*Compound
	unsortingHelper []int
	unsortedAuxBuffer []string
}

type DB interface {
	Has(uint64) bool
	Put(uint64, string) bool
	Get(uint64) string
	Sync()
}

func (d *drum) getBucketIdentififer(key uint64) int {
	return int(key % uint64(d.numBuckets))
}

func (d *drum) readInfoBucketIntoMergeBuffer(bucket int) {
	kv, err := os.Open(d.fileNames[bucket][0])
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	written := d.currentPointers[bucket][0]

	for pos, _ := kv.Seek(0, io.SeekCurrent); pos < written; pos, _ = kv.Seek(0, io.SeekCurrent) {
		/*
			//It would be nice to have semantics to move the element into the container. To avoid an
			//extra copy I insert the element and use a reference to it.
			sorted_merge_buffer_.push_back(CompoundType());
			CompoundType & element = sorted_merge_buffer_.back();
	
			//Keep track of the element's original order in file.
			std::size_t & order = boost::tuples::get<3>(element);
			order = sorted_merge_buffer_.size() - 1;
	
			//Operation.
			char & op = boost::tuples::get<2>(element);
			kv_file.read(&op, sizeof(char));
	
			//Key.
			KeyType & key = boost::tuples::get<0>(element);
			std::size_t key_size;
			kv_file.read(reinterpret_cast<char*>(&key_size), sizeof(std::size_t));
			std::vector<char> key_serial(key_size);
			kv_file.read(&key_serial[0], key_size);
			ElementIO<KeyType>::Deserialize(key, key_size, &key_serial[0]);
	
			//Value.
			ValueType & value = boost::tuples::get<1>(element);
			std::size_t value_size;
			kv_file.read(reinterpret_cast<char*>(&value_size), sizeof(std::size_t));
			std::vector<char> value_serial(value_size);
			kv_file.read(&value_serial[0], value_size);
			ElementIO<ValueType>::Deserialize(value, value_size, &value_serial[0]);
		 */
	}
}

func (d *drum) sortMergeBuffer() {
	sort.Slice(d.sortedMergeBuffer, func(i, j int) bool {
		return d.sortedMergeBuffer[i].Key < d.sortedMergeBuffer[j].Key
	})
}

func (d *drum) synchronizeWithDisk() {
	for _, element := range d.sortedMergeBuffer {
		if CHECK == element.Op || CHECK_UPDATE == element.Op {
	      if !d.db.Has(element.Key) {
			  element.Result = UNIQUE_KEY
		  } else {
			element.Result = DUPLICATE_KEY
	        if CHECK == element.Op {
			  element.Value = d.db.Get(element.Key)
	        }
	      }
	    }
	    if UPDATE == element.Op || CHECK_UPDATE == element.Op {
			d.db.Put(element.Key, element.Value)
	    }
	}
	d.db.Sync()
}

func (d *drum) unsortMergeBuffer() {
	d.unsortingHelper = d.unsortingHelper[:len(d.sortedMergeBuffer)] // or append
	for i := 0; i < len(d.sortedMergeBuffer); i+=1 {
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
		d.unsortedAuxBuffer = append(d.unsortedAuxBuffer, "") // push back
		buf := make([]byte, 1) // read size of next aux file
		aux.Read(buf)
		serial := make([]byte, buf[0])
		aux.Read(serial)
		d.unsortedAuxBuffer[len(d.unsortedAuxBuffer)-1] = string(serial)
	}
}
const (
	UNIQUE_KEY byte = iota
	DUPLICATE_KEY byte = iota
)
func (d *drum) dispatch() {
	for i := 0; i < len(d.unsortingHelper); i += 1 {
		idx := d.unsortingHelper[i]
		e := d.sortedMergeBuffer[idx]
		aux := d.unsortedAuxBuffer[i]

		if CHECK == e.Op && UNIQUE_KEY == e.Result {
			d.ch <- UniqueKeyCheck{
				Key: e.Key,
				Aux: aux,
			}
		} else if CHECK == e.Op && DUPLICATE_KEY == e.Result {
			d.ch <- DuplicateKeyCheck{
				Key: e.Key,
				Value: e.Value,
				Aux: aux,
			}
		} else if CHECK_UPDATE == e.Op && UNIQUE_KEY == e.Result {
			d.ch <- UniqueKeyUpdate{
				Key: e.Key,
				Value: e.Value,
				Aux: aux,
			}
		} else if CHECK_UPDATE == e.Op && DUPLICATE_KEY == e.Result {
			d.ch <- DuplicateKeyUpdate{
				Key: e.Key,
				Value: e.Value,
				Aux: aux,
			}
		} else if UPDATE == e.Op {
			d.ch <- Update{
				Key: e.Key,
				Value: e.Value,
				Aux: aux,
			}
		} else {
			panic("not implemented")
		}
	}

}

func (d *drum) resetSynchronizationBuffers() {
	d.sortedMergeBuffer = make([]*Compound, 0, d.bucketBufferElementsSize)
	d.unsortingHelper = make([]int, 0, d.bucketBufferElementsSize)
	d.unsortedAuxBuffer = make([]string, 0, d.bucketBufferElementsSize)
}

func (d *drum) resetFilePointers() {
	for bucket := 0; bucket < d.numBuckets; bucket += 1 {
		d.currentPointers[bucket] = [2]int64{0, 0}
	}
}

func (d *drum) mergeBuckets() {
	for bucket := 0; bucket < d.numBuckets; bucket += 1 {
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

func (d *drum) getBucketAndBufferPos (key uint64) (int, int) {
	bucket := d.getBucketIdentififer(key)
	d.nextBufferPosisions[bucket] += 1

	if d.nextBufferPosisions[bucket] == d.bucketBufferElementsSize {
		d.feed = true
	}

	return bucket, d.nextBufferPosisions[bucket]
}

func (d *drum) add(key uint64, value string, op byte) (int, int) {
	bucket, position := d.getBucketAndBufferPos(key)
	d.kvBuffers[bucket][position] = &Compound{
		Key: key,
		Value: value,
		Op: op,
	}
	return bucket, position
}

const (
	CHECK byte = iota
	UPDATE
	CHECK_UPDATE
)

func (d *drum) Check(key uint64, aux string) {
	bucket, position := d.add(key, "", CHECK)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *drum) Update(key uint64, value, aux string) {
	bucket, position := d.add(key, value, UPDATE)
	d.auxBuffers[bucket][position] = aux
	d.checkTimeToFeed()
}

func (d *drum) CheckAndUpdate(key uint64, value, aux string) {
	bucket, position := d.add(key, value, CHECK_UPDATE)
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
	for bucket := 0; bucket < d.numBuckets; bucket += 1 {
		d.nextBufferPosisions[bucket] = 0
	}
}

func (d *drum) feedBuckets() {
	for bucket := 0; bucket < d.numBuckets; bucket += 1 {
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
	pos1, _ := kv.Seek(d.currentPointers[bucket][0], io.SeekCurrent)
	pos2, _ := aux.Seek(d.currentPointers[bucket][1], io.SeekCurrent)
	for i := 0; i < current; i += 1 {
		element := d.kvBuffers[bucket][i]
		
		b := make([]byte, 1)
		b[0] = element.Op
		kv.Write(b)

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, element.Key)
		kv.Write(key)

		b[0] = byte(len(element.Value))
		kv.Write(b)
		kv.WriteString(element.Value)

		a := d.auxBuffers[bucket][i]
		b[0] = byte(len(element.Value))
		aux.Write(b)
		aux.WriteString(a)
	}

	d.currentPointers[bucket][0], err = kv.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	d.currentPointers[bucket][1], err = aux.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}

	if d.currentPointers[bucket][0] - kvBegin > d.bucketByteSize || d.currentPointers[bucket][1] - auxBegin > d.bucketByteSize {
		d.merge = true
	}
}

func (d *drum) checkTimeToMerge() {
	if d.merge {
		d.mergeBuckets()
	}
}

func NewDrum(buckets int, buf int, size int) DRUM {
	return nil
}
