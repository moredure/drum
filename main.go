package main

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

type Pair struct {
	X, Y int
}

type drum struct {
	merge  bool
	feed bool
	ch chan interface{}
	numBuckets int
	auxBuffers [][]string
	kvBuffers  [][]Compound
	fileNames  []string
	currentPointers []Pair
	nextBufferPosisions []int
}

func (d *drum) getBucketIdentififer(key uint64) int {
	return int(key % uint64(d.numBuckets))
}

func (d *drum) readInfoBucketIntoMergeBuffer(bucket int) {
	/*
	std::fstream kv_file;
	  this->OpenFile(file_names_[bucket_id].first, kv_file);
	  std::streampos kv_written = current_pointers_[bucket_id].first;

	  while (kv_file.tellg() < kv_written)
	  {
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
	  }

	  this->CloseFile(kv_file);
	 */
}
func (d *drum) sortMergeBuffer() {

}
func (d *drum) synchronizeWithDisk() {
	/*
	//Fast Berkeley DB queries are expected due to the locality of the keys in the bucket (and
	  //consequently in the merge buffer).

	  int ret;
	  for (typename std::vector<CompoundType>::size_type i = 0; i < sorted_merge_buffer_.size(); ++i)
	  {
	    CompoundType & element = sorted_merge_buffer_[i];

	    std::size_t key_size;
	    const char* key_serial;
	    KeyType const& key = boost::tuples::get<0>(element);
	    ElementIO<KeyType>::Serialize(key, key_size, key_serial);

	    Dbt db_key(const_cast<char*>(key_serial), key_size); //Berkeley DB key.

	    char op = boost::tuples::get<2>(element);
	    assert((op == CHECK || op == CHECK_UPDATE || op == UPDATE) && "Impossible operation.");

	    if (CHECK == op || CHECK_UPDATE == op)
	    {
	      if (DB_NOTFOUND == db_.exists(0, &db_key, 0))
	        boost::tuples::get<4>(element) = UNIQUE_KEY;
	      else
	      {
	        boost::tuples::get<4>(element) = DUPLICATE_KEY;

	        //Retrieve the value associated to the key only if it's a check operation.
	        if (CHECK == op)
	        {
	          //The size of the value associated to the key is unknown.
	          //Berkeley DB guide suggests to use a candidate size and if DB_BUFFER_SMALL is
	          //returned, perform a reallocation and try again. This is done here.
	          //I use an ad-hoc data size. Change it according to your needs.
	          std::vector<char> value_serial(db_default_data_size_);

	          Dbt db_value;
	          db_value.set_data(&value_serial[0]);
	          db_value.set_ulen(db_default_data_size_);
	          db_value.set_flags(DB_DBT_USERMEM);
	          if (DB_BUFFER_SMALL == db_.get(0, &db_key, &db_value, 0))
	          {
	            value_serial.resize(db_value.get_size());
	            db_value.set_data(&value_serial[0]);
	            db_value.set_ulen(db_value.get_size());
	            if (0 != (ret = db_.get(0, &db_key, &db_value, 0)))
	              throw DrumException("Error retrieving key/value from repository.", ret);
	          }

	          //Set the info.
	          ValueType & value = boost::tuples::get<1>(element);
	          ElementIO<ValueType>::Deserialize(value, db_value.get_size(), &value_serial[0]);
	        }
	      }
	    }

	    if (UPDATE == op || CHECK_UPDATE == op)
	    {
	      std::size_t value_size;
	      const char* value_serial;
	      ValueType const& value = boost::tuples::get<1>(element);
	      ElementIO<ValueType>::Serialize(value, value_size, value_serial);

	      Dbt db_value(const_cast<char*>(value_serial), value_size);

	      if (0 != (ret = db_.put(0, &db_key, &db_value, 0))) //Overwrite if the key is already present.
	        throw DrumException("Error merging with repository.", ret);
	    }
	  }

	  //Persist.
	  if (0 != (ret = db_.sync(0))) throw DrumException("Error persisting repository.");
	 */
}
func (d *drum) unsortMergeBuffer() {
	/*
	//When elements were read into the merge buffer their original positions were stored.
	  //Now I use those positions as keys to "sort" them back in linear time.
	  //Traversing unsorting_helper_ gives the indexes into sorted_merge_buffer considering the original
	  //order. (I use only the indexes to avoid element copies.)
	  std::size_t total = static_cast<std::size_t>(sorted_merge_buffer_.size());
	  unsorting_helper_.resize(total);
	  for (std::size_t i = 0; i < total; ++i)
	  {
	    CompoundType const& element = sorted_merge_buffer_[i];
	    std::size_t original = boost::tuples::get<3>(element);
	    unsorting_helper_[original] = i;
	  }
	 */
}
func (d *drum) readAuxBucketForDispatching(bucket int) {
	/*
	std::fstream aux_file;
	  this->OpenFile(file_names_[bucket_id].second, aux_file);
	  std::streampos aux_written = current_pointers_[bucket_id].second;

	  while (aux_file.tellg() < aux_written)
	  {
	    unsorted_aux_buffer_.push_back(AuxType());
	    AuxType & aux = unsorted_aux_buffer_.back();

	    std::size_t aux_size;
	    aux_file.read(reinterpret_cast<char*>(&aux_size), sizeof(std::size_t));
	    std::vector<char> aux_serial(aux_size);
	    aux_file.read(&aux_serial[0], aux_size);
	    ElementIO<AuxType>::Deserialize(aux, aux_size, &aux_serial[0]);
	  }

	  this->CloseFile(aux_file);
	 */
}

func (d *drum) dispatch() {
	/*
	std::size_t total = static_cast<std::size_t>(unsorting_helper_.size());
	  for (std::size_t i = 0; i < total; ++i)
	  {
	    std::size_t element_index = unsorting_helper_[i];
	    CompoundType const& element = sorted_merge_buffer_[element_index];
	    KeyType const& key = boost::tuples::get<0>(element);
	    char op = boost::tuples::get<2>(element);
	    char result = boost::tuples::get<4>(element);

	    AuxType const& aux = unsorted_aux_buffer_[i];

	    if (CHECK == op && UNIQUE_KEY == result)
	      dispatcher_.UniqueKeyCheck(key, aux);
	    else
	    {
	      ValueType const& value = boost::tuples::get<1>(element);

	      if (CHECK == op && DUPLICATE_KEY == result)
	        dispatcher_.DuplicateKeyCheck(key, value, aux);
	      else if (CHECK_UPDATE == op && UNIQUE_KEY == result)
	        dispatcher_.UniqueKeyUpdate(key, value, aux);
	      else if (CHECK_UPDATE == op && DUPLICATE_KEY == result)
	        dispatcher_.DuplicateKeyUpdate(key, value, aux);
	      else if (UPDATE == op)
	        dispatcher_.Update(key, value, aux);
	      else assert("Invalid combination of operation and result.");
	    }
	  }
	 */
}
func (d *drum) resetSynchronizationBuffers() {
/*
	sorted_merge_buffer_.clear();
     sorted_merge_buffer_.reserve(bucket_buff_elem_size_nt); //At least.
     unsorting_helper_.clear();
     unsorting_helper_.reserve(bucket_buff_elem_size_nt);
     unsorted_aux_buffer_.clear();
     unsorted_aux_buffer_.reserve(bucket_buff_elem_size_nt);
 */
}
func (d *drum) resetFilePointers() {

}

func (d *drum) mergeBuckets() {
	for bucket := 0; bucket < d.numBuckets; bucket++ {
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

func (d *drum) getBucketAndBufferPos (key uint64) (bucket int, position int) {
	/*
	std::size_t bucket_id = BucketIdentififer<key_t, num_buckets_nt>::Calculate(key);
	  std::size_t pos = next_positions_[bucket_id]++; //Notice this increments the position.

	  assert(pos <= bucket_buff_elem_size_nt && "Position must not be larger than buffer size.");

	  if (next_positions_[bucket_id] == bucket_buff_elem_size_nt)
	    feed_buckets_ = true;

	  return std::make_pair(bucket_id, pos);
	 */
	return 0, 0
}

func (d *drum) add(key uint64, value, aux string, op int) int {
	/*
	std::pair<std::size_t, std::size_t> bucket_pos = this->GetBucketAndBufferPos(key);
	  CompoundType & element = kv_buffers_[bucket_pos.first][bucket_pos.second];
	  this->Make(element, key, value, op);
	  return bucket_pos;
	 */
}

func (d *drum) Check(key uint64) {
	/*
	std::pair<std::size_t, std::size_t> bucket_pos = this->Add(key, CHECK);
	  aux_buffers_[bucket_pos.first][bucket_pos.second] = aux;
	  this->CheckTimeToFeed();
	 */
}

func (d *drum) Update(key uint64, value, aux string) {
	/*
	std::pair<std::size_t, std::size_t> bucket_pos = this->Add(key, value, UPDATE);
	  aux_buffers_[bucket_pos.first][bucket_pos.second] = aux;
	  this->CheckTimeToFeed();
	 */
}

func (d *drum) CheckAndUpdate(key uint64, value, aux string) {
	/*
	 std::pair<std::size_t, std::size_t> bucket_pos = this->Add(key, value, CHECK_UPDATE);
	  aux_buffers_[bucket_pos.first][bucket_pos.second] = aux;
	  this->CheckTimeToFeed();
	 */
}

func (d *drum) checkTimeToFeed() {
	//if (feed_buckets_) this->FeedBuckets();
	//
	//this->CheckTimeToMerge();
}
func (d *drum) feedBuckets() {
	/*
	 for (std::size_t bucket_id = 0; bucket_id < num_buckets_nt; ++bucket_id)
	    this->FeedBucket(bucket_id);

	  this->ResetNextBufferPositions();
	  feed_buckets_ = false;
	 */
}
func (d *drum) feedBucket(bucket int) {
	/*
	 //Positions in each aux buffer is controlled by position in the corresponding info buffer.
	  std::size_t current = next_positions_[bucket_id];
	  if (0 == current) return;

	  std::fstream kv_file;
	  this->OpenFile(file_names_[bucket_id].first, kv_file);
	  std::streampos kv_begin = kv_file.tellp();

	  std::fstream aux_file;
	  this->OpenFile(file_names_[bucket_id].second, aux_file);
	  std::streampos aux_begin = aux_file.tellp();

	  //Set current pointers of the files.
	  kv_file.seekp(current_pointers_[bucket_id].first);
	  aux_file.seekp(current_pointers_[bucket_id].second);

	  for (std::size_t i = 0; i < current; ++i)
	  {
	    //Write the following sequentially:
	    // - operation;
	    // - key length;
	    // - key;
	    // - value length;
	    // - value.
	    CompoundType const& element = kv_buffers_[bucket_id][i];

	    //Operation.
	    char op = boost::tuples::get<2>(element);
	    kv_file.write(&op, sizeof(char));

	    //Key.
	    std::size_t key_size;
	    const char* key_serial;
	    KeyType const& key = boost::tuples::get<0>(element);
	    ElementIO<KeyType>::Serialize(key, key_size, key_serial);
	    kv_file.write(reinterpret_cast<const char*>(&key_size), sizeof(std::size_t));
	    kv_file.write(key_serial, key_size);

	    //Value.
	    std::size_t value_size;
	    const char* value_serial;
	    ValueType const& value = boost::tuples::get<1>(element);
	    ElementIO<ValueType>::Serialize(value, value_size, value_serial);
	    kv_file.write(reinterpret_cast<const char*>(&value_size), sizeof(std::size_t));
	    kv_file.write(value_serial, value_size);

	    //Write the following sequentially:
	    // - aux length;
	    // - aux.
	    AuxType const& aux = aux_buffers_[bucket_id][i];

	    std::size_t aux_size;
	    const char* aux_serial;
	    ElementIO<AuxType>::Serialize(aux, aux_size, aux_serial);
	    aux_file.write(reinterpret_cast<const char*>(&aux_size), sizeof(std::size_t));
	    aux_file.write(aux_serial, aux_size);
	  }

	  //Store pointers for the next feed.
	  current_pointers_[bucket_id].first = kv_file.tellp();
	  current_pointers_[bucket_id].second = aux_file.tellp();

	  this->CloseFile(kv_file);
	  this->CloseFile(aux_file);

	  //Is it time to merge?
	  if (current_pointers_[bucket_id].first - kv_begin > bucket_byte_size_nt ||
	      current_pointers_[bucket_id].second - aux_begin > bucket_byte_size_nt)
	  {
	    merge_buckets_ = true;
	  }
	*/
}

func (d *drum) checkTimeToMerge() {
//	if (merge_buckets_) this->MergeBuckets();
}

func NewDrum(buckets int, buf int, size int) DRUM {
	return &drum{}
}

func main() {
}
