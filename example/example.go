package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/moredure/drum"
	"strconv"
)

type dispatcher struct {
}

func (d dispatcher) UniqueKeyCheckEvent(event *drum.Event) {
	fmt.Println(event.Key)
}

func (d dispatcher) DuplicateKeyCheckEvent(event *drum.Event) {
	fmt.Println(event.Key)
}

func (d dispatcher) UniqueKeyUpdateEvent(event *drum.Event) {
	fmt.Println(event.Key)
}

func (d dispatcher) DuplicateKeyUpdateEvent(event *drum.Event) {
	fmt.Println(event.Key)
}

func (d dispatcher) UpdateEvent(event *drum.Event) {
	fmt.Println(event.Key)
}

func main() {
	pdb, err := pebble.Open("/tmp/database", nil)
	if err != nil {
		panic(err)
	}
	defer pdb.Close()
	db := &db{
		db: pdb,
	}
	dr := drum.NewDrum(8, 32 * 1024, 1024 * 1024, db, new(dispatcher), "/tmp/buckets")

	for i := 0; i < 1000000; i += 1 {
		dr.CheckUpdate(uint64(i), []byte(strconv.Itoa(i)), nil)
	}
}

type db struct {
	key [8]byte
	db  *pebble.DB
}

func (d *db) Has(u uint64) bool {
	binary.BigEndian.PutUint64(d.key[:], u)
	_, closer, err := d.db.Get(d.key[:])
	if err == pebble.ErrNotFound {
		return false
	}
	if err != nil {
		panic(err)
	}
	if err := closer.Close(); err != nil {
		panic(err)
	}
	return true
}

func (d *db) Put(u uint64, s []byte) {
	binary.BigEndian.PutUint64(d.key[:], u)
	err := d.db.Set(d.key[:], s, pebble.NoSync)
	if err != nil {
		panic(err)
	}
	return
}

func (d *db) Get(u uint64) []byte {
	binary.BigEndian.PutUint64(d.key[:], u)
	us, closer, err := d.db.Get(d.key[:])
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		panic(err)
	}
	defer closer.Close()
	return bytes.Repeat(us, 1)
}

func (d *db) Sync() {
	if err := d.db.Flush(); err != nil {
		panic(err)
	}
}
