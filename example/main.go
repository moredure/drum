//go:generate bash init.sh

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/moredure/drum"
	"strconv"
)

const buckets = 8

const size = 1024 * 1024

const elements = 32 * 1024

func main() {
	pdb, err := pebble.Open("/tmp/database", nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := pdb.Close(); err != nil {
			panic(err)
		}
	}()
	db := &pebbleDB{
		db: pdb,
	}
	counter := 0
	dispatcherFunc := drum.DispatcherFunc(func(i interface{}) { // or use dispatcher type
		switch i.(type) {
		case *drum.UniqueKeyCheckEvent:
		case *drum.DuplicateKeyCheckEvent:
		case *drum.UniqueKeyUpdateEvent:
			counter += 1
		case *drum.DuplicateKeyUpdateEvent:
		case *drum.UpdateEvent:
		}
	})
	dr := drum.Open("/tmp/buckets", buckets, elements, size, db, dispatcherFunc)
	for i := 0; i < 100; i += 1 {
		dr.CheckUpdate(uint64(i), []byte(strconv.Itoa(i)), nil)
	}
	dr.Sync()
	fmt.Println(counter == 100)
}

type pebbleDB struct {
	key [8]byte
	db  *pebble.DB
}

func (d *pebbleDB) Has(u uint64) bool {
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

func (d *pebbleDB) Put(u uint64, s []byte) {
	binary.BigEndian.PutUint64(d.key[:], u)
	err := d.db.Set(d.key[:], s, pebble.NoSync)
	if err != nil {
		panic(err)
	}
	return
}

func (d *pebbleDB) Get(u uint64) []byte {
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

func (d *pebbleDB) Sync() {
	if err := d.db.Flush(); err != nil {
		panic(err)
	}
}

type dispatcher struct {
}

func (d dispatcher) UniqueKeyCheckEvent(event *drum.UniqueKeyCheckEvent) {
	fmt.Println("UniqueKeyCheckEvent")
}

func (d dispatcher) DuplicateKeyCheckEvent(event *drum.DuplicateKeyCheckEvent) {
	fmt.Println("DuplicateKeyCheckEvent")
}

func (d dispatcher) UniqueKeyUpdateEvent(event *drum.UniqueKeyUpdateEvent) {
	fmt.Println("UniqueKeyUpdateEvent")
}

func (d dispatcher) DuplicateKeyUpdateEvent(event *drum.DuplicateKeyUpdateEvent) {
	fmt.Println("DuplicateKeyUpdateEvent")
}

func (d dispatcher) UpdateEvent(event *drum.UpdateEvent) {
	fmt.Println("UpdateEvent")
}
