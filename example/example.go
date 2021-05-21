package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/moredure/drum"
	"strconv"
)

func main() {
	pdb, err := pebble.Open("/tmp/database", nil)
	if err != nil {
		panic(err)
	}
	defer pdb.Close()
	db := &db{
		db: pdb,
	}
	dispatcher := make(chan interface{}, 16)
	dr := drum.NewDrum(2, 8, 1024, db, dispatcher, "/tmp/buckets")
	go func() {
		for i := 0; i < 16; i += 1 {
			dr.CheckAndUpdate(uint64(i), []byte(strconv.Itoa(i)), nil)
		}
		dr.Sync()
	}()
	for message := range dispatcher {
		switch _ := message.(type) {
		case *drum.DuplicateKeyCheckEvent:
			fmt.Println("DuplicateKeyCheckEvent")
		case *drum.DuplicateKeyUpdateEvent:
			fmt.Println("DuplicateKeyUpdateEvent")
		case *drum.UniqueKeyCheckEvent:
			fmt.Println("UniqueKeyCheckEvent")
		case *drum.UniqueKeyUpdateEvent:
			fmt.Println("UniqueKeyUpdateEvent")
		case *drum.UpdateEvent:
			fmt.Println("UpdateEvent")
		default:
			panic("not implemented")
		}
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
	closer.Close()
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
	d.db.Flush()
}
