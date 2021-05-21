package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/moredure/drum"
)

func main() {
	pdb, err := pebble.Open("/tmp/database", &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer pdb.Close()
	db := &db{
		db: pdb,
	}
	dispatcher := make(chan interface{})
	dr := drum.NewDrum(4, 8, 1024, db, dispatcher, "/tmp/buckets")
	go func() {
		dr.CheckAndUpdate(1, nil, nil)
		dr.Sync()
	}()
	for k := range dispatcher {
		fmt.Println(k)
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
	defer closer.Close()
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
