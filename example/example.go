package main

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/moredure/drum"
)

func main() {
	pdb, err := pebble.Open("/tmp/abc1233", &pebble.Options{})
	if err != nil {
		panic(err)
	}
	defer pdb.Close()
	db := &db{
		db: pdb,
	}
	dispatcher := make(chan interface{})
	dr := drum.NewDrum(4, 8, 1024, db, dispatcher)
	go func() {
		dr.CheckAndUpdate(1, "", "")
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

func (d *db) Put(u uint64, s string) {
	binary.BigEndian.PutUint64(d.key[:], u)
	err := d.db.Set(d.key[:], []byte(s), pebble.NoSync)
	if err != nil {
		panic(err)
	}
	return
}

func (d *db) Get(u uint64) string {
	binary.BigEndian.PutUint64(d.key[:], u)
	us, closer, err := d.db.Get(d.key[:])
	if err == pebble.ErrNotFound {
		return ""
	}
	if err != nil {
		panic(err)
	}
	defer closer.Close()
	return string(us)
}

func (d *db) Sync() {
	d.db.Flush()
}
