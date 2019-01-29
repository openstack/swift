// Copyright (c) 2010-2012 OpenStack Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This implements the KV interface using goleveldb, a native golang leveldb package.
// Its behavior has been adapted to match the levigo behavior.

package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type goLevelDB struct {
	db *leveldb.DB
	ro *opt.ReadOptions
	wo *opt.WriteOptions
}

type levelDBIterator struct {
	it        iterator.Iterator
	namespace byte
}

type levelDBWriteBatch struct {
	wb  *leveldb.Batch
	ldb *goLevelDB
}

// openGoLevelDB Opens or create the DB.
// (should use an interface?)
func openGoLevelDb(path string) (*goLevelDB, error) {

	// TODO check options
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	ro := &opt.ReadOptions{}
	wo := &opt.WriteOptions{}

	ldb := goLevelDB{db, ro, wo}

	return &ldb, nil
}

// Key value operations
//
// All operations take a namespace byte to denote the type of object the entry refers to.
func (ldb *goLevelDB) Get(namespace byte, key []byte) (value []byte, err error) {
	db := ldb.db
	ro := ldb.ro

	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	value, err = db.Get(buf, ro)
	// Behave similarly to levigo
	if err == leveldb.ErrNotFound {
		value = nil
		err = nil
	}
	return
}

func (ldb *goLevelDB) Put(namespace byte, key, value []byte) error {
	db := ldb.db
	wo := ldb.wo

	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	return db.Put(buf, value, wo)
}

// PutSync will write an entry with the "Sync" option set
func (ldb *goLevelDB) PutSync(namespace byte, key, value []byte) error {
	db := ldb.db
	wo := &opt.WriteOptions{Sync: true}

	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	return db.Put(buf, value, wo)
}

func (ldb *goLevelDB) Close() {
	ldb.db.Close()
}

func (ldb *goLevelDB) Delete(namespace byte, key []byte) error {
	db := ldb.db
	wo := ldb.wo

	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	return db.Delete(buf, wo)
}

func (ldb *goLevelDB) NewWriteBatch() WriteBatch {
	lwb := &levelDBWriteBatch{}
	lwb.wb = new(leveldb.Batch)
	lwb.ldb = ldb
	return lwb
}

// Put on a WriteBatch
func (lwb *levelDBWriteBatch) Put(namespace byte, key, value []byte) {
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	lwb.wb.Put(buf, value)
	return
}

// Delete on a WriteBatch
func (lwb *levelDBWriteBatch) Delete(namespace byte, key []byte) {
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	lwb.wb.Delete(buf)
	return
}

// Commit a WriteBatch
func (lwb *levelDBWriteBatch) Commit() (err error) {
	db := lwb.ldb.db
	wo := lwb.ldb.wo
	wb := lwb.wb

	err = db.Write(wb, wo)

	return
}

// Close a WriteBatch
func (lwb *levelDBWriteBatch) Close() {
	// TODO: check if there really is nothing to do
}

// Iterator functions
//
// NewIterator creates a new iterator for the given object type (namespace)
func (ldb *goLevelDB) NewIterator(namespace byte) Iterator {
	db := ldb.db
	ro := ldb.ro

	// Could use the "range" thing in this library
	lit := &levelDBIterator{}
	lit.it = db.NewIterator(nil, ro)
	lit.namespace = namespace
	return lit
}

// SeekToFirst will seek to the first object of the given type
func (lit *levelDBIterator) SeekToFirst() {
	// The "first key" is the first one in the iterator's namespace
	buf := make([]byte, 1)
	buf[0] = lit.namespace

	lit.it.Seek(buf)
	return
}

// Seek moves the iterator to the position of the key
func (lit *levelDBIterator) Seek(key []byte) {
	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = lit.namespace
	copy(buf[1:], key)

	lit.it.Seek(buf)
	return
}

// Next moves the iterator to the next key
func (lit *levelDBIterator) Next() {
	lit.it.Next()
	return
}

// Key returns the key (without the leading namespace byte)
func (lit *levelDBIterator) Key() (key []byte) {
	return lit.it.Key()[1:]
}

// Value returns the value at the current iterator position
func (lit *levelDBIterator) Value() (key []byte) {
	return lit.it.Value()
}

// Valid returns false if we are past the first or last key in the key-value
func (lit *levelDBIterator) Valid() bool {
	if lit.it.Key() != nil && lit.it.Key()[0] == lit.namespace {
		return true
	} else {
		return false
	}
}

// Close the iterator
func (lit *levelDBIterator) Close() {
	lit.it.Release()
	return
}
