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

// This implements the KV interface using levigo, which is a golang wrapper around the leveldb C++ library.

package main

import (
	"github.com/jmhodges/levigo"
)

// levigoDB holds the leveldb handle and options
type levigoDB struct {
	db *levigo.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
}

// levigoIterator wraps a levelDB iterator. The namespace byte is used to specify which type of
// entry (volume, vfile..) it will iterate on.
type levigoIterator struct {
	it        *levigo.Iterator
	namespace byte
}

// levigoWriteBatch wraps a levigoDB WriteBatch
type levigoWriteBatch struct {
	wb  *levigo.WriteBatch
	ldb *levigoDB
}

// openLevigoDB Opens or create the DB.
// (shoult use an interface?)
func openLevigoDB(path string) (*levigoDB, error) {

	opts := levigo.NewOptions()
	// filter := levigo.NewBloomFilter(10)
	// opts.SetFilterPolicy(filter)
	// That may be useless, since we're supposed to fit in memory ? 10MB for now
	opts.SetCache(levigo.NewLRUCache(10 * 1048576))
	opts.SetCreateIfMissing(true)

	// This will open or create the DB. A new DB is not marked as clean. It
	// may have been lost or deleted, while there is data in volumes on-disk.
	// A new DB will have to be checked and marked as clean.
	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}

	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()

	ldb := levigoDB{db, ro, wo}

	return &ldb, nil
}

// Key value operations
//
// All operations take a namespace byte to denote the type of object the entry refers to.
// Get wraps levigoDB Get
func (ldb *levigoDB) Get(namespace byte, key []byte) (value []byte, err error) {
	db := ldb.db
	ro := ldb.ro

	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	value, err = db.Get(ro, buf)
	return
}

// Put wraps levigoDB Put
func (ldb *levigoDB) Put(namespace byte, key, value []byte) error {
	db := ldb.db
	wo := ldb.wo

	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	return db.Put(wo, buf, value)
}

// PutSync will write an entry with the "Sync" option set
func (ldb *levigoDB) PutSync(namespace byte, key, value []byte) error {
	db := ldb.db
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)

	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	return db.Put(wo, buf, value)
}

// Close wraps levigoDB Close
func (ldb *levigoDB) Close() {
	ldb.db.Close()
}

// Delete wraps levigoDB Delete
func (ldb *levigoDB) Delete(namespace byte, key []byte) error {
	db := ldb.db
	wo := ldb.wo

	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	return db.Delete(wo, buf)
}

// NewWriteBatch creates a new WriteBatch
func (ldb *levigoDB) NewWriteBatch() WriteBatch {
	lwb := &levigoWriteBatch{}
	lwb.wb = levigo.NewWriteBatch()
	lwb.ldb = ldb
	return lwb
}

// Put on a WriteBatch
func (lwb *levigoWriteBatch) Put(namespace byte, key, value []byte) {
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	lwb.wb.Put(buf, value)
	return
}

// Delete on a WriteBatch
func (lwb *levigoWriteBatch) Delete(namespace byte, key []byte) {
	buf := make([]byte, len(key)+1)
	buf[0] = namespace
	copy(buf[1:], key)

	lwb.wb.Delete(buf)
	return
}

// Commit a WriteBatch
func (lwb *levigoWriteBatch) Commit() (err error) {
	db := lwb.ldb.db
	wo := lwb.ldb.wo
	wb := lwb.wb

	err = db.Write(wo, wb)

	return
}

// Close a WriteBatch
func (lwb *levigoWriteBatch) Close() {
	wb := lwb.wb

	wb.Close()
}

// Iterator functions
//
// NewIterator creates a new iterator for the given object type (namespace)
func (ldb *levigoDB) NewIterator(namespace byte) Iterator {
	lit := &levigoIterator{}
	lit.it = ldb.db.NewIterator(ldb.ro)
	lit.namespace = namespace
	return lit
}

// SeekToFirst will seek to the first object of the given type
func (lit *levigoIterator) SeekToFirst() {
	// The "first key" is the first one in the iterator's namespace
	buf := make([]byte, 1)
	buf[0] = lit.namespace

	lit.it.Seek(buf)
	return
}

// Seek moves the iterator to the position of the key
func (lit *levigoIterator) Seek(key []byte) {
	// Prefix the key with a single byte (namespace)
	buf := make([]byte, len(key)+1)
	buf[0] = lit.namespace
	copy(buf[1:], key)

	lit.it.Seek(buf)
	return
}

// Next moves the iterator to the next key
func (lit *levigoIterator) Next() {
	lit.it.Next()
	return
}

// Key returns the key (without the leading namespace byte)
func (lit *levigoIterator) Key() (key []byte) {
	return lit.it.Key()[1:]
}

// Value returns the value at the current iterator position
func (lit *levigoIterator) Value() (key []byte) {
	return lit.it.Value()
}

// Valid returns false if we are past the first or last key in the key-value
func (lit *levigoIterator) Valid() bool {
	if lit.it.Valid() && lit.it.Key()[0] == lit.namespace {
		return true
	} else {
		return false
	}
}

// Close the iterator
func (lit *levigoIterator) Close() {
	lit.it.Close()
	return
}
