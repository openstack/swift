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

// This is the definition of the key-value interface.

package main

import (
	"bytes"
	"fmt"
)

// KV is the interface for operations that must be supported on the key-value store.
// the namespace is a single byte that is used as a key prefix for the different types of objects
// represented in the key-value; (volume, vfile..)
type KV interface {
	Get(namespace byte, key []byte) ([]byte, error)
	Put(namespace byte, key, value []byte) error
	PutSync(namespace byte, key, value []byte) error
	Delete(namespace byte, key []byte) error
	NewWriteBatch() WriteBatch
	NewIterator(namespace byte) Iterator
	Close()
}

// Iterator is the interface for operations that must be supported on the key-value iterator.
type Iterator interface {
	SeekToFirst()
	Seek(key []byte)
	Next()
	Key() []byte
	Value() []byte
	Valid() bool
	Close()
}

// WriteBatch is the interface for operations that must be supported on a "WriteBatch".
// The key-value used must support a write batch (atomic write of multiple entries)
type WriteBatch interface {
	// Put places a key-value pair into the WriteBatch for writing later.
	Put(namespace byte, key, value []byte)
	Delete(namespace byte, key []byte)

	// Commit the WriteBatch atomically
	Commit() error
	Close()
}

// Key for the state of the DB. If it has shut down cleanly, the value should be "closed"
const dbStateKey = "dbstate"
const closeState = "closed"
const openState = "opened"

// setKvState will be called on startup and check whether the kv was closed cleanly.
// It will then mark the db as "opened".
// This is needed because we write asynchronously to the key-value. After a crash/power loss/OOM kill, the db
// may be not in sync with the actual state of the volumes.
func setKvState(kv KV) (isClean bool, err error) {
	// Check if we stopped cleanly
	isClean, err = IsDbClean(kv)
	if err != nil {
		log.Warn("Could not check if DB is clean")
		return
	}

	if isClean {
		log.Info("DB is clean, set db state to open")
		err = MarkDbOpened(kv)
		if err != nil {
			log.Warn("Failed to mark db as opened when starting")
			return
		}
	}

	return
}

// IsDbClean will return true if the db has been previously closed properly.
// This is determined from a specific key in the database that should be set before closing.
func IsDbClean(kv KV) (isClean bool, err error) {
	value, err := kv.Get(statsPrefix, []byte(dbStateKey))
	if err != nil {
		log.Warn("failed to check kv state")
		return
	}

	// if the state is "closed", consider it clean
	// if the key is missing (new db) consider it dirty. It may have been deleted after a
	// corruption and we want to rebuild the DB with the existing volumes, not let the cluster
	// restart from scratch. If it's an actual new machine, the check will do nothing (no existing volumes)
	if bytes.Equal(value, []byte(closeState)) {
		isClean = true
	} else {
		log.Info(fmt.Sprintf("DB was not closed cleanly, state: %s", value))
	}
	return
}

// MarkDbClosed marks the DB as clean by setting the value of the db state key
func MarkDbClosed(kv KV) (err error) {
	err = kv.PutSync(statsPrefix, []byte(dbStateKey), []byte(closeState))
	return
}

// MarkDbOpened marks the DB as opened by setting the value of the db state key
func MarkDbOpened(kv KV) (err error) {
	err = kv.PutSync(statsPrefix, []byte(dbStateKey), []byte(openState))
	return
}
