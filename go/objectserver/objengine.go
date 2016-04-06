//  Copyright (c) 2015 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package objectserver

import (
	"errors"
	"flag"
	"io"
	"net/http"

	"github.com/openstack/swift/go/hummingbird"
)

// DriveFullError can be returned by Object.SetData and Object.Delete if the disk is too full for the operation.
var DriveFullError = errors.New("Drive Full")

type Object interface {
	// Exists determines whether or not there is an object to serve. Deleted objects do not exist, even if there is a tombstone.
	Exists() bool
	// Quarantine removes the file's data, presumably after determining it's been corrupted.
	Quarantine() error
	// Metadata returns the object's metadata.  Will be nil if the object doesn't exist.
	Metadata() map[string]string
	// ContentLength returns the object's content-length.
	ContentLength() int64
	// CopyRange copies a range of data from the object to the writer.
	CopyRange(io.Writer, int64, int64) (int64, error)
	// Copy copies an object's entire contents to the writer(s).
	Copy(...io.Writer) (int64, error)
	// SetData sets the data for the object, given the size (if known).  It returns a writer and an error if any.
	SetData(size int64) (io.Writer, error)
	// Commit saves a new object data that was started with SetData.
	Commit(metadata map[string]string) error
	// Delete deletes the object.
	Delete(metadata map[string]string) error
	// Close releases any resources held by the Object instance.
	Close() error
	// Repr returns a representation of the object, used for logging.
	Repr() string
}

// ObjectEngine is the type you have to give hummingbird to create a new object engine.
type ObjectEngine interface {
	// New creates a new instance of the Object, for interacting with a single object.
	New(vars map[string]string, needData bool) (Object, error)
	// RegisterHandlers registers custom routes needed by this object engine.  Examples are replication handlers.  Be careful of collisions.
	RegisterHandlers(AddRoute func(method, path string, handler http.HandlerFunc))
}

// ObjectEngineConstructor> is a function that, given configs and flags, returns an ObjectEngine
type ObjectEngineConstructor func(hummingbird.IniFile, *flag.FlagSet) (ObjectEngine, error)

type engineFactoryEntry struct {
	name        string
	constructor ObjectEngineConstructor
}

var engineFactories = []engineFactoryEntry{}

// RegisterObjectEngine lets you tell hummingbird about a new object engine.
func RegisterObjectEngine(name string, newEngine ObjectEngineConstructor) {
	for _, e := range engineFactories {
		if e.name == name {
			e.constructor = newEngine
			return
		}
	}
	engineFactories = append(engineFactories, engineFactoryEntry{name, newEngine})
}

// FindEngine returns the registered object engine with the given name.
func FindEngine(name string) (ObjectEngineConstructor, error) {
	for _, e := range engineFactories {
		if e.name == name {
			return e.constructor, nil
		}
	}
	return nil, errors.New("Not found")
}
