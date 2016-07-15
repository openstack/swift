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

package probe

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openstack/swift/go/hummingbird"
)

func TestReplicationHandoff(t *testing.T) {
	e := NewEnvironment()
	defer e.Close()

	// put a file
	timestamp := hummingbird.GetTimestamp()
	assert.True(t, e.PutObject(0, timestamp, "X", 0))

	// make a drive look unmounted with a handler that always 507s
	origHandler := e.servers[1].Config.Handler
	e.servers[1].Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(507)
	})

	// run a primary node's replicator
	e.replicators[0].Run()

	// so it's on the primary nodes that are up
	assert.True(t, e.ObjExists(0, timestamp, 0))
	assert.True(t, e.ObjExists(2, timestamp, 0))

	// and now it's on the handoff node
	assert.True(t, e.ObjExists(3, timestamp, 0))

	// fix the "unmounted" drive
	e.servers[1].Config.Handler = origHandler

	// make sure it's not on the newly fixed node yet
	assert.False(t, e.ObjExists(1, timestamp, 0))

	// run the handoff node's replicator
	e.replicators[3].Run()

	// it's no longer on the handoff node
	assert.False(t, e.ObjExists(3, timestamp, 0))

	// make sure it's on all the primary nodes
	assert.True(t, e.ObjExists(0, timestamp, 0))
	assert.True(t, e.ObjExists(1, timestamp, 0))
	assert.True(t, e.ObjExists(2, timestamp, 0))
}

func TestReplicationUnlinkOld(t *testing.T) {
	e := NewEnvironment()
	defer e.Close()

	// put a file to a primary node
	timestamp := hummingbird.GetTimestamp()
	assert.True(t, e.PutObject(0, timestamp, "X", 0))

	// put a newer file to another primary node
	timestamp2 := hummingbird.GetTimestamp()
	assert.True(t, e.PutObject(1, timestamp2, "X", 0))

	assert.True(t, e.ObjExists(0, timestamp, 0))
	assert.True(t, e.ObjExists(1, timestamp2, 0))

	// run the replicator on the server with the old file
	e.replicators[0].Run()

	// verify the old file was removed by the replicator
	assert.False(t, e.ObjExists(0, timestamp, 0))
}
