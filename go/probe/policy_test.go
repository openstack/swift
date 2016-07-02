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
	"os"
	"path/filepath"
	"testing"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/stretchr/testify/require"
)

func TestPolicySeparate(t *testing.T) {
	e := NewEnvironment()
	defer e.Close()

	timestamp := hummingbird.GetTimestamp()
	require.True(t, e.PutObject(0, timestamp, "X", 0))

	require.True(t, e.ObjExists(0, timestamp, 0))
	require.False(t, e.ObjExists(0, timestamp, 1))

	timestamp = hummingbird.GetTimestamp()
	require.True(t, e.DeleteObject(0, timestamp, 0))
	require.True(t, e.PutObject(0, timestamp, "X", 1))

	require.False(t, e.ObjExists(0, timestamp, 0))
	require.True(t, e.ObjExists(0, timestamp, 1))
}

func TestBasicPolicyReplication(t *testing.T) {
	e := NewEnvironment()
	defer e.Close()

	timestamp := hummingbird.GetTimestamp()
	require.True(t, e.PutObject(0, timestamp, "X", 1))

	e.replicators[0].Run()

	require.False(t, e.ObjExists(0, timestamp, 0))
	require.False(t, e.ObjExists(1, timestamp, 0))
	require.True(t, e.ObjExists(0, timestamp, 1))
	require.True(t, e.ObjExists(2, timestamp, 1))
}

func TestOtherPolicyAuditReplicate(t *testing.T) {
	e := NewEnvironment()
	defer e.Close()

	timestamp := hummingbird.GetTimestamp()
	e.PutObject(0, timestamp, "X", 1)
	e.PutObject(1, timestamp, "X", 1)

	locations := e.FileLocations("a", "c", "o", 1)
	path := filepath.Join(locations[0], timestamp+".data")

	e.auditors[0].Run()
	e.auditors[1].Run()

	require.True(t, e.ObjExists(0, timestamp, 1))
	require.True(t, e.ObjExists(1, timestamp, 1))

	f, _ := os.OpenFile(path, os.O_RDWR, 0777)
	f.Write([]byte("!"))
	f.Close()

	e.auditors[0].Run()
	e.auditors[1].Run()

	require.True(t, e.ObjExists(1, timestamp, 1))
	require.False(t, e.ObjExists(0, timestamp, 1))

	e.replicators[0].Run()
	e.replicators[1].Run()

	require.True(t, e.ObjExists(0, timestamp, 1))
	require.True(t, e.ObjExists(1, timestamp, 1))

	require.False(t, e.ObjExists(0, timestamp, 0))
	require.False(t, e.ObjExists(1, timestamp, 0))
}
