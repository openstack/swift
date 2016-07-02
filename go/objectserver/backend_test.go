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
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadMetadata(t *testing.T) {

	data := map[string]string{
		strings.Repeat("la", 5):    strings.Repeat("la", 30),
		strings.Repeat("moo", 500): strings.Repeat("moo", 300),
	}
	testFile, err := ioutil.TempFile("/tmp", "backend_test")
	defer testFile.Close()
	defer os.Remove(testFile.Name())
	assert.Equal(t, err, nil)
	WriteMetadata(testFile.Fd(), data)
	checkData := map[string]string{
		strings.Repeat("la", 5):    strings.Repeat("la", 30),
		strings.Repeat("moo", 500): strings.Repeat("moo", 300),
	}
	readData, err := ReadMetadata(testFile.Name())
	assert.Equal(t, err, nil)
	assert.True(t, reflect.DeepEqual(checkData, readData))

	readData, err = ReadMetadata(testFile.Fd())
	assert.Equal(t, err, nil)
	assert.True(t, reflect.DeepEqual(checkData, readData))
}

func TestGetHashes(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	os.MkdirAll(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	os.MkdirAll(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "00000000000000000000000000000abc"), 0777)
	f, _ := os.Create(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	f.Close()
	f, _ = os.Create(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "00000000000000000000000000000abc", "67890.data"))
	f.Close()

	hashes, err := GetHashes(driveRoot, "sda", "1", nil, int64(hummingbird.ONE_WEEK), 0, nil)
	assert.Nil(t, err)
	assert.Equal(t, "b1589029b7db9d01347caece2159d588", hashes["abc"])

	// write a new file there
	f, _ = os.Create(filepath.Join(driveRoot, "", "sda", "objects", "1", "abc", "00000000000000000000000000000abc", "99999.meta"))
	f.Close()

	// make sure hash for "abc" isn't recalculated yet.
	hashes, err = GetHashes(driveRoot, "sda", "1", nil, int64(hummingbird.ONE_WEEK), 0, nil)
	assert.Nil(t, err)
	assert.Equal(t, "b1589029b7db9d01347caece2159d588", hashes["abc"])

	// force recalculate of "abc"
	hashes, err = GetHashes(driveRoot, "sda", "1", []string{"abc"}, int64(hummingbird.ONE_WEEK), 0, nil)
	assert.Nil(t, err)
	assert.Equal(t, "8834e84467693c2e8f670f4afbea5334", hashes["abc"])
}

func TestInvalidateHash(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	os.MkdirAll(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	os.MkdirAll(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "00000000000000000000000000000abc"), 0777)
	f, _ := os.Create(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	f.Close()
	f, _ = os.Create(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "00000000000000000000000000000abc", "67890.data"))
	f.Close()

	hashes, err := GetHashes(driveRoot, "sda", "1", nil, int64(hummingbird.ONE_WEEK), 0, nil)
	assert.Nil(t, err)
	assert.Equal(t, "b1589029b7db9d01347caece2159d588", hashes["abc"])

	// write a new file there
	f, _ = os.Create(filepath.Join(driveRoot, "", "sda", "objects", "1", "abc", "00000000000000000000000000000abc", "99999.meta"))
	f.Close()

	// make sure hash for "abc" isn't recalculated yet.
	hashes, err = GetHashes(driveRoot, "sda", "1", nil, int64(hummingbird.ONE_WEEK), 0, nil)
	assert.Nil(t, err)
	assert.Equal(t, "b1589029b7db9d01347caece2159d588", hashes["abc"])

	// invalidate hash of suffix "abc"
	InvalidateHash(filepath.Join(driveRoot, "", "sda", "objects", "1", "abc", "00000000000000000000000000000abc"))
	hashes, err = GetHashes(driveRoot, "sda", "1", nil, int64(hummingbird.ONE_WEEK), 0, nil)
	assert.Nil(t, err)
	assert.Equal(t, "8834e84467693c2e8f670f4afbea5334", hashes["abc"])
}

func TestPolicyDir(t *testing.T) {
	policy, err := UnPolicyDir("objects")
	require.Nil(t, err)
	require.Equal(t, "objects", PolicyDir(policy))

	policy, err = UnPolicyDir("objects-1")
	require.Nil(t, err)
	require.Equal(t, "objects-1", PolicyDir(policy))

	policy, err = UnPolicyDir("objects-100")
	require.Nil(t, err)
	require.Equal(t, "objects-100", PolicyDir(policy))
}

func TestQuarantineHash(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)

	hashDir := filepath.Join(driveRoot, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc")
	os.MkdirAll(hashDir, 0777)
	QuarantineHash(hashDir)
	require.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "quarantined", "objects")))
	require.False(t, hummingbird.Exists(hashDir))

	hashDir = filepath.Join(driveRoot, "sdb", "objects-1", "1", "abc", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	os.MkdirAll(hashDir, 0777)
	QuarantineHash(hashDir)
	require.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sdb", "quarantined", "objects-1")))
	require.False(t, hummingbird.Exists(hashDir))
}
