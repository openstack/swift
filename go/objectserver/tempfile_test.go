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
	"testing"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/stretchr/testify/require"
)

func TestTempFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	f, err := NewAtomicFileWriter(dir, filepath.Join(dir, "somefile"))
	require.Nil(t, err)
	f.Write([]byte("some crap"))
	require.Nil(t, f.Save())
	require.True(t, hummingbird.Exists(filepath.Join(dir, "somefile")))
}

func TestTempFileDirRemoved(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(tempdir)
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	f, err := NewAtomicFileWriter(tempdir, filepath.Join(dir, "somefile"))
	require.Nil(t, err)
	f.Write([]byte("some crap"))
	os.RemoveAll(dir)
	require.Nil(t, f.Save())
	require.True(t, hummingbird.Exists(filepath.Join(dir, "somefile")))
}

func TestTempFileReplace(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	require.Nil(t, ioutil.WriteFile(filepath.Join(dir, "somefile"), []byte("original contents"), 0666))
	f, err := NewAtomicFileWriter(dir, filepath.Join(dir, "somefile"))
	require.Nil(t, err)
	f.Write([]byte("some crap"))
	require.Nil(t, f.Save())
	require.True(t, hummingbird.Exists(filepath.Join(dir, "somefile")))
	data, err := ioutil.ReadFile(filepath.Join(dir, "somefile"))
	require.Nil(t, err)
	require.Equal(t, []byte("some crap"), data)
}
