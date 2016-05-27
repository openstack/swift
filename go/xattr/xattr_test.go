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

package xattr

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFXattr(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer fp.Close()
	defer os.RemoveAll(fp.Name())

	_, err = Setxattr(fp.Fd(), "user.swift.metadata", []byte("somevalue"))
	require.Nil(t, err)

	count, err := Getxattr(fp.Fd(), "user.swift.metadata", nil)
	require.Nil(t, err)
	value := make([]byte, count)
	count, err = Getxattr(fp.Fd(), "user.swift.metadata", value)
	require.Nil(t, err)
	require.Equal(t, "somevalue", string(value))
}

func TestXattr(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer fp.Close()
	defer os.RemoveAll(fp.Name())

	_, err = Setxattr(fp.Name(), "user.swift.metadata", []byte("somevalue"))
	require.Nil(t, err)

	count, err := Getxattr(fp.Name(), "user.swift.metadata", nil)
	require.Nil(t, err)
	value := make([]byte, count)
	count, err = Getxattr(fp.Name(), "user.swift.metadata", value)
	require.Nil(t, err)
	require.Equal(t, "somevalue", string(value))
}
