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

package hummingbird

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDumpReconCache(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value", "somethingelse": "othervalue"})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "value", mapdata["something"])
}

func TestDumpReconCacheEmptyDeletes(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value"})
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{}})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	_, ok := mapdata["something"]
	assert.False(t, ok)
}

func TestDumpReconCacheNilDeletes(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value"})
	DumpReconCache(dir, "object", map[string]interface{}{"something": nil})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	_, ok := mapdata["something"]
	assert.False(t, ok)
}

func TestDumpReconCacheOverwrite(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value", "somethingelse": "othervalue"})
	DumpReconCache(dir, "object", map[string]interface{}{"something": "value2"})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "othervalue", mapdata["somethingelse"])
	assert.Equal(t, "value2", mapdata["something"])
}

func TestDumpReconCacheUpdateSubMap(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": "subvalue1", "subkey2": "subvalue2"}})
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": "newvalue"}})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "subvalue2", mapdata["something"].(map[string]interface{})["subkey2"])
	assert.Equal(t, "newvalue", mapdata["something"].(map[string]interface{})["subkey1"])
}

func TestDumpReconCacheNilDeleteSubKey(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": "subvalue1", "subkey2": "subvalue2"}})
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": nil}})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "subvalue2", mapdata["something"].(map[string]interface{})["subkey2"])
	_, ok := mapdata["something"].(map[string]interface{})["subkey1"]
	assert.False(t, ok)
}

func TestDumpReconCacheEmptyDeleteSubKey(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": "subvalue1", "subkey2": "subvalue2"}})
	DumpReconCache(dir, "object", map[string]interface{}{"something": map[string]interface{}{"subkey1": map[string]interface{}{}}})
	filedata, _ := ioutil.ReadFile(filepath.Join(dir, "object.recon"))
	var data interface{}
	json.Unmarshal(filedata, &data)
	mapdata := data.(map[string]interface{})
	assert.Equal(t, "subvalue2", mapdata["something"].(map[string]interface{})["subkey2"])
	_, ok := mapdata["something"].(map[string]interface{})["subkey1"]
	assert.False(t, ok)
}
