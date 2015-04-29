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
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/stretchr/testify/assert"
)

func makeObjectServer(settings ...string) (ts *httptest.Server, host string, port int) {
	conf, _ := ioutil.TempFile("", "")
	conf.WriteString("[app:object-server]\n")
	for i := 0; i < len(settings); i += 2 {
		fmt.Fprintf(conf, "%s=%s\n", settings[i], settings[i+1])
	}
	defer conf.Close()
	defer os.RemoveAll(conf.Name())
	_, _, handler, _, _ := GetServer(conf.Name())

	ts = httptest.NewServer(handler)
	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ = strconv.Atoi(ports)
	return ts, host, port
}

func TestSyncWorks(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	ts, host, port := makeObjectServer("devices", driveRoot, "mount_check", "false")
	defer ts.Close()
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data", host, port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.True(t, hummingbird.Exists(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data"))
}

func TestSyncOlderData(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	ts, host, port := makeObjectServer("devices", driveRoot, "mount_check", "false")
	defer ts.Close()
	os.MkdirAll(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff", 0770)
	f, _ := os.Create(driveRoot + "/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.99999.data")
	f.Close()
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data", host, port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.True(t, hummingbird.Exists(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.99999.data"))
}

func TestSyncBadFilename(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	ts, host, port := makeObjectServer("devices", driveRoot, "mount_check", "false")
	defer ts.Close()
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.datums", host, port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, hummingbird.Exists(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.datums"))
}

func TestSyncBadXattrs(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	ts, host, port := makeObjectServer("devices", driveRoot, "mount_check", "false")
	defer ts.Close()
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data", host, port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "XXX")
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, hummingbird.Exists(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data"))
}

func TestSyncConflictOnExists(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	ts, host, port := makeObjectServer("devices", driveRoot, "mount_check", "false")
	defer ts.Close()
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data", host, port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.True(t, hummingbird.Exists(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data"))

	req, err = http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err = client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestReplicateInitialRequest(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	ts, host, port := makeObjectServer("devices", driveRoot, "mount_check", "false", "replication_limit", "1/1")
	defer ts.Close()
	os.MkdirAll(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff", 0770)
	f, _ := os.Create(driveRoot + "/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.99999.data")
	f.Close()
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%d/sda/1", host, port)
	req, err := http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "12345")
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ := ioutil.ReadAll(resp.Body)
	hashes, err := hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	req, err = http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "67890")
	resp, err = client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestReplicateEndRequest(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	ts, host, port := makeObjectServer("devices", driveRoot, "mount_check", "false", "replication_limit", "1/1")
	defer ts.Close()
	os.MkdirAll(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff", 0770)
	f, _ := os.Create(driveRoot + "/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.99999.data")
	f.Close()
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%d/sda/1", host, port)
	req, err := http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "12345")
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ := ioutil.ReadAll(resp.Body)
	hashes, err := hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	url2 := fmt.Sprintf("http://%s:%d/sda/1/end", host, port)
	req, err = http.NewRequest("REPLICATE", url2, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "12345")
	resp, err = client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "67890")
	resp, err = client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestReplicateRecalculate(t *testing.T) {
	driveRoot, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(driveRoot)
	ts, host, port := makeObjectServer("devices", driveRoot, "mount_check", "false")
	defer ts.Close()
	os.MkdirAll(driveRoot+"/sda/objects/1/fff/ffffffffffffffffffffffffffffffff", 0770)
	f, _ := os.Create(driveRoot + "/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.99999.data")
	f.Close()
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%d/sda/1", host, port)
	req, err := http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ := ioutil.ReadAll(resp.Body)
	hashes, err := hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	f, _ = os.Create(driveRoot + "/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753550.00000.meta")
	f.Close()
	req, err = http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	resp, err = client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ = ioutil.ReadAll(resp.Body)
	hashes, err = hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	req, err = http.NewRequest("REPLICATE", url+"/fff", nil)
	assert.Nil(t, err)
	resp, err = client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ = ioutil.ReadAll(resp.Body)
	hashes, err = hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "f78ade0081b2648499a4395d406e625c")
}
