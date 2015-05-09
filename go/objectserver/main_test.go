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
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/stretchr/testify/assert"
)

type TestServer struct {
	*httptest.Server
	host string
	port int
	root string
}

func (t *TestServer) Close() {
	os.RemoveAll(t.root)
	t.Server.Close()
}

func (t *TestServer) Do(method string, path string, body io.ReadCloser) (*http.Response, error) {
	req, err := http.NewRequest(method, fmt.Sprintf("http://%s:%d%s", t.host, t.port, path), body)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func makeObjectServer(settings ...string) (*TestServer, error) {
	driveRoot, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	conf, err := ioutil.TempFile(driveRoot, "")
	if err != nil {
		return nil, err
	}
	conf.WriteString("[app:object-server]\n")
	fmt.Fprintf(conf, "devices=%s\n", driveRoot)
	fmt.Fprintf(conf, "mount_check=false\n")
	for i := 0; i < len(settings); i += 2 {
		fmt.Fprintf(conf, "%s=%s\n", settings[i], settings[i+1])
	}
	if err := conf.Close(); err != nil {
		return nil, err
	}
	_, _, handler, _, _ := GetServer(conf.Name())
	ts := httptest.NewServer(handler)
	u, err := url.Parse(ts.URL)
	if err != nil {
		return nil, err
	}
	host, ports, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(ports)
	if err != nil {
		return nil, err
	}
	return &TestServer{Server: ts, host: host, port: port, root: driveRoot}, nil
}

func TestSyncWorks(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data", ts.host, ts.port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.True(t, hummingbird.Exists(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.77564.data")))
}

func TestSyncOlderData(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()
	os.MkdirAll(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff"), 0770)
	f, _ := os.Create(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.99999.data"))
	f.Close()
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data", ts.host, ts.port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.True(t, hummingbird.Exists(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.99999.data")))
}

func TestSyncBadFilename(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.datums", ts.host, ts.port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, hummingbird.Exists(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.77564.datums")))
}

func TestSyncBadXattrs(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data", ts.host, ts.port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "XXX")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, hummingbird.Exists(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.77564.data")))
}

func TestSyncConflictOnExists(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()
	url := fmt.Sprintf("http://%s:%d/sda/objects/1/fff/ffffffffffffffffffffffffffffffff/1425753549.77564.data", ts.host, ts.port)
	req, err := http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	assert.Nil(t, err)
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.True(t, hummingbird.Exists(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.77564.data")))

	req, err = http.NewRequest("SYNC", url, bytes.NewBuffer([]byte("SOME BODY")))
	req.Header.Set("X-Attrs", "0123456789ABCDEF")
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestReplicateInitialRequest(t *testing.T) {
	ts, err := makeObjectServer("replication_limit", "1/1")
	assert.Nil(t, err)
	defer ts.Close()
	os.MkdirAll(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff"), 0770)
	f, _ := os.Create(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.99999.data"))
	f.Close()
	url := fmt.Sprintf("http://%s:%d/sda/1", ts.host, ts.port)
	req, err := http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "12345")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	hashes, err := hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	req, err = http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "67890")
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestReplicateEndRequest(t *testing.T) {
	ts, err := makeObjectServer("replication_limit", "1/1")
	assert.Nil(t, err)
	defer ts.Close()

	os.MkdirAll(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff"), 0770)
	f, _ := os.Create(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.99999.data"))
	f.Close()
	url := fmt.Sprintf("http://%s:%d/sda/1", ts.host, ts.port)
	req, err := http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "12345")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ := ioutil.ReadAll(resp.Body)
	hashes, err := hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	url2 := fmt.Sprintf("http://%s:%d/sda/1/end", ts.host, ts.port)
	req, err = http.NewRequest("REPLICATE", url2, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "12345")
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequest("REPLICATE", url, nil)
	assert.Nil(t, err)
	req.Header.Set("X-Replication-Id", "67890")
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestReplicateRecalculate(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()
	os.MkdirAll(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff"), 0770)
	f, _ := os.Create(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753549.99999.data"))
	f.Close()

	resp, err := ts.Do("REPLICATE", "/sda/1", nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ := ioutil.ReadAll(resp.Body)
	hashes, err := hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	f, _ = os.Create(filepath.Join(ts.root, "sda", "objects", "1", "fff", "ffffffffffffffffffffffffffffffff", "1425753550.00000.meta"))
	f.Close()
	resp, err = ts.Do("REPLICATE", "/sda/1", nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ = ioutil.ReadAll(resp.Body)
	hashes, err = hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "b80a90865c11519b608af8471f5ab9ca")

	resp, err = ts.Do("REPLICATE", "/sda/1/fff", nil)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	data, _ = ioutil.ReadAll(resp.Body)
	hashes, err = hummingbird.PickleLoads(data)
	assert.Nil(t, err)
	assert.Equal(t, hashes.(map[interface{}]interface{})["fff"], "f78ade0081b2648499a4395d406e625c")
}

func TestBasicPutGet(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := hummingbird.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	assert.Nil(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, timestamp, resp.Header.Get("X-Timestamp"))
	assert.Equal(t, "9", resp.Header.Get("Content-Length"))
}

func TestBasicPutDelete(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	timestamp := hummingbird.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	timestamp = hummingbird.GetTimestamp()
	req, err = http.NewRequest("DELETE", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 204, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	assert.Nil(t, err)
	assert.Equal(t, 404, resp.StatusCode)
}

func TestGetRanges(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	getRanges := func(ranges string) (*http.Response, []byte) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
		assert.Nil(t, err)
		req.Header.Set("Range", ranges)
		resp, err := http.DefaultClient.Do(req)
		assert.Nil(t, err)
		body, err := ioutil.ReadAll(resp.Body)
		assert.Nil(t, err)
		return resp, body
	}

	resp, body := getRanges("bytes=0-5")
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "ABCDEF", string(body))

	resp, body = getRanges("bytes=20-")
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "UVWXYZ", string(body))

	resp, body = getRanges("bytes=-6")
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "UVWXYZ", string(body))

	resp, body = getRanges("bytes=27-28")
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)

	resp, body = getRanges("bytes=20-,-6")
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "multipart/byteranges;boundary="))
	assert.Equal(t, "356", resp.Header.Get("Content-Length"))
	assert.Equal(t, 2, strings.Count(string(body), "UVWXYZ"))
}
