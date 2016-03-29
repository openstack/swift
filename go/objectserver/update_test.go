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
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gorilla/context"
	"github.com/openstack/swift/go/hummingbird"
)

type DummyLogger struct{}

func (a *DummyLogger) LogDebug(format string, args ...interface{}) {}
func (a *DummyLogger) LogError(format string, args ...interface{}) {}
func (a *DummyLogger) LogInfo(format string, args ...interface{})  {}
func (a *DummyLogger) LogPanics(m string) {
	if e := recover(); e != nil {
	}
}

func TestExpirerContainer(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()
	server := ts.objServer
	server.hashPathPrefix = ""
	server.hashPathSuffix = "changeme"
	/*
		I'm just testing this by comparing it against the output from Swift.

		>> : get_expirer_container("1434707411", 10, "a", "c", "o")
		<< : '1434707373'
	*/
	server.expiringDivisor = 10
	require.Equal(t, "1434707373", server.expirerContainer(time.Unix(1434707411, 0), "a", "c", "o"))
	/*
		>> : get_expirer_container("1434707411", 10, "a", "c", "o")
		<< : '1434671963'
	*/
	server.expiringDivisor = 86400
	require.Equal(t, "1434671963", server.expirerContainer(time.Unix(1434707411, 0), "a", "c", "o"))
}

func TestUpdateDeleteAt(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	server := ts.objServer
	ts.Close()
	server.hashPathPrefix = ""
	server.hashPathSuffix = "changeme"

	requestSent := false
	cs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/sdb/678/.expiring_objects/1434671963/1434707411-a/c/o", r.URL.Path)
		requestSent = true
	}))
	defer cs.Close()
	u, err := url.Parse(cs.URL)
	require.Nil(t, err)
	req, err := http.NewRequest("PUT", "/I/dont/think/this/matters", nil)
	require.Nil(t, err)

	req.Header.Add("X-Delete-At-Container", "1434671963")
	req.Header.Add("X-Delete-At-Partition", "678")
	req.Header.Add("X-Delete-At-Host", u.Host)
	req.Header.Add("X-Delete-At-Device", "sdb")
	req.Header.Add("X-Timestamp", "12345.6789")

	dl := DummyLogger{}

	vars := map[string]string{"account": "a", "container": "c", "obj": "o", "device": "sda"}
	hummingbird.SetVars(req, vars)
	defer context.Clear(req)
	deleteAtStr := "1434707411"
	server.updateDeleteAt(req, deleteAtStr, vars, &dl)
	require.True(t, requestSent)

	cs.Close()
	server.updateDeleteAt(req, deleteAtStr, vars, &dl)
	expectedFile := filepath.Join(ts.root, "sda", "async_pending", "8fc", "02cc012fe572f27e455edbea32da78fc-12345.6789")
	require.True(t, hummingbird.Exists(expectedFile))
	data, err := ioutil.ReadFile(expectedFile)
	require.Nil(t, err)
	a, err := hummingbird.PickleLoads(data)
	require.Nil(t, err)
	asyncData := a.(map[interface{}]interface{})
	require.Equal(t, asyncData["op"], "PUT")
	require.Equal(t, asyncData["account"], ".expiring_objects")
	require.Equal(t, asyncData["container"], "1434671963")
	require.Equal(t, asyncData["obj"], "1434707411-a/c/o")
}

func TestUpdateDeleteAtNoHeaders(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	server := ts.objServer
	ts.Close()
	server.hashPathPrefix = ""
	server.hashPathSuffix = "changeme"
	req, err := http.NewRequest("PUT", "/I/dont/think/this/matters", nil)
	require.Nil(t, err)
	req.Header.Add("X-Timestamp", "12345.6789")

	vars := map[string]string{"account": "a", "container": "c", "obj": "o", "device": "sda"}
	hummingbird.SetVars(req, vars)
	defer context.Clear(req)
	deleteAtStr := "1434707411"
	server.updateDeleteAt(req, deleteAtStr, vars, &DummyLogger{})
	expectedFile := filepath.Join(ts.root, "sda", "async_pending", "8fc", "02cc012fe572f27e455edbea32da78fc-12345.6789")
	require.True(t, hummingbird.Exists(expectedFile))
	data, err := ioutil.ReadFile(expectedFile)
	require.Nil(t, err)
	a, err := hummingbird.PickleLoads(data)
	require.Nil(t, err)
	asyncData := a.(map[interface{}]interface{})
	require.Equal(t, asyncData["op"], "PUT")
	require.Equal(t, asyncData["account"], ".expiring_objects")
	require.Equal(t, asyncData["container"], "1434671963")
	require.Equal(t, asyncData["obj"], "1434707411-a/c/o")
}

func TestUpdateContainer(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	server := ts.objServer
	ts.Close()
	server.hashPathPrefix = ""
	server.hashPathSuffix = "changeme"

	requestSent := false
	cs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/sdb/1/a/c/o", r.URL.Path)
		require.Equal(t, "text/plain", r.Header.Get("X-Content-Type"))
		require.Equal(t, "30", r.Header.Get("X-Size"))
		require.Equal(t, "ffffffffffffffffffffffffffffffff", r.Header.Get("X-Etag"))
		requestSent = true
	}))
	defer cs.Close()
	u, err := url.Parse(cs.URL)
	require.Nil(t, err)
	req, err := http.NewRequest("PUT", "/I/dont/think/this/matters", nil)
	require.Nil(t, err)
	req.Header.Add("X-Container-Partition", "1")
	req.Header.Add("X-Container-Host", u.Host)
	req.Header.Add("X-Container-Device", "sdb")
	req.Header.Add("X-Timestamp", "12345.6789")

	dl := DummyLogger{}

	vars := map[string]string{"account": "a", "container": "c", "obj": "o", "device": "sda"}
	hummingbird.SetVars(req, vars)
	defer context.Clear(req)
	metadata := map[string]string{
		"X-Timestamp":    "12345.789",
		"Content-Type":   "text/plain",
		"Content-Length": "30",
		"ETag":           "ffffffffffffffffffffffffffffffff",
	}
	server.updateContainer(metadata, req, vars, &dl)
	require.True(t, requestSent)

	cs.Close()
	server.updateContainer(metadata, req, vars, &dl)
	expectedFile := filepath.Join(ts.root, "sda", "async_pending", "099", "2f714cd91b0e5d803cde2012b01d7099-12345.6789")
	require.True(t, hummingbird.Exists(expectedFile))
	data, err := ioutil.ReadFile(expectedFile)
	require.Nil(t, err)
	a, err := hummingbird.PickleLoads(data)
	require.Nil(t, err)
	asyncData := a.(map[interface{}]interface{})
	require.Equal(t, asyncData["op"], "PUT")
	require.Equal(t, asyncData["account"], "a")
	require.Equal(t, asyncData["container"], "c")
	require.Equal(t, asyncData["obj"], "o")
}

func TestUpdateContainerNoHeaders(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	server := ts.objServer
	ts.Close()
	server.hashPathPrefix = ""
	server.hashPathSuffix = "changeme"

	requestSent := false
	cs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestSent = true
	}))
	defer cs.Close()
	req, err := http.NewRequest("PUT", "/I/dont/think/this/matters", nil)
	require.Nil(t, err)
	req.Header.Add("X-Timestamp", "12345.6789")

	vars := map[string]string{"account": "a", "container": "c", "obj": "o", "device": "sda"}
	hummingbird.SetVars(req, vars)
	defer context.Clear(req)
	metadata := map[string]string{
		"X-Timestamp":    "12345.789",
		"Content-Type":   "text/plain",
		"Content-Length": "30",
		"ETag":           "ffffffffffffffffffffffffffffffff",
	}
	dl := DummyLogger{}
	server.updateContainer(metadata, req, vars, &dl)
	require.False(t, requestSent)

	cs.Close()
	server.updateContainer(metadata, req, vars, &dl)
	expectedFile := filepath.Join(ts.root, "sda", "async_pending", "099", "2f714cd91b0e5d803cde2012b01d7099-12345.6789")
	require.False(t, hummingbird.Exists(expectedFile))
}
