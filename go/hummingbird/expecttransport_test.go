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
	"bytes"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type FakeReader struct {
	readFrom bool
}

func (f *FakeReader) Read(p []byte) (int, error) {
	if !f.readFrom {
		f.readFrom = true
		for i := range p {
			p[i] = '.'
		}
		return 25, io.EOF
	}
	return 0, io.EOF
}

func TestRoundTrip(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readbody" {
			data, err := ioutil.ReadAll(r.Body)
			require.Nil(t, err)
			require.Equal(t, 25, len(data))
		}
		w.WriteHeader(200)
		w.Header().Set("Content-Length", "2")
		w.Write([]byte("hi"))
	}))

	client := &http.Client{
		Timeout: time.Second * 300,
		Transport: &ExpectTransport{
			Dial: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).Dial,
		},
	}

	readbuf := make([]byte, 16)

	body := &FakeReader{readFrom: false}
	req, err := http.NewRequest("POST", server.URL+"/readbody", body)
	require.Nil(t, err)
	req.Header.Add("Expect", "100-continue")
	req.ContentLength = 25
	resp, err := client.Do(req)
	require.Nil(t, err)
	require.True(t, body.readFrom)
	require.Equal(t, 200, resp.StatusCode)
	readlen, err := resp.Body.Read(readbuf)
	require.Equal(t, 2, readlen)
	resp.Body.Close()

	body = &FakeReader{readFrom: false}
	req, err = http.NewRequest("POST", server.URL+"/dontreadbody", body)
	require.Nil(t, err)
	req.Header.Add("Expect", "100-continue")
	req.ContentLength = 25
	resp, err = client.Do(req)
	require.Nil(t, err)
	require.False(t, body.readFrom)
	require.Equal(t, 200, resp.StatusCode)
	readlen, err = resp.Body.Read(readbuf)
	require.Equal(t, 2, readlen)
	resp.Body.Close()

	body = &FakeReader{readFrom: false}
	req, err = http.NewRequest("POST", server.URL+"/readbody", body)
	require.Nil(t, err)
	req.ContentLength = 25
	resp, err = client.Do(req)
	require.Nil(t, err)
	require.True(t, body.readFrom)
	require.Equal(t, 200, resp.StatusCode)
	readlen, err = resp.Body.Read(readbuf)
	require.Equal(t, 2, readlen)
	resp.Body.Close()

	body = &FakeReader{readFrom: false}
	req, err = http.NewRequest("POST", server.URL+"/dontreadbody", body)
	require.Nil(t, err)
	req.ContentLength = 25
	resp, err = client.Do(req)
	require.Nil(t, err)
	require.True(t, body.readFrom)
	require.Equal(t, 200, resp.StatusCode)
	readlen, err = resp.Body.Read(readbuf)
	require.Equal(t, 2, readlen)
	resp.Body.Close()
}

func TestRoundTripConcurrency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		require.Nil(t, err)
		require.Equal(t, 17, len(data))
		w.WriteHeader(200)
		w.Header().Set("Content-Length", "2")
		w.Write([]byte("hi"))
	}))

	client := &http.Client{
		Timeout: time.Second * 300,
		Transport: &ExpectTransport{
			Dial: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).Dial,
		},
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := bytes.NewReader([]byte("THIS IS SOME DATA"))
			req, err := http.NewRequest("POST", server.URL+"/", body)
			require.Nil(t, err)
			req.Header.Add("Expect", "100-continue")
			req.ContentLength = 17
			resp, err := client.Do(req)
			require.Nil(t, err)
			require.Equal(t, 200, resp.StatusCode)
			readbuf := make([]byte, 16)
			readlen, err := resp.Body.Read(readbuf)
			require.Equal(t, 2, readlen)
			resp.Body.Close()
		}()
	}
	wg.Wait()
}
