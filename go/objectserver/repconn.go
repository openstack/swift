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
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

var RepUnmountedError = fmt.Errorf("Device unmounted")
var repDialer = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).Dial

const repITimeout = time.Minute * 10
const repOTimeout = time.Minute
const repConnBufferSize = 32768

type BeginReplicationRequest struct {
	Device     string
	Partition  string
	NeedHashes bool
}

type BeginReplicationResponse struct {
	Hashes map[string]string
}

type SyncFileRequest struct {
	Path   string
	Xattrs string
	Size   int64
	Done   bool
}

type SyncFileResponse struct {
	Exists      bool
	NewerExists bool
	GoAhead     bool
	Msg         string
}

type FileUploadResponse struct {
	Success bool
	Msg     string
}

type RepConn struct {
	rw           *bufio.ReadWriter
	c            net.Conn
	Disconnected bool
}

func (r *RepConn) SendMessage(v interface{}) error {
	jsoned, err := json.Marshal(v)
	if err != nil {
		r.Close()
		return err
	}
	if err := binary.Write(r, binary.BigEndian, uint32(len(jsoned))); err != nil {
		r.Close()
		return err
	}
	if _, err := r.Write(jsoned); err != nil {
		r.Close()
		return err
	}
	if err := r.Flush(); err != nil {
		r.Close()
		return err
	}
	return nil
}

func (r *RepConn) RecvMessage(v interface{}) (err error) {
	r.c.SetDeadline(time.Now().Add(repITimeout))
	var length uint32
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		r.Close()
		return
	}
	data := make([]byte, length)
	if _, err = io.ReadFull(r, data); err != nil {
		r.Close()
		return
	}
	if err = json.Unmarshal(data, v); err != nil {
		r.Close()
		return
	}
	return
}

func (r *RepConn) Write(data []byte) (l int, err error) {
	r.c.SetDeadline(time.Now().Add(repOTimeout))
	if l, err = r.rw.Write(data); err != nil {
		r.Close()
	}
	return
}

func (r *RepConn) Flush() (err error) {
	r.c.SetDeadline(time.Now().Add(repOTimeout))
	if err = r.rw.Flush(); err != nil {
		r.Close()
	}
	return
}

func (r *RepConn) Read(data []byte) (l int, err error) {
	r.c.SetDeadline(time.Now().Add(repITimeout))
	if l, err = io.ReadFull(r.rw, data); err != nil {
		r.Close()
	}
	return
}

func (r *RepConn) Close() {
	r.Disconnected = true
	r.c.Close()
}

func NewRepConn(dev *hummingbird.Device, partition string, policy int) (*RepConn, error) {
	url := fmt.Sprintf("http://%s:%d/%s/%s", dev.ReplicationIp, dev.ReplicationPort, dev.Device, partition)
	req, err := http.NewRequest("REPCONN", url, nil)
	req.Header.Set("X-Backend-Storage-Policy-Index", strconv.Itoa(policy))
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-Trans-Id", fmt.Sprintf("%s-%d", hummingbird.UUID(), dev.Id))
	conn, err := repDialer("tcp", req.URL.Host)
	if err != nil {
		return nil, err
	}
	hc := httputil.NewClientConn(conn, nil)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, RepUnmountedError
	}
	newc, _ := hc.Hijack()
	if newc, ok := newc.(*net.TCPConn); ok {
		newc.SetNoDelay(true)
	}
	return &RepConn{
		rw: bufio.NewReadWriter(
			bufio.NewReaderSize(newc, repConnBufferSize),
			bufio.NewWriterSize(newc, repConnBufferSize)),
		c: newc,
	}, nil
}
