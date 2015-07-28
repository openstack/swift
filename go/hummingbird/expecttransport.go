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
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
)

// ExpectTransport is an http.RoundTripper that supports Expect: 100-continue.
// Nobody is happy that this exists.  Hopefully it will go away after Go 1.6.
// https://go-review.googlesource.com/#/c/10091/
type ExpectTransport struct {
	Dial               func(network, addr string) (net.Conn, error)
	DisableCompression bool // for compat sake, we don't use it.
}

func writeRequestHeaders(req *http.Request, conn net.Conn) error {
	bw := bufio.NewWriter(conn)
	if _, err := fmt.Fprintf(bw, "%s %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\n", req.Method, req.URL.RequestURI(), req.Host, req.ContentLength); err != nil {
		return err
	}
	if len(req.Header["User-Agent"]) == 0 {
		if _, err := bw.WriteString("User-Agent: Go-http-client/1.1\r\n"); err != nil {
			return err
		}
	}
	for key, values := range req.Header {
		if _, err := fmt.Fprintf(bw, "%s: %s\r\n", key, values[0]); err != nil {
			return err
		}
	}
	if _, err := bw.WriteString("\r\n"); err != nil {
		return err
	}
	return bw.Flush()
}

// roundtrip performs an http request, returning the response and error.
func (t *ExpectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		defer req.Body.Close()
	}
	dial := t.Dial
	if dial == nil {
		dial = net.Dial
	}
	conn, err := dial("tcp", req.URL.Host)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}
	if err := writeRequestHeaders(req, conn); err != nil {
		conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	if req.Header.Get("Expect") == "100-continue" {
		resp, err := http.ReadResponse(reader, req)
		if err != nil {
			conn.Close()
			return nil, err
		}
		if resp.StatusCode != 100 {
			resp.Body = &responseBody{ReadCloser: resp.Body, conn: conn}
			return resp, nil
		}
	}
	if req.Body != nil {
		if _, err := io.CopyN(conn, req.Body, req.ContentLength); err != nil {
			conn.Close()
			return nil, err
		}
	}
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		conn.Close()
		return nil, err
	}
	resp.Body = &responseBody{ReadCloser: resp.Body, conn: conn}
	return resp, nil
}

func (t *ExpectTransport) CancelRequest(req *http.Request) {
	// TODO(redbo)
}

type responseBody struct {
	io.ReadCloser
	conn net.Conn
}

// Close closes the response body, and either closes the underlying connection or returns it to the pool.
func (r *responseBody) Close() error {
	r.ReadCloser.Close()
	r.conn.Close()
	return nil
}
