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

package middleware

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"io"
	"net/http"
	"regexp"
)

type grepWriter struct {
	w      io.Writer
	h      http.Header
	status int
}

func (g *grepWriter) Header() http.Header {
	return g.h
}

func (g *grepWriter) Write(buf []byte) (int, error) {
	return g.w.Write(buf)
}

func (g *grepWriter) WriteHeader(status int) {
	g.status = status
}

// GrepObject is an http middleware that searches objects line-by-line on the object server, similar to grep(1).
func GrepObject(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "GREP" {
			next.ServeHTTP(writer, request)
			return
		}
		pr, pw := io.Pipe()
		defer pr.Close()
		defer pw.Close()
		newWriter := &grepWriter{w: pw, h: make(http.Header), status: 200}
		newRequest, _ := http.NewRequest("GET", request.URL.String(), nil)
		newRequest.Header = request.Header
		go func() {
			defer pw.Close()
			next.ServeHTTP(newWriter, newRequest)
		}()
		q := request.URL.Query().Get("e")
		if q == "" {
			writer.WriteHeader(400)
			return
		}
		re, err := regexp.Compile(q)
		if err != nil {
			writer.WriteHeader(400)
			return
		}
		// peek at response data first to make sure the downstream handler has set a status code
		br := bufio.NewReader(pr)
		magic, err := br.Peek(4)
		if newWriter.status == 200 {
			var scanner *bufio.Scanner
			if err == nil && magic[0] == 0x1f && magic[1] == 0x8b {
				if gzr, err := gzip.NewReader(br); err != nil {
					writer.WriteHeader(500)
					return
				} else {
					scanner = bufio.NewScanner(gzr)
				}
			} else if err == nil && magic[0] == 'B' && magic[1] == 'Z' && magic[2] == 'h' && magic[3] >= '1' && magic[3] <= '9' {
				scanner = bufio.NewScanner(bzip2.NewReader(br))
			} else {
				scanner = bufio.NewScanner(br)
			}
			writer.WriteHeader(200)
			for scanner.Scan() {
				if line := scanner.Bytes(); re.Match(line) {
					writer.Write(line)
					writer.Write([]byte{'\n'})
				}
			}
		} else {
			writer.WriteHeader(newWriter.status)
			io.Copy(writer, br)
		}
	})
}
