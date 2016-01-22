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

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// TODO: Some of this code was pulled from the go stdlib and modified. figure out how to attribute this.
// https://wiki.openstack.org/wiki/LegalIssuesFAQ#Incorporating_BSD.2FMIT_Licensed_Code

package hummingbird

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/textproto"
)

type MultiWriter struct {
	w        io.Writer
	boundary string
	lastpart *part
}

func NewMultiWriter(w io.Writer) *MultiWriter {
	var buf [32]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic(err)
	}
	return &MultiWriter{
		w:        w,
		boundary: fmt.Sprintf("%x", buf[:]),
	}
}

func (w *MultiWriter) Boundary() string {
	return w.boundary
}

func (w *MultiWriter) CreatePart(header textproto.MIMEHeader) (io.Writer, error) {
	if w.lastpart != nil {
		if err := w.lastpart.close(); err != nil {
			return nil, err
		}
	}
	b := &bytes.Buffer{}
	if w.lastpart != nil {
		fmt.Fprintf(b, "\r\n--%s\r\n", w.boundary)
	} else {
		fmt.Fprintf(b, "--%s\r\n", w.boundary)
	}
	for k, vv := range header {
		for _, v := range vv {
			fmt.Fprintf(b, "%s: %s\r\n", k, v)
		}
	}
	fmt.Fprintf(b, "\r\n")
	_, err := io.Copy(w.w, b)
	if err != nil {
		return nil, err
	}
	p := &part{
		mw: w,
	}
	w.lastpart = p
	return p, nil
}

func (w *MultiWriter) Close() error {
	if w.lastpart != nil {
		if err := w.lastpart.close(); err != nil {
			return err
		}
		w.lastpart = nil
	}
	_, err := fmt.Fprintf(w.w, "\r\n--%s--", w.boundary)
	return err
}

type part struct {
	mw     *MultiWriter
	closed bool
	we     error
}

func (p *part) close() error {
	p.closed = true
	return p.we
}

func (p *part) Write(d []byte) (n int, err error) {
	if p.closed {
		return 0, errors.New("multipart: can't write to finished part")
	}
	n, err = p.mw.w.Write(d)
	if err != nil {
		p.we = err
	}
	return
}
