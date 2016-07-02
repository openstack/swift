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
	"net/http"
	"strconv"
	"strings"
)

type variable struct {
	name string
	pos  int
}

type matcher struct {
	method       string
	length       int
	hasCatchall  bool
	endSlash     bool
	catchallName string
	vars         []variable
	static       []variable
	policy       int
	handler      http.Handler
}

type router struct {
	matchers                []*matcher
	NotFoundHandler         http.Handler
	MethodNotAllowedHandler http.Handler
}

const anyPolicy = -1

// Split a string in twain on sep.  Doing it this way over strings.Split*() saves allocating a slice.
func Split2(path string, sep string) (string, string) {
	if nextSlash := strings.Index(path, sep); nextSlash == -1 {
		return path, ""
	} else {
		return path[:nextSlash], path[nextSlash+1:]
	}
}

// Given a method and path, return the handler and vars that should be used to serve them.
func (r *router) route(method, path string, policy int) (http.Handler, map[string]string) {
	methodFound := false
	path = path[1:]
	slashCount := strings.Count(path, "/")
	// This code is slightly gnarly because it avoids allocating anything until it's sure of a match.
NEXTMATCH:
	for _, m := range r.matchers {
		if m.policy != anyPolicy && m.policy != policy {
			continue
		}
		if m.method != method {
			continue
		}
		methodFound = true
		if !m.hasCatchall && len(path) != 0 && m.endSlash != (path[len(path)-1] == '/') {
			continue
		}
		if slashCount+1 == m.length || (m.hasCatchall && slashCount+1 > m.length) {
			var vars map[string]string

			// make sure all static parts of the route match the pattern
			j := 0
			part, tmppath := Split2(path, "/")
			for _, s := range m.static {
				for ; j < s.pos; j++ {
					part, tmppath = Split2(tmppath, "/")
				}
				if s.name != part {
					continue NEXTMATCH
				}
			}

			// if there's a catchall, grab it from the end of the path.
			if m.hasCatchall {
				tmppath = path
				for i := 0; i < m.length-1; i++ {
					_, tmppath = Split2(tmppath, "/")
				}
				if tmppath == "" {
					continue NEXTMATCH
				}
				vars = map[string]string{m.catchallName: tmppath}
			} else {
				vars = make(map[string]string, len(m.vars))
			}

			// extract any vars
			j = 0
			part, tmppath = Split2(path, "/")
			for _, s := range m.vars {
				for ; j < s.pos; j++ {
					part, tmppath = Split2(tmppath, "/")
				}
				vars[s.name] = part
			}
			return m.handler, vars
		}
	}
	if !methodFound {
		return r.MethodNotAllowedHandler, nil
	}
	return r.NotFoundHandler, nil
}

// HandlePolicy registers a handler for the given method, pattern, and policy header.
// The pattern is pretty much what you're used to, i.e. /static/:variable/*catchall
func (r *router) HandlePolicy(method, pattern string, policy int, handler http.Handler) {
	m := &matcher{method: method, policy: policy}
	parts := strings.Split(pattern[1:], "/")
	m.length = len(parts)
	if pattern[len(pattern)-1] == '/' {
		m.endSlash = true
	}
	for i, v := range parts {
		if len(v) > 0 && v[0] == ':' {
			m.vars = append(m.vars, variable{v[1:], i})
		} else if len(v) > 0 && v[0] == '*' {
			m.catchallName = v[1:]
			m.hasCatchall = true
		} else {
			m.static = append(m.static, variable{v, i})
		}
	}
	m.handler = handler
	r.matchers = append(r.matchers, m)
}

// Handle registers a handler for the given method and pattern.
// The pattern is pretty much what you're used to, i.e. /static/:variable/*catchall
func (r *router) Handle(method, pattern string, handler http.Handler) {
	r.HandlePolicy(method, pattern, anyPolicy, handler)
}

func (r *router) Get(path string, handler http.Handler) {
	r.Handle("GET", path, handler)
}

func (r *router) Put(path string, handler http.Handler) {
	r.Handle("PUT", path, handler)
}

func (r *router) Head(path string, handler http.Handler) {
	r.Handle("HEAD", path, handler)
}

func (r *router) Delete(path string, handler http.Handler) {
	r.Handle("DELETE", path, handler)
}

func (r *router) Replicate(path string, handler http.Handler) {
	r.Handle("REPLICATE", path, handler)
}

func (r *router) Sync(path string, handler http.Handler) {
	r.Handle("SYNC", path, handler)
}

func (r *router) Post(path string, handler http.Handler) {
	r.Handle("POST", path, handler)
}

func (r *router) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	policy, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policy = 0
	}
	handler, vars := r.route(request.Method, request.URL.Path, policy)
	SetVars(request, vars)
	handler.ServeHTTP(writer, request)
}

func NewRouter() *router {
	return &router{
		NotFoundHandler:         http.HandlerFunc(http.NotFound),
		MethodNotAllowedHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "Method Not Allowed", 405) }),
	}
}
