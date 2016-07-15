//  Copyright (c) 2015 Rackspace //
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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newRequest(method, path string) *http.Request {
	r, _ := http.NewRequest(method, path, nil)
	u, _ := url.Parse(path)
	r.URL = u
	r.RequestURI = path
	return r
}

type mockResponseWriter struct{}

func (m mockResponseWriter) Header() (h http.Header) {
	return http.Header{}
}

func (m mockResponseWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (m mockResponseWriter) WriteString(s string) (n int, err error) {
	return len(s), nil
}

func (m mockResponseWriter) WriteHeader(int) {}

func TestRouterDisambiguation(t *testing.T) {
	var vars map[string]string
	var handledBy string
	router := NewRouter()

	addRoute := func(method, pattern, handlerID string) {
		router.Handle(method, pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handledBy = handlerID
			vars = GetVars(r)
		}))
	}

	makeRequest := func(method, path string) {
		handledBy = "UNKNOWN"
		vars = nil
		router.ServeHTTP(mockResponseWriter{}, newRequest(method, path))
	}

	addRoute("GET", "/", "ROOT")
	addRoute("GET", "/healthcheck", "HEALTH_CHECK")
	addRoute("GET", "/diskusage", "DISKUSAGE")
	addRoute("GET", "/recon/:method", "RECON")
	addRoute("GET", "/:device/456/:account/:container", "STATIC_MIDDLE_ROUTE")
	addRoute("GET", "/:device/:partition/:account/:container/*obj", "OBJ_GET")
	addRoute("GET", "/:device/:partition/:account/:container", "CONTAINER_GET")
	addRoute("GET", "/:device/:partition/:account/:container/", "CONTAINER_GET")
	addRoute("GET", "/:device/:partition/:account", "ACCOUNT_GET")
	addRoute("GET", "/:device/:partition/:account/", "ACCOUNT_GET")
	addRoute("REPLICATE", "/:device/:partition/:suffixes", "REPLICATE")
	addRoute("REPLICATE", "/:device/:partition", "REPLICATE")
	addRoute("SYNC", "/:device/*relpath", "SYNC")
	addRoute("GET", "/info", "INFO")
	router.HandlePolicy("GET", "/reconstruct", 10, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handledBy = "POLICY_VERB_WORKED"
	}))
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handledBy = "NOT_FOUND"
		vars = nil
	})
	router.MethodNotAllowedHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handledBy = "NOT_ALLOWED"
		vars = nil
	})

	makeRequest("GET", "/")
	assert.Equal(t, "ROOT", handledBy)

	makeRequest("GET", "/healthcheck")
	assert.Equal(t, "HEALTH_CHECK", handledBy)

	makeRequest("SYNC", "/sda/some/other/stuff")
	assert.Equal(t, "SYNC", handledBy)
	assert.Equal(t, "sda", vars["device"])
	assert.Equal(t, "some/other/stuff", vars["relpath"])

	makeRequest("REPLICATE", "/sda/123")
	assert.Equal(t, "REPLICATE", handledBy)
	assert.Equal(t, "sda", vars["device"])
	assert.Equal(t, "123", vars["partition"])
	assert.Equal(t, "", vars["suffixes"])

	makeRequest("REPLICATE", "/sda/123/some-suffixes")
	assert.Equal(t, "REPLICATE", handledBy)
	assert.Equal(t, "sda", vars["device"])
	assert.Equal(t, "123", vars["partition"])
	assert.Equal(t, "some-suffixes", vars["suffixes"])

	makeRequest("GET", "/sda/123/acc/cont/a/bunch/of/stuff")
	assert.Equal(t, "OBJ_GET", handledBy)
	assert.Equal(t, "sda", vars["device"])
	assert.Equal(t, "123", vars["partition"])
	assert.Equal(t, "acc", vars["account"])
	assert.Equal(t, "cont", vars["container"])
	assert.Equal(t, "a/bunch/of/stuff", vars["obj"])

	makeRequest("GET", "/sda/123/acc/cont/a/bunch/of/stuff/")
	assert.Equal(t, "OBJ_GET", handledBy)
	assert.Equal(t, "a/bunch/of/stuff/", vars["obj"])

	makeRequest("GET", "/sda/456/acc/cont")
	assert.Equal(t, "STATIC_MIDDLE_ROUTE", handledBy)
	assert.Equal(t, "sda", vars["device"])
	assert.Equal(t, "acc", vars["account"])
	assert.Equal(t, "cont", vars["container"])

	makeRequest("GET", "/sda/123/acc/cont")
	assert.Equal(t, "CONTAINER_GET", handledBy)
	assert.Equal(t, "sda", vars["device"])
	assert.Equal(t, "123", vars["partition"])
	assert.Equal(t, "acc", vars["account"])
	assert.Equal(t, "cont", vars["container"])

	makeRequest("GET", "/sda/123/acc/cont/")
	assert.Equal(t, "CONTAINER_GET", handledBy)
	assert.Equal(t, "sda", vars["device"])
	assert.Equal(t, "123", vars["partition"])
	assert.Equal(t, "acc", vars["account"])
	assert.Equal(t, "cont", vars["container"])

	makeRequest("GET", "/sda/123/acc/cont//////")
	assert.Equal(t, "OBJ_GET", handledBy)
	assert.Equal(t, "/////", vars["obj"])

	makeRequest("GET", "/recon/whatever")
	assert.Equal(t, "RECON", handledBy)
	assert.Equal(t, "whatever", vars["method"])

	makeRequest("KISS", "/sda/123/acc/cont/a/bunch/of/stuff/")
	assert.Equal(t, "NOT_ALLOWED", handledBy)
	assert.Nil(t, vars)

	makeRequest("GET", "/something/bad")
	assert.Equal(t, "NOT_FOUND", handledBy)
	assert.Nil(t, vars)

	req, _ := http.NewRequest("GET", "/reconstruct", nil)
	req.URL, _ = url.Parse("/reconstruct")
	req.RequestURI = "/reconstruct"
	router.ServeHTTP(mockResponseWriter{}, req)
	assert.Equal(t, "NOT_FOUND", handledBy)

	req.Header.Set("X-Backend-Storage-Policy-Index", "10")
	router.ServeHTTP(mockResponseWriter{}, req)
	assert.Equal(t, "POLICY_VERB_WORKED", handledBy)
}

func BenchmarkRouteObject(b *testing.B) {
	router := NewRouter()
	objGet := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	router.Get("/healthcheck", objGet)
	router.Get("/diskusage", objGet)
	router.Get("/recon/:method", objGet)
	router.Head("/:device/:partition/:account/:container/*obj", objGet)
	router.Put("/:device/:partition/:account/:container/*obj", objGet)
	router.Delete("/:device/:partition/:account/:container/*obj", objGet)
	router.Replicate("/:device/:partition/:suffixes", objGet)
	router.Replicate("/:device/:partition", objGet)
	router.Sync("/:device/*relpath", objGet)
	router.Get("/:device/:partition/:account/:container/*obj", objGet)

	r := newRequest("GET", "/sda/123/acc/cont/a/bunch/of/stuff")
	w := mockResponseWriter{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		router.ServeHTTP(w, r)
	}
}
