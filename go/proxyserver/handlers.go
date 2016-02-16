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

package proxyserver

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/bradfitz/gomemcache/memcache"
)

//Health check handler
func (server *ProxyServer) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
	return
}

//Account handlers
func (server *ProxyServer) AccountGetHeadHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	partition := server.accountRing.GetPartition(vars["account"], "", "")
	for _, device := range server.accountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s?%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), request.URL.RawQuery)
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			hummingbird.GetLogger(request).LogError("Error creating request for account GET: %s", err.Error())
			continue
		}
		hummingbird.CopyRequestHeaders(request, req)
		resp, err := server.client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if err == nil && (resp.StatusCode/100) == 2 {
			hummingbird.CopyResponseHeaders(writer, resp)
			writer.WriteHeader(http.StatusOK)
			if request.Method == "GET" {
				io.Copy(writer, resp.Body)
			} else {
				writer.Write([]byte(""))
			}
			return
		}
	}
}

func (server *ProxyServer) AccountPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	partition := server.containerRing.GetPartition(vars["account"], "", "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.accountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(vars["account"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		hummingbird.CopyRequestHeaders(request, req)
		rs.Do(hummingbird.GetLogger(request), req)
	}
	rs.BestResponse(writer)
}

func (server *ProxyServer) AccountDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	partition := server.containerRing.GetPartition(vars["account"], "", "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.accountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(vars["account"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		hummingbird.CopyRequestHeaders(request, req)
		rs.Do(hummingbird.GetLogger(request), req)
	}
	rs.BestResponse(writer)
}

//Object handlers
func (server *ProxyServer) ObjectGetHeadHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	partition := server.objectRing.GetPartition(vars["account"], vars["container"], vars["obj"])
	for _, device := range server.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			hummingbird.GetLogger(request).LogError("Error creating request to %s: %s", url, err.Error())
			continue
		}
		hummingbird.CopyRequestHeaders(request, req)
		resp, err := server.client.Do(req)
		if err != nil {
			hummingbird.GetLogger(request).LogError("Error getting response for %s to %s: %s", req.Method, req.URL.Host, err.Error())
			continue
		}
		if err == nil && (resp.StatusCode/100) == 2 {
			hummingbird.CopyResponseHeaders(writer, resp)
			writer.WriteHeader(resp.StatusCode)
			if request.Method == "GET" {
				io.Copy(writer, resp.Body)
			} else {
				writer.Write([]byte{})
			}
			resp.Body.Close()
			return
		}
		resp.Body.Close()
	}
}

func (server *ProxyServer) ObjectDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	partition := server.objectRing.GetPartition(vars["account"], vars["container"], vars["obj"])
	containerPartition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	containerDevices := server.containerRing.GetNodes(containerPartition)

	rs := MultiClient{server.client, 0, nil}
	for i, device := range server.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		req, _ := http.NewRequest("DELETE", url, nil)
		hummingbird.CopyRequestHeaders(request, req)
		req.ContentLength = request.ContentLength
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		rs.Do(hummingbird.GetLogger(request), req)
	}
	rs.BestResponse(writer)
}

func (server *ProxyServer) ObjectPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	partition := server.objectRing.GetPartition(vars["account"], vars["container"], vars["obj"])
	containerPartition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	containerDevices := server.containerRing.GetNodes(containerPartition)
	var writers []*io.PipeWriter
	resultSet := MultiClient{server.client, 0, nil}
	for i, device := range server.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		rp, wp := io.Pipe()
		defer wp.Close()
		defer rp.Close()
		req, err := http.NewRequest("PUT", url, rp)
		if err != nil {
			hummingbird.GetLogger(request).LogError("Error creating request for object PUT: %s", err.Error())
			continue
		}
		writers = append(writers, wp)
		hummingbird.CopyRequestHeaders(request, req)
		req.ContentLength = request.ContentLength
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		resultSet.Do(hummingbird.GetLogger(request), req)
	}
	mw := io.MultiWriter(writers[0], writers[1], writers[2])
	io.Copy(mw, request.Body)
	for _, writer := range writers {
		writer.Close()
	}
	resultSet.BestResponse(writer)
}

//Container handlers

func (server *ProxyServer) ContainerGetHeadHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)

	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	for _, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s?%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), request.URL.RawQuery)
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			hummingbird.GetLogger(request).LogError("Error creating request for container GET: %s", err.Error())
			continue
		}
		hummingbird.CopyRequestHeaders(request, req)
		resp, err := server.client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if err == nil && (resp.StatusCode/100) == 2 {
			hummingbird.CopyResponseHeaders(writer, resp)
			writer.WriteHeader(http.StatusOK)
			if request.Method == "GET" {
				io.Copy(writer, resp.Body)
			} else {
				writer.Write([]byte(""))
			}
			return
		}
	}
}

func (server *ProxyServer) ContainerPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	accountPartition := server.accountRing.GetPartition(vars["account"], "", "")
	accountDevices := server.accountRing.GetNodes(accountPartition)

	rs := MultiClient{server.client, 0, nil}
	for i, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		hummingbird.CopyRequestHeaders(request, req)
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		req.Header.Set("X-Account-Host", fmt.Sprintf("%s:%d", accountDevices[i].Ip, accountDevices[i].Port))
		req.Header.Set("X-Account-Device", accountDevices[i].Device)
		rs.Do(hummingbird.GetLogger(request), req)
	}
	rs.BestResponse(writer)
}

func (server *ProxyServer) ContainerDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	accountPartition := server.accountRing.GetPartition(vars["account"], "", "")
	accountDevices := server.accountRing.GetNodes(accountPartition)

	rs := MultiClient{server.client, 0, nil}
	for i, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]))
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			hummingbird.GetLogger(request).LogError("Error creating request for container DELETE: %s", err.Error())
		}
		hummingbird.CopyRequestHeaders(request, req)
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		req.Header.Set("X-Account-Host", fmt.Sprintf("%s:%d", accountDevices[i].Ip, accountDevices[i].Port))
		req.Header.Set("X-Account-Device", accountDevices[i].Device)
		rs.Do(hummingbird.GetLogger(request), req)
	}
	rs.BestResponse(writer)
}

// Auth Handlers

func (server *ProxyServer) AuthHandler(writer http.ResponseWriter, request *http.Request) {
	token := make([]byte, 32)
	for i := range token {
		token[i] = byte('A' + (rand.Int() % 26))
	}
	user := request.Header.Get("X-Auth-User")
	key := fmt.Sprintf("auth/AUTH_%s/%s", user, string(token))
	server.mc.Set(&memcache.Item{Key: key, Value: []byte("VALID")})
	writer.Header().Set("X-Storage-Token", string(token))
	writer.Header().Set("X-Auth-Token", string(token))
	writer.Header().Set("X-Storage-URL", fmt.Sprintf("http://%s/v1/AUTH_%s", request.Host, user))
	http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
}
