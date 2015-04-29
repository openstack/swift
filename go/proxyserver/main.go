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
	"errors"
	"fmt"
	"io"
	"log/syslog"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/bradfitz/gomemcache/memcache"
)

type ProxyHandler struct {
	objectRing    hummingbird.Ring
	containerRing hummingbird.Ring
	accountRing   hummingbird.Ring
	client        *http.Client
	logger        *syslog.Writer
	mc            *memcache.Client
}

// object that performs some number of requests asynchronously and aggregates the results

type MultiClient struct {
	client       *http.Client
	requestCount int
	done         []chan int
}

func (mc *MultiClient) Do(log hummingbird.LoggingContext, req *http.Request) {
	donech := make(chan int)
	mc.done = append(mc.done, donech)
	go func(client *http.Client, req *http.Request, done chan int) {
		resp, err := client.Do(req)
		if err != nil {
			log.LogError("Error getting response for %s to %s: %s", req.Method, req.URL.Host, err.Error())
			done <- 500
		} else {
			resp.Body.Close()
			if resp.StatusCode/100 == 5 {
				log.LogError("5XX response code on %s to %s", req.Method, req.URL.Host)
			}
			done <- resp.StatusCode
		}
	}(mc.client, req, donech)
	mc.requestCount += 1
}

func (mc *MultiClient) BestResponse(writer *hummingbird.WebWriter) {
	var responses []int
	quorum := (mc.requestCount / 2) + 1
	for chanIndex, done := range mc.done {
		responses = append(responses, <-done)
		for statusRange := 200; statusRange <= 400; statusRange += 100 {
			rangeCount := 0
			for _, response := range responses {
				if response >= statusRange && response < (statusRange+100) {
					rangeCount += 1
					if rangeCount >= quorum {
						http.Error(writer, http.StatusText(response), response)
						go func() { // wait for any remaining connections to finish
							for i := chanIndex + 1; i < len(mc.done); i++ {
								<-mc.done[i]
							}
						}()
						return
					}
				}
			}
		}
	}
	http.Error(writer, http.StatusText(500), 500)
}

// request handlers

func (server *ProxyHandler) ObjectGetHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	partition := server.objectRing.GetPartition(vars["account"], vars["container"], vars["obj"])
	for _, device := range server.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			request.LogError("Error creating request to %s: %s", url, err.Error())
			continue
		}
		request.CopyRequestHeaders(req)
		resp, err := server.client.Do(req)
		if err != nil {
			request.LogError("Error getting response for %s to %s: %s", req.Method, req.URL.Host, err.Error())
			continue
		}
		if err == nil && (resp.StatusCode/100) == 2 {
			writer.CopyResponseHeaders(resp)
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

func (server *ProxyHandler) ObjectPutHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
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
			request.LogError("Error creating request for object PUT: %s", err.Error())
			continue
		}
		writers = append(writers, wp)
		request.CopyRequestHeaders(req)
		req.ContentLength = request.ContentLength
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		resultSet.Do(request, req)
	}
	mw := io.MultiWriter(writers[0], writers[1], writers[2])
	io.Copy(mw, request.Body)
	for _, writer := range writers {
		writer.Close()
	}
	resultSet.BestResponse(writer)
}

func (server *ProxyHandler) ObjectDeleteHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	partition := server.objectRing.GetPartition(vars["account"], vars["container"], vars["obj"])
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.objectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		req, _ := http.NewRequest("DELETE", url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(request, req)
	}
	rs.BestResponse(writer)
}

func (server *ProxyHandler) ContainerGetHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	for _, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s?%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), request.URL.RawQuery)
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			request.LogError("Error creating request for container GET: %s", err.Error())
			continue
		}
		request.CopyRequestHeaders(req)
		request.CopyRequestHeaders(req)
		resp, err := server.client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if err == nil && (resp.StatusCode/100) == 2 {
			writer.CopyResponseHeaders(resp)
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

func (server *ProxyHandler) ContainerPutHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(request, req)
	}
	rs.BestResponse(writer)
}

func (server *ProxyHandler) ContainerDeleteHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], vars["container"], "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.containerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]))
		req, err := http.NewRequest(request.Method, url, nil)
		if err != nil {
			request.LogError("Error creating request for container DELETE: %s", err.Error())
		}
		request.CopyRequestHeaders(req)
		rs.Do(request, req)
	}
	rs.BestResponse(writer)
}

func (server *ProxyHandler) AccountGetHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	fmt.Println("ACCOUNT GET?!")
	http.Error(writer, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
}

func (server *ProxyHandler) AccountPutHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], "", "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.accountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(vars["account"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(request, req)
	}
	rs.BestResponse(writer)
}

func (server *ProxyHandler) AccountDeleteHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	partition := server.containerRing.GetPartition(vars["account"], "", "")
	rs := MultiClient{server.client, 0, nil}
	for _, device := range server.accountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(vars["account"]))
		req, _ := http.NewRequest(request.Method, url, nil)
		request.CopyRequestHeaders(req)
		rs.Do(request, req)
	}
	rs.BestResponse(writer)
}

func (server *ProxyHandler) AuthHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
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

// access log

func (server *ProxyHandler) LogRequest(writer *hummingbird.WebWriter, request *hummingbird.WebRequest) {
	go server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
		request.RemoteAddr,
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		request.Method,
		request.URL.Path,
		writer.Status,
		hummingbird.HeaderGetDefault(writer.Header(), "Content-Length", "-"),
		hummingbird.HeaderGetDefault(request.Header, "Referer", "-"),
		request.TransactionId,
		hummingbird.HeaderGetDefault(request.Header, "User-Agent", "-"),
		time.Since(request.Start).Seconds(),
		"-")) // TODO: "additional info", probably saved in request?
}

func (server ProxyHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/healthcheck" {
		writer.Header().Set("Content-Length", "2")
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("OK"))
		return
	}
	parts := strings.SplitN(request.URL.Path, "/", 5)
	vars := make(map[string]string)
	if len(parts) > 2 {
		vars["account"] = parts[2]
		if len(parts) > 3 {
			vars["container"] = parts[3]
			if len(parts) > 4 {
				vars["obj"] = parts[4]
			}
		}
	}
	newWriter := &hummingbird.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
	newRequest := &hummingbird.WebRequest{
		Request:       request,
		TransactionId: hummingbird.GetTransactionId(),
		XTimestamp:    hummingbird.GetTimestamp(),
		Start:         time.Now(),
		Logger:        server.logger}
	defer newRequest.LogPanics(newWriter)
	defer server.LogRequest(newWriter, newRequest) // log the request after return

	if len(parts) >= 1 && parts[1] == "auth" {
		server.AuthHandler(newWriter, newRequest, vars)
		return
	} else if val := request.Header.Get("X-Auth-Token"); val == "" {
		http.Error(writer, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	key := fmt.Sprintf("auth/%s/%s", vars["account"], request.Header.Get("X-Auth-Token"))
	it, err := server.mc.Get(key)
	if err != nil || string(it.Value) != "VALID" {
		http.Error(writer, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	if len(parts) == 5 && parts[1] == "v1" {
		switch request.Method {
		case "GET":
			server.ObjectGetHandler(newWriter, newRequest, vars)
		case "HEAD":
			server.ObjectGetHandler(newWriter, newRequest, vars)
		case "PUT":
			server.ObjectPutHandler(newWriter, newRequest, vars)
		case "DELETE":
			server.ObjectDeleteHandler(newWriter, newRequest, vars)
		}
	} else if len(parts) == 4 && parts[1] == "v1" {
		switch request.Method {
		case "GET":
			server.ContainerGetHandler(newWriter, newRequest, vars)
		case "HEAD":
			server.ContainerGetHandler(newWriter, newRequest, vars)
		case "PUT":
			server.ContainerPutHandler(newWriter, newRequest, vars)
		case "DELETE":
			server.ContainerDeleteHandler(newWriter, newRequest, vars)
		}
	} else if len(parts) == 3 && parts[1] == "v1" {
		switch request.Method {
		case "GET":
			server.AccountGetHandler(newWriter, newRequest, vars)
		case "HEAD":
			server.AccountGetHandler(newWriter, newRequest, vars)
		case "PUT":
			server.AccountPutHandler(newWriter, newRequest, vars)
		case "DELETE":
			server.AccountDeleteHandler(newWriter, newRequest, vars)
		}
	} else {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	}
}

func GetServer(conf string) (string, int, http.Handler, *syslog.Writer, error) {
	handler := ProxyHandler{}

	transport := http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
	}

	handler.client = &http.Client{Transport: &transport, Timeout: 30 * time.Second}
	handler.mc = memcache.New("127.0.0.1:11211")
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return "", 0, nil, nil, err
	}

	serverconf, err := hummingbird.LoadIniFile(conf)
	if err != nil {
		return "", 0, nil, nil, errors.New(fmt.Sprintf("Unable to load %s", conf))
	}

	bindIP := serverconf.GetDefault("DEFAULT", "bind_ip", "0.0.0.0")
	bindPort := serverconf.GetInt("DEFAULT", "bind_port", 8080)

	handler.logger = hummingbird.SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "proxy-server")
	handler.objectRing, err = hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix)
	handler.containerRing, err = hummingbird.GetRing("container", hashPathPrefix, hashPathSuffix)
	handler.accountRing, err = hummingbird.GetRing("account", hashPathPrefix, hashPathSuffix)
	if err != nil {
		return "", 0, nil, nil, err
	}

	return bindIP, int(bindPort), handler, handler.logger, nil
}
