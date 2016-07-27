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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

/*This hash is used to represent a zero byte async file that is
  created for an expiring object*/
const zeroByteHash = "d41d8cd98f00b204e9800998ecf8427e"
const deleteAtAccount = ".expiring_objects"

func headerToMap(headers http.Header) map[string]string {
	ret := make(map[string]string)
	for key, value := range headers {
		if len(value) > 0 {
			ret[key] = headers.Get(key)
		}
	}
	return ret
}

func splitHeader(header string) []string {
	if header == "" {
		return []string{}
	}
	return strings.Split(header, ",")
}

func (server *ObjectServer) hashPath(account, container, obj string) string {
	h := md5.New()
	io.WriteString(h, server.hashPathPrefix+"/"+account+"/"+container+"/"+obj+server.hashPathSuffix)
	return hex.EncodeToString(h.Sum(nil))
}

func (server *ObjectServer) expirerContainer(deleteAt time.Time, account, container, obj string) string {
	i := new(big.Int)
	fmt.Sscanf(server.hashPath(account, container, obj), "%x", i)
	shardInt := i.Mod(i, big.NewInt(100)).Int64()
	timestamp := (deleteAt.Unix()/server.expiringDivisor)*server.expiringDivisor - shardInt
	if timestamp < 0 {
		timestamp = 0
	} else if timestamp > 9999999999 {
		timestamp = 9999999999
	}
	return fmt.Sprintf("%010d", timestamp)
}

func (server *ObjectServer) sendContainerUpdate(host, device, method, partition, account, container, obj string, headers http.Header) bool {
	obj_url := fmt.Sprintf("http://%s/%s/%s/%s/%s/%s", host, device, partition,
		hummingbird.Urlencode(account), hummingbird.Urlencode(container), hummingbird.Urlencode(obj))
	if req, err := http.NewRequest(method, obj_url, nil); err == nil {
		req.Header = headers
		if resp, err := server.updateClient.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return true
			}
		}
	}
	return false
}

func (server *ObjectServer) saveAsync(method, account, container, obj, localDevice string, headers http.Header) {
	hash := server.hashPath(account, container, obj)
	asyncFile := filepath.Join(server.driveRoot, localDevice, "async_pending", hash[29:32], hash+"-"+headers.Get("X-Timestamp"))
	tempDir := TempDirPath(server.driveRoot, localDevice)
	data := map[string]interface{}{
		"op":        method,
		"account":   account,
		"container": container,
		"obj":       obj,
		"headers":   headerToMap(headers),
	}
	if os.MkdirAll(filepath.Dir(asyncFile), 0755) == nil {
		writer, err := NewAtomicFileWriter(tempDir, filepath.Dir(asyncFile))
		if err == nil {
			defer writer.Abandon()
			writer.Write(hummingbird.PickleDumps(data))
			writer.Save(asyncFile)
		}
	}
}

func (server *ObjectServer) updateContainer(metadata map[string]string, request *http.Request, vars map[string]string, logger hummingbird.LoggingContext) {
	partition := request.Header.Get("X-Container-Partition")
	hosts := splitHeader(request.Header.Get("X-Container-Host"))
	devices := splitHeader(request.Header.Get("X-Container-Device"))
	if partition == "" || len(hosts) == 0 || len(devices) == 0 {
		return
	}
	requestHeaders := http.Header{
		"X-Backend-Storage-Policy-Index": {hummingbird.GetDefault(request.Header, "X-Backend-Storage-Policy-Index", "0")},
		"Referer":                        {hummingbird.GetDefault(request.Header, "Referer", "-")},
		"User-Agent":                     {hummingbird.GetDefault(request.Header, "User-Agent", "-")},
		"X-Trans-Id":                     {hummingbird.GetDefault(request.Header, "X-Trans-Id", "-")},
		"X-Timestamp":                    {request.Header.Get("X-Timestamp")},
	}
	if request.Method != "DELETE" {
		requestHeaders.Add("X-Content-Type", metadata["Content-Type"])
		requestHeaders.Add("X-Size", metadata["Content-Length"])
		requestHeaders.Add("X-Etag", metadata["ETag"])
	}
	failures := 0
	for index := range hosts {
		if !server.sendContainerUpdate(hosts[index], devices[index], request.Method, partition, vars["account"], vars["container"], vars["obj"], requestHeaders) {
			logger.LogError("ERROR container update failed with %s/%s (saving for async update later)", hosts[index], devices[index])
			failures++
		}
	}
	if failures > 0 {
		server.saveAsync(request.Method, vars["account"], vars["container"], vars["obj"], vars["device"], requestHeaders)
	}
}

func (server *ObjectServer) updateDeleteAt(request *http.Request, deleteAtStr string, vars map[string]string, logger hummingbird.LoggingContext) {
	deleteAt, err := hummingbird.ParseDate(deleteAtStr)
	if err != nil {
		return
	}
	container := hummingbird.GetDefault(request.Header, "X-Delete-At-Container", "")
	if container == "" {
		container = server.expirerContainer(deleteAt, vars["account"], vars["container"], vars["obj"])
	}
	obj := fmt.Sprintf("%010d-%s/%s/%s", deleteAt.Unix(), vars["account"], vars["container"], vars["obj"])
	partition := hummingbird.GetDefault(request.Header, "X-Delete-At-Partition", "")
	hosts := splitHeader(request.Header.Get("X-Delete-At-Host"))
	devices := splitHeader(request.Header.Get("X-Delete-At-Device"))
	requestHeaders := http.Header{
		"X-Backend-Storage-Policy-Index": {hummingbird.GetDefault(request.Header, "X-Backend-Storage-Policy-Index", "0")},
		"Referer":                        {hummingbird.GetDefault(request.Header, "Referer", "-")},
		"User-Agent":                     {hummingbird.GetDefault(request.Header, "User-Agent", "-")},
		"X-Trans-Id":                     {hummingbird.GetDefault(request.Header, "X-Trans-Id", "-")},
		"X-Timestamp":                    {request.Header.Get("X-Timestamp")},
	}
	if request.Method != "DELETE" {
		requestHeaders.Add("X-Content-Type", "text/plain")
		requestHeaders.Add("X-Size", "0")
		requestHeaders.Add("X-Etag", zeroByteHash)
	}
	failures := 0
	for index := range hosts {
		if !server.sendContainerUpdate(hosts[index], devices[index], request.Method, partition, deleteAtAccount, container, obj, requestHeaders) {
			logger.LogError("ERROR container update failed with %s/%s (saving for async update later)", hosts[index], devices[index])
			failures++
		}
	}
	if failures > 0 || len(hosts) == 0 {
		server.saveAsync(request.Method, deleteAtAccount, container, obj, vars["device"], requestHeaders)
	}
}

func (server *ObjectServer) containerUpdates(request *http.Request, metadata map[string]string, deleteAt string, vars map[string]string, logger hummingbird.LoggingContext) {
	defer logger.LogPanics("PANIC WHILE UPDATING CONTAINER LISTINGS")
	if deleteAt != "" {
		go server.updateDeleteAt(request, deleteAt, vars, logger)
	}

	firstDone := make(chan struct{}, 1)
	go func() {
		server.updateContainer(metadata, request, vars, logger)
		firstDone <- struct{}{}
	}()
	select {
	case <-firstDone:
	case <-time.After(server.updateTimeout):
	}
}
