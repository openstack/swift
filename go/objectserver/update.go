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
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

const deleteAtDivisor = 3600
const deleteAtAccount = ".expiring_objects"

/*This hash is used to represent a zero byte async file that is
  created for an expiring object*/
const zeroByteHash = "d41d8cd98f00b204e9800998ecf8427e"

var client = &http.Client{Timeout: time.Second * 10}

func HeaderToMap(headers http.Header) map[string]string {
	ret := make(map[string]string)
	for key, value := range headers {
		if len(value) > 0 {
			ret[key] = value[0]
		}
	}
	return ret
}

func UpdateContainer(metadata map[string]string, request *hummingbird.WebRequest, vars map[string]string, hashDir string) {
	contpartition := request.Header.Get("X-Container-Partition")
	if contpartition == "" {
		return
	}
	conthosts := strings.Split(request.Header.Get("X-Container-Host"), ",")
	contdevices := strings.Split(request.Header.Get("X-Container-Device"), ",")
	for index := range conthosts {
		if conthosts[index] == "" {
			break
		}
		host := conthosts[index]
		device := contdevices[index]
		obj_url := fmt.Sprintf("http://%s/%s/%s/%s/%s/%s", host, device, contpartition,
			hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
		req, err := http.NewRequest(request.Method, obj_url, nil)
		if err != nil {
			continue
		}
		req.Header.Add("User-Agent", hummingbird.GetDefault(request.Header, "User-Agent", "-"))
		referer := hummingbird.GetDefault(request.Header, "Referer", "-")
		if len(referer) > 1 {
			split_ref := strings.Split(referer, " ")
			ref_url, err := url.Parse(split_ref[1])
			if err == nil {
				split_ref[1] = ref_url.String()
			}
			referer = strings.Join(split_ref, " ")
		}
		req.Header.Add("Referer", referer)
		req.Header.Add("X-Trans-Id", hummingbird.GetDefault(request.Header, "X-Trans-Id", "-"))
		req.Header.Add("X-Timestamp", metadata["X-Timestamp"])
		if request.Method != "DELETE" {
			req.Header.Add("X-Content-Type", metadata["Content-Type"])
			req.Header.Add("X-Size", metadata["Content-Length"])
			req.Header.Add("X-Etag", metadata["ETag"])
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		if err != nil || (resp.StatusCode/100) != 2 {
			request.LogError("Container update failed with %s/%s, saving async", host, device)
			data := map[string]interface{}{
				"op":        request.Method,
				"account":   vars["account"],
				"container": vars["container"],
				"obj":       vars["obj"],
				"headers":   HeaderToMap(req.Header),
			}
			suffDir, hash := filepath.Split(hashDir)
			suffDir = filepath.Dir(hashDir)
			partitionDir, suff := filepath.Split(suffDir)
			partitionDir = filepath.Dir(suffDir)
			objDir := filepath.Dir(partitionDir)
			rootDir := filepath.Dir(objDir)
			asyncDir := filepath.Join(rootDir, "async_pending", suff)
			os.MkdirAll(asyncDir, 0700)
			asyncFile := filepath.Join(asyncDir, fmt.Sprintf("%s-%s", hash, request.XTimestamp))
			hummingbird.WriteFileAtomic(asyncFile, hummingbird.PickleDumps(data), 0600)
		}
	}
}

func UpdateDeleteAt(request *hummingbird.WebRequest, vars map[string]string, hashDir string) {
	deleteAt, err := hummingbird.ParseDate(request.Header.Get("X-Delete-At"))
	deleteAtContainer := deleteAt.Unix()
	partition := hummingbird.GetDefault(request.Header, "X-Delete-At-Partition", "")
	host := hummingbird.GetDefault(request.Header, "X-Delete-At-Host", "")
	device := hummingbird.GetDefault(request.Header, "X-Delete-At-Device", "")

	// TODO: do the thing where it subtracts a randomish number so it doesn't hammer the containers
	url := fmt.Sprintf("http://%s/%s/%s/%s/%d/%d-%s/%s/%s", host, device, partition, deleteAtAccount, deleteAtContainer,
		deleteAt.Unix(), hummingbird.Urlencode(vars["account"]), hummingbird.Urlencode(vars["container"]), hummingbird.Urlencode(vars["obj"]))
	if partition == "" || host == "" || device == "" {
		request.LogError(fmt.Sprintf("Trying to save an x-delete-at but did not send required headers: %s", url))
		return
	}
	req, err := http.NewRequest(request.Method, url, nil)
	req.Header.Add("X-Trans-Id", hummingbird.GetDefault(request.Header, "X-Trans-Id", "-"))
	req.Header.Add("X-Timestamp", request.Header.Get("X-Timestamp"))
	req.Header.Add("X-Size", "0")
	req.Header.Add("X-Content-Type", "text/plain")
	req.Header.Add("X-Etag", zeroByteHash)
	resp, err := client.Do(req)
	if err != nil && resp != nil {
		resp.Body.Close()
	}
	if err != nil || (resp.StatusCode/100) != 2 {
		expiredObjData := map[string]string{
			"account":   deleteAtAccount,
			"container": strconv.FormatInt(deleteAtContainer, 10),
			"obj":       strconv.FormatInt(deleteAtContainer, 10) + "-" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"],
			"device":    vars["device"],
			"partition": vars["partition"],
		}
		expiredHashDir := ObjHashDir(expiredObjData, vars["driveRoot"], vars["hashPathPrefix"], vars["hashPathSuffix"])
		headers := HeaderToMap(req.Header)
		headers["Referer"] = hummingbird.GetDefault(request.Header, "Referer", "-")
		headers["User-Agent"] = hummingbird.GetDefault(request.Header, "User-Agent", "-")

		data := map[string]interface{}{
			"op":      request.Method,
			"headers": headers,
		}
		for k, v := range expiredObjData {
			data[k] = v
		}

		suffDir, hash := filepath.Split(expiredHashDir)
		suffDir = filepath.Dir(expiredHashDir)
		partitionDir, suff := filepath.Split(suffDir)
		partitionDir = filepath.Dir(suffDir)
		objDir := filepath.Dir(partitionDir)
		rootDir := filepath.Dir(objDir)
		asyncDir := filepath.Join(rootDir, "async_pending", suff)
		os.MkdirAll(asyncDir, 0700)
		asyncFile := filepath.Join(asyncDir, fmt.Sprintf("%s-%s", hash, request.XTimestamp))
		hummingbird.WriteFileAtomic(asyncFile, hummingbird.PickleDumps(data), 0600)
	}
}
