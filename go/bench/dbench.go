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

package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

type DirectObject struct {
	Url  string
	Data []byte
}

func (obj *DirectObject) Put() bool {
	req, _ := http.NewRequest("PUT", obj.Url, bytes.NewReader(obj.Data))
	req.Header.Set("Content-Length", strconv.FormatInt(int64(len(obj.Data)), 10))
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(obj.Data))
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	if err != nil {
		fmt.Println("failed Put: ", err)
	}
	return err == nil && resp.StatusCode/100 == 2
}

func (obj *DirectObject) Get() bool {
	req, _ := http.NewRequest("GET", obj.Url, nil)
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		io.Copy(ioutil.Discard, resp.Body)
	}
	if err != nil {
		fmt.Println("failed Get: ", err)
	}
	return err == nil && resp.StatusCode/100 == 2
}

func (obj *DirectObject) Replicate() bool {
	req, _ := http.NewRequest("REPLICATE", obj.Url, nil)
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		io.Copy(ioutil.Discard, resp.Body)
	}
	return err == nil && resp.StatusCode/100 == 2
}

func (obj *DirectObject) Delete() bool {
	req, _ := http.NewRequest("DELETE", obj.Url, nil)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	if err != nil {
		fmt.Println("failed Delete: ", err)
	}
	return err == nil && resp.StatusCode/100 == 2
}

func GetDevices(address string, checkMounted bool) []string {
	deviceUrl := fmt.Sprintf("%srecon/diskusage", address)
	req, err := http.NewRequest("GET", deviceUrl, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(fmt.Sprintf("ERROR GETTING DEVICES: %s", err))
		os.Exit(1)
	}
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	var rdata interface{}
	json.Unmarshal(body, &rdata)
	retvals := []string{}
	for _, v := range rdata.([]interface{}) {
		val := v.(map[string]interface{})
		if !checkMounted || val["mounted"].(bool) {
			retvals = append(retvals, val["device"].(string))
		}
	}
	return retvals
}

func RunDBench(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: [configuration file]")
		fmt.Println("The configuration file should look something like:")
		fmt.Println("    [dbench]")
		fmt.Println("    address = http://localhost:6010/")
		fmt.Println("    concurrency = 15")
		fmt.Println("    object_size = 131072")
		fmt.Println("    num_objects = 5000")
		fmt.Println("    num_gets = 30000")
		fmt.Println("    do_replicates = false")
		fmt.Println("    delete = yes")
		fmt.Println("    minimum_partition_number = 1000000000")
		fmt.Println("    check_mounted = false")
		fmt.Println("    #drive_list = sdb1,sdb2")
		os.Exit(1)
	}

	benchconf, err := hummingbird.LoadIniFile(args[0])
	if err != nil {
		fmt.Println("Error parsing ini file:", err)
		os.Exit(1)
	}

	address := benchconf.GetDefault("dbench", "address", "http://localhost:6010/")
	if !strings.HasSuffix(address, "/") {
		address = address + "/"
	}
	concurrency := int(benchconf.GetInt("dbench", "concurrency", 16))
	objectSize := benchconf.GetInt("dbench", "object_size", 131072)
	numObjects := benchconf.GetInt("dbench", "num_objects", 5000)
	numGets := benchconf.GetInt("dbench", "num_gets", 30000)
	doReplicates := benchconf.GetBool("dbench", "do_replicates", false)
	checkMounted := benchconf.GetBool("dbench", "check_mounted", false)
	driveList := benchconf.GetDefault("dbench", "drive_list", "")
	numPartitions := int64(100)
	minPartition := benchconf.GetInt("dbench", "minimum_partition_number", 1000000000)
	delete := benchconf.GetBool("dbench", "delete", true)

	deviceList := GetDevices(address, checkMounted)
	if driveList != "" {
		deviceList = strings.Split(driveList, ",")
	}

	data := make([]byte, objectSize)
	objects := make([]*DirectObject, numObjects)
	deviceParts := make(map[string]bool)
	for i, _ := range objects {
		device := strings.Trim(deviceList[i%len(deviceList)], " ")
		part := rand.Int63()%numPartitions + minPartition
		objects[i] = &DirectObject{
			Url:  fmt.Sprintf("%s%s/%d/%s/%s/%d", address, device, part, "a", "c", rand.Int63()),
			Data: data,
		}
		deviceParts[fmt.Sprintf("%s/%d", device, part)] = true
	}

	work := make([]func() bool, len(objects))
	for i, _ := range objects {
		work[i] = objects[i].Put
	}
	DoJobs("PUT", work, concurrency)

	time.Sleep(time.Second * 2)

	replWork := make([]func() bool, 0)
	for replKey := range deviceParts {
		devicePart := strings.Split(replKey, "/")
		replWork = append(replWork, (&DirectObject{Url: fmt.Sprintf("%s%s/%s", address, devicePart[0], devicePart[1])}).Replicate)
	}
	if doReplicates {
		DoJobs("REPLICATE", replWork, concurrency)
	}

	work = make([]func() bool, numGets)
	for i := int64(0); i < numGets; i++ {
		work[i] = objects[int(rand.Int63()%int64(len(objects)))].Get
	}
	DoJobs("GET", work, concurrency)

	if delete {
		work = make([]func() bool, len(objects))
		for i, _ := range objects {
			work[i] = objects[i].Delete
		}
		DoJobs("DELETE", work, concurrency)
	}

	if doReplicates {
		DoJobs("REPLICATE", replWork, concurrency)
	}
}
