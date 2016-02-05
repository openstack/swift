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
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

var client = &http.Client{}

var storageURL = ""
var authToken = ""

func Auth(endpoint string, user string, key string, allowInsecureAuthCert bool) (string, string) {
	req, err := http.NewRequest("GET", endpoint, nil)
	req.Header.Set("X-Auth-User", user)
	req.Header.Set("X-Auth-Key", key)
	authClient := &http.Client{}
	if allowInsecureAuthCert {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		authClient = &http.Client{Transport: tr}
	}
	resp, err := authClient.Do(req)
	if err != nil {
		fmt.Println("ERROR MAKING AUTH REQUEST", err)
		os.Exit(1)
	}
	resp.Body.Close()
	return resp.Header.Get("X-Storage-Url"), resp.Header.Get("X-Auth-Token")
}

func PutContainers(storageURL string, authToken string, count int, salt string) {
	for i := 0; i < count; i++ {
		url := fmt.Sprintf("%s/%d-%s", storageURL, i, salt)
		req, _ := http.NewRequest("PUT", url, nil)
		req.Header.Set("X-Auth-Token", authToken)
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode/100 != 2 {
			fmt.Println("ERROR CREATING CONTAINERS", resp.StatusCode)
			os.Exit(1)
		}
		resp.Body.Close()
	}
}

type Object struct {
	Url   string
	Data  []byte
	Id    int
	State int
}

func (obj *Object) Put() bool {
	req, _ := http.NewRequest("PUT", obj.Url, bytes.NewReader(obj.Data))
	req.Header.Set("X-Auth-Token", authToken)
	req.Header.Set("Content-Length", strconv.FormatInt(int64(len(obj.Data)), 10))
	req.ContentLength = int64(len(obj.Data))
	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	return err == nil && resp.StatusCode/100 == 2
}

func (obj *Object) Get() bool {
	req, _ := http.NewRequest("GET", obj.Url, nil)
	req.Header.Set("X-Auth-Token", authToken)
	resp, err := client.Do(req)
	if resp != nil {
		io.Copy(ioutil.Discard, resp.Body)
	}
	return err == nil && resp.StatusCode/100 == 2
}

func (obj *Object) Delete() bool {
	req, _ := http.NewRequest("DELETE", obj.Url, nil)
	req.Header.Set("X-Auth-Token", authToken)
	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	return err == nil && resp.StatusCode/100 == 2
}

func DoJobs(name string, work []func() bool, concurrency int) {
	wg := sync.WaitGroup{}
	starterPistol := make(chan int)
	jobId := int32(-1)
	errors := 0
	wg.Add(concurrency)
	jobTimes := make([]float64, len(work))
	for i := 0; i < concurrency; i++ {
		go func() {
			_, _ = <-starterPistol
			for {
				job := int(atomic.AddInt32(&jobId, 1))
				if job >= len(work) {
					wg.Done()
					return
				}
				startJob := time.Now()
				result := work[job]()
				jobTimes[job] = float64(time.Now().Sub(startJob)) / float64(time.Second)
				if !result {
					errors += 1
				}
			}
		}()
	}
	start := time.Now()
	close(starterPistol)
	wg.Wait()
	totalTime := float64(time.Now().Sub(start)) / float64(time.Second)

	sort.Float64s(jobTimes)
	sum := 0.0
	for _, val := range jobTimes {
		sum += val
	}
	avg := sum / float64(len(work))
	diffsum := 0.0
	for _, val := range jobTimes {
		diffsum += math.Pow(val-avg, 2.0)
	}
	fmt.Printf("%ss: %d @ %.2f/s\n", name, len(work), float64(len(work))/totalTime)
	fmt.Println("  Failures:", errors)
	fmt.Printf("  Mean: %.5fs (%.1f%% RSD)\n", avg, math.Sqrt(diffsum/float64(len(work)))*100.0/avg)
	fmt.Printf("  Median: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.5)])
	fmt.Printf("  85%%: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.85)])
	fmt.Printf("  90%%: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.90)])
	fmt.Printf("  95%%: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.95)])
	fmt.Printf("  99%%: %.5fs\n", jobTimes[int(float64(len(jobTimes))*0.99)])
}

func RunBench(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: [configuration file]")
		fmt.Println("Only supports auth 1.0.")
		fmt.Println("The configuration file should look something like:")
		fmt.Println("    [bench]")
		fmt.Println("    auth = http://localhost:8080/auth/v1.0")
		fmt.Println("    user = test:tester")
		fmt.Println("    key = testing")
		fmt.Println("    concurrency = 15")
		fmt.Println("    object_size = 131072")
		fmt.Println("    num_objects = 5000")
		fmt.Println("    num_gets = 30000")
		fmt.Println("    delete = yes")
		fmt.Println("    allow_insecure_auth_cert = no")
		os.Exit(1)
	}

	benchconf, err := hummingbird.LoadIniFile(args[0])
	if err != nil {
		fmt.Println("Error parsing ini file:", err)
		os.Exit(1)
	}

	authURL := benchconf.GetDefault("bench", "auth", "http://localhost:8080/auth/v1.0")
	authUser := benchconf.GetDefault("bench", "user", "test:tester")
	authKey := benchconf.GetDefault("bench", "key", "testing")
	concurrency := int(benchconf.GetInt("bench", "concurrency", 16))
	objectSize := benchconf.GetInt("bench", "object_size", 131072)
	numObjects := benchconf.GetInt("bench", "num_objects", 5000)
	numGets := benchconf.GetInt("bench", "num_gets", 30000)
	delete := benchconf.GetBool("bench", "delete", true)
	allowInsecureAuthCert := benchconf.GetBool("bench", "allow_insecure_auth_cert", false)
	salt := fmt.Sprintf("%d", rand.Int63())

	storageURL, authToken = Auth(authURL, authUser, authKey, allowInsecureAuthCert)

	PutContainers(storageURL, authToken, concurrency, salt)

	data := make([]byte, objectSize)
	objects := make([]Object, numObjects)
	for i, _ := range objects {
		objects[i].Url = fmt.Sprintf("%s/%d-%s/%d", storageURL, i%concurrency, salt, rand.Int63())
		objects[i].Data = data
		objects[i].Id = i
	}

	work := make([]func() bool, len(objects))
	for i, _ := range objects {
		work[i] = objects[i].Put
	}
	DoJobs("PUT", work, concurrency)

	time.Sleep(time.Second * 2)

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
}

func RunThrash(args []string) {
	rand.Seed(time.Now().UTC().UnixNano())
	if len(args) < 1 {
		fmt.Println("Usage: [configuration file]")
		fmt.Println("Only supports auth 1.0.")
		fmt.Println("The configuration file should look something like:")
		fmt.Println("    [thrash]")
		fmt.Println("    auth = http://localhost:8080/auth/v1.0")
		fmt.Println("    user = test:tester")
		fmt.Println("    key = testing")
		fmt.Println("    concurrency = 15")
		fmt.Println("    object_size = 131072")
		fmt.Println("    num_objects = 5000")
		fmt.Println("    gets_per_object = 5")
		fmt.Println("    allow_insecure_auth_cert = no")
		os.Exit(1)
	}

	thrashconf, err := hummingbird.LoadIniFile(args[0])
	if err != nil {
		fmt.Println("Error parsing ini file:", err)
		os.Exit(1)
	}

	authURL := thrashconf.GetDefault("thrash", "auth", "http://localhost:8080/auth/v1.0")
	authUser := thrashconf.GetDefault("thrash", "user", "test:tester")
	authKey := thrashconf.GetDefault("thrash", "key", "testing")
	concurrency := int(thrashconf.GetInt("thrash", "concurrency", 16))
	objectSize := thrashconf.GetInt("thrash", "object_size", 131072)
	numObjects := thrashconf.GetInt("thrash", "num_objects", 5000)
	numGets := int(thrashconf.GetInt("thrash", "gets_per_object", 5))
	allowInsecureAuthCert := thrashconf.GetBool("bench", "allow_insecure_auth_cert", false)

	storageURL, authToken = Auth(authURL, authUser, authKey, allowInsecureAuthCert)

	salt := fmt.Sprintf("%d", rand.Int63())

	PutContainers(storageURL, authToken, concurrency, salt)

	data := make([]byte, objectSize)
	objects := make([]*Object, numObjects)
	for i, _ := range objects {
		objects[i] = &Object{}
		objects[i].Url = fmt.Sprintf("%s/%d-%s/%d", storageURL, i%concurrency, salt, rand.Int63())
		objects[i].Data = data
		objects[i].Id = i
		objects[i].State = 1
	}

	workch := make(chan func() bool)

	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				(<-workch)()
			}
		}()
	}

	for {
		i := int(rand.Int63() % int64(len(objects)))
		if objects[i].State == 1 {
			workch <- objects[i].Put
		} else if objects[i].State < numGets+2 {
			workch <- objects[i].Get
		} else if objects[i].State >= numGets+2 {
			workch <- objects[i].Delete
			objects[i] = &Object{Url: fmt.Sprintf("%s/%d-%s/%d", storageURL, i%concurrency, salt, rand.Int63()), Data: data, Id: i, State: 1}
			continue
		}
		objects[i].State += 1
	}
}
