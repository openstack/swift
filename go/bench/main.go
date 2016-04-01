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
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/openstack/swift/go/client"
	"github.com/openstack/swift/go/hummingbird"
)

type Object struct {
	c         client.Client
	state     int
	container string
	name      string
	data      []byte
}

func (obj *Object) Put() bool {
	return obj.c.PutObject(obj.container, obj.name, nil, bytes.NewReader(obj.data)) == nil
}

func (obj *Object) Get() bool {
	if r, _, err := obj.c.GetObject(obj.container, obj.name, nil); err != nil {
		return false
	} else {
		io.Copy(ioutil.Discard, r)
		r.Close()
		return true
	}
}

func (obj *Object) Delete() bool {
	return obj.c.DeleteObject(obj.container, obj.name, nil) == nil
}

func DoJobs(name string, work []func() bool, concurrency int) {
	wg := sync.WaitGroup{}
	cwg := sync.WaitGroup{}
	errorCount := 0
	jobTimes := make([]float64, 0, len(work))
	times := make(chan float64)
	errors := make(chan int)
	jobqueue := make(chan func() bool)
	cwg.Add(2)
	go func() {
		for n := range errors {
			errorCount += n
		}
		cwg.Done()
	}()
	go func() {
		for t := range times {
			jobTimes = append(jobTimes, t)
		}
		sort.Float64s(jobTimes)
		cwg.Done()
	}()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for job := range jobqueue {
				startJob := time.Now()
				if !job() {
					errors <- 1
				}
				times <- float64(time.Now().Sub(startJob)) / float64(time.Second)
			}
			wg.Done()
		}()
	}
	start := time.Now()
	for _, job := range work {
		jobqueue <- job
	}
	close(jobqueue)
	wg.Wait()
	totalTime := float64(time.Now().Sub(start)) / float64(time.Second)
	close(errors)
	close(times)
	cwg.Wait()
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
	fmt.Println("  Failures:", errorCount)
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

	var cli client.Client
	if allowInsecureAuthCert {
		cli, err = client.NewInsecureClient("", authUser, "", authKey, "", authURL, false)
	} else {
		cli, err = client.NewClient("", authUser, "", authKey, "", authURL, false)
	}
	if err != nil {
		fmt.Println("Error creating client:", err)
		os.Exit(1)
	}

	for i := 0; i < concurrency; i++ {
		if err := cli.PutContainer(fmt.Sprintf("%d-%s", i, salt), nil); err != nil {
			fmt.Println("Error putting container:", err)
			os.Exit(1)
		}
	}

	data := make([]byte, objectSize)
	objects := make([]*Object, numObjects)
	for i, _ := range objects {
		objects[i] = &Object{
			state:     0,
			container: fmt.Sprintf("%d-%s", i%concurrency, salt),
			name:      fmt.Sprintf("%x", rand.Int63()),
			data:      data,
			c:         cli,
		}
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
	salt := fmt.Sprintf("%d", rand.Int63())

	var cli client.Client
	if allowInsecureAuthCert {
		cli, err = client.NewInsecureClient("", authUser, "", authKey, "", authURL, false)
	} else {
		cli, err = client.NewClient("", authUser, "", authKey, "", authURL, false)
	}
	if err != nil {
		fmt.Println("Error creating client:", err)
		os.Exit(1)
	}

	for i := 0; i < concurrency; i++ {
		if err := cli.PutContainer(fmt.Sprintf("%d-%s", i, salt), nil); err != nil {
			fmt.Println("Error putting container:", err)
			os.Exit(1)
		}
	}

	data := make([]byte, objectSize)
	objects := make([]*Object, numObjects)
	for i, _ := range objects {
		objects[i] = &Object{
			state:     0,
			container: fmt.Sprintf("%d-%s", i%concurrency, salt),
			name:      fmt.Sprintf("%x", rand.Int63()),
			data:      data,
			c:         cli,
		}
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
		if objects[i].state == 1 {
			workch <- objects[i].Put
		} else if objects[i].state < numGets+2 {
			workch <- objects[i].Get
		} else if objects[i].state >= numGets+2 {
			workch <- objects[i].Delete
			objects[i] = &Object{
				container: fmt.Sprintf("%d-%s", i%concurrency, salt),
				name:      fmt.Sprintf("%x", rand.Int63()),
				data:      data,
				c:         cli,
			}
			continue
		}
		objects[i].state += 1
	}
}
