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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

type devLimiter struct {
	inUse             map[int]int
	m                 sync.Mutex
	max               int
	somethingFinished chan struct{}
}

func (d *devLimiter) start(j *PriorityRepJob) bool {
	d.m.Lock()
	doable := d.inUse[j.FromDevice.Id] < d.max
	for _, dev := range j.ToDevices {
		doable = doable && d.inUse[dev.Id] < d.max
	}
	if doable {
		d.inUse[j.FromDevice.Id] += 1
		for _, dev := range j.ToDevices {
			d.inUse[dev.Id] += 1
		}
	}
	d.m.Unlock()
	return doable
}

func (d *devLimiter) finished(j *PriorityRepJob) {
	d.m.Lock()
	d.inUse[j.FromDevice.Id] -= 1
	for _, dev := range j.ToDevices {
		d.inUse[dev.Id] -= 1
	}
	d.m.Unlock()
	select {
	case d.somethingFinished <- struct{}{}:
	default:
	}
}

func (d *devLimiter) waitForSomethingToFinish() {
	<-d.somethingFinished
}

// doPriRepJobs executes a list of PriorityRepJobs, limiting concurrent jobs per device to deviceMax.
func doPriRepJobs(jobs []*PriorityRepJob, deviceMax int, client *http.Client) {
	limiter := &devLimiter{inUse: make(map[int]int), max: deviceMax, somethingFinished: make(chan struct{}, 1)}
	wg := sync.WaitGroup{}
	for len(jobs) > 0 {
		foundDoable := false
		for i := range jobs {
			if !limiter.start(jobs[i]) {
				continue
			}
			foundDoable = true
			wg.Add(1)
			go func(job *PriorityRepJob) {
				defer wg.Done()
				defer limiter.finished(job)
				url := fmt.Sprintf("http://%s:%d/priorityrep", job.FromDevice.ReplicationIp, job.FromDevice.ReplicationPort+500)
				jsonned, err := json.Marshal(job)
				if err != nil {
					fmt.Println("Failed to serialize job for some reason:", err)
					return
				}
				req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonned))
				if err != nil {
					fmt.Println("Failed to create request for some reason:", err)
					return
				}
				req.ContentLength = int64(len(jsonned))
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					fmt.Printf("Error moving partition %d: %v\n", job.Partition, err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode/100 != 2 {
					fmt.Printf("Bad status code moving partition %d: %d\n", job.Partition, resp.StatusCode)
				} else {
					fmt.Printf("Replicating partition %d from %s/%s\n", job.Partition, job.FromDevice.Ip, job.FromDevice.Device)
				}
			}(jobs[i])
			jobs = append(jobs[:i], jobs[i+1:]...)
			break
		}
		if !foundDoable {
			limiter.waitForSomethingToFinish()
		}
	}
	wg.Wait()
}

// getPartMoveJobs takes two rings and creates a list of jobs for any partition moves between them.
func getPartMoveJobs(oldRing, newRing hummingbird.Ring) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	for partition := uint64(0); true; partition++ {
		olddevs := oldRing.GetNodesInOrder(partition)
		newdevs := newRing.GetNodesInOrder(partition)
		if olddevs == nil || newdevs == nil {
			break
		}
		for i := range olddevs {
			if olddevs[i].Id != newdevs[i].Id {
				// TODO: handle if a node just changes positions, which doesn't happen, but isn't against the contract.
				jobs = append(jobs, &PriorityRepJob{
					Partition:  partition,
					FromDevice: olddevs[i],
					ToDevices:  []*hummingbird.Device{newdevs[i]},
				})
			}
		}
	}
	return jobs
}

// MoveParts takes two object .ring.gz files as []string{oldRing, newRing} and dispatches priority replication jobs to rebalance data in line with any ring changes.
func MoveParts(args []string) {
	if len(args) != 1 {
		fmt.Println("USAGE: hummingbird moveparts [old ringfile]")
		return
	}
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return
	}
	oldRing, err := hummingbird.LoadRing(args[0], hashPathPrefix, hashPathSuffix)
	if err != nil {
		fmt.Println("Unable to load old ring:", err)
		return
	}
	curRing, err := hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix)
	if err != nil {
		fmt.Println("Unable to load current ring:", err)
		return
	}
	client := &http.Client{Timeout: time.Hour}
	jobs := getPartMoveJobs(oldRing, curRing)
	fmt.Println("Job count:", len(jobs))
	doPriRepJobs(jobs, 2, client)
	fmt.Println("Done sending jobs.")
}

// getRestoreDeviceJobs takes an ip address and device name, and creates a list of jobs to restore that device's data from peers.
func getRestoreDeviceJobs(ring hummingbird.Ring, ip string, devName string) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	for partition := uint64(0); true; partition++ {
		devs := ring.GetNodesInOrder(partition)
		if devs == nil {
			break
		}
		for i, dev := range devs {
			if dev.Device == devName && (dev.Ip == ip || dev.ReplicationIp == ip) {
				src := devs[(i+1)%len(devs)]
				jobs = append(jobs, &PriorityRepJob{
					Partition:  partition,
					FromDevice: src,
					ToDevices:  []*hummingbird.Device{dev},
				})
			}
		}
	}
	return jobs
}

// RestoreDevice takes an IP address and device name such as []string{"172.24.0.1", "sda1"} and attempts to restores its data from peers.
func RestoreDevice(args []string) {
	if len(args) != 2 {
		fmt.Println("USAGE: hummingbird restoredevice [ip] [device]")
		return
	}
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return
	}
	objRing, err := hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix)
	if err != nil {
		fmt.Println("Unable to load ring:", err)
		return
	}
	client := &http.Client{Timeout: time.Hour}
	jobs := getRestoreDeviceJobs(objRing, args[0], args[1])
	fmt.Println("Job count:", len(jobs))
	doPriRepJobs(jobs, 2, client)
	fmt.Println("Done sending jobs.")
}

func getRescuePartsJobs(objRing hummingbird.Ring, partitions []uint64) []*PriorityRepJob {
	jobs := make([]*PriorityRepJob, 0)
	allDevices := objRing.AllDevices()
	for d := range allDevices {
		for _, p := range partitions {
			nodes, _ := objRing.GetJobNodes(p, allDevices[d].Id)
			jobs = append(jobs, &PriorityRepJob{
				Partition:  p,
				FromDevice: &allDevices[d],
				ToDevices:  nodes,
			})
		}
	}
	return jobs
}

func RescueParts(args []string) {
	if len(args) != 1 {
		fmt.Println("USAGE: hummingbird rescueparts partnum1,partnum2,...")
		return
	}
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		fmt.Println("Unable to load hash path prefix and suffix:", err)
		return
	}
	objRing, err := hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix)
	if err != nil {
		fmt.Println("Unable to load ring:", err)
		return
	}
	partsStr := strings.Split(args[0], ",")
	partsInt := make([]uint64, len(partsStr))
	for i, p := range partsStr {
		partsInt[i], err = strconv.ParseUint(p, 10, 64)
		if err != nil {
			fmt.Println("Invalid Partition:", p)
			return
		}
	}
	client := &http.Client{Timeout: time.Hour}
	jobs := getRescuePartsJobs(objRing, partsInt)
	fmt.Println("Job count:", len(jobs))
	doPriRepJobs(jobs, 1, client)
	fmt.Println("Done sending jobs.")
}
