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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/stretchr/testify/require"
)

type priFakeRing struct {
	mapping map[uint64][]int
}

func (p *priFakeRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	isaHandoff := false
	if localDevice == 0 {
		response = append(response, &hummingbird.Device{Id: 1, Device: "drive1", Ip: "127.0.0.1", Port: 1})
		response = append(response, &hummingbird.Device{Id: 2, Device: "drive2", Ip: "127.0.0.1", Port: 1})
		isaHandoff = true
	} else {
		response = append(response, &hummingbird.Device{Id: localDevice%2 + 1, Device: fmt.Sprintf("drive%d", localDevice%2+1), Ip: "127.0.0.1", Port: 1})
	}
	return response, isaHandoff
}

func (p *priFakeRing) GetPartition(account string, container string, object string) uint64 { return 0 }

func (p *priFakeRing) LocalDevices(localPort int) (devs []*hummingbird.Device, err error) {
	return nil, nil
}

func (p *priFakeRing) AllDevices() (devs []hummingbird.Device) {
	devs = append(devs, hummingbird.Device{Id: 0, Device: "drive0", Ip: "127.0.0.0", Port: 1})
	devs = append(devs, hummingbird.Device{Id: 1, Device: "drive1", Ip: "127.0.0.1", Port: 1})
	devs = append(devs, hummingbird.Device{Id: 2, Device: "drive2", Ip: "127.0.0.1", Port: 1})
	return devs
}

func (p *priFakeRing) GetMoreNodes(partition uint64) hummingbird.MoreNodes { return nil }

func (p *priFakeRing) GetNodes(partition uint64) (response []*hummingbird.Device) {
	for _, p := range p.mapping[partition] {
		response = append(response, &hummingbird.Device{Id: p, Device: fmt.Sprintf("drive%d", p), Ip: "127.0.0.1", Port: p})
	}
	return
}

func (p *priFakeRing) GetNodesInOrder(partition uint64) (response []*hummingbird.Device) {
	return p.GetNodes(partition)
}

func TestGetPartMoveJobs(t *testing.T) {
	t.Parallel()
	oldRing := &priFakeRing{
		mapping: map[uint64][]int{
			0: {1, 2, 3, 4, 5},
			1: {6, 7, 8, 9, 10},
		},
	}
	newRing := &priFakeRing{
		mapping: map[uint64][]int{
			0: {6, 2, 3, 4, 5},
			1: {6, 7, 8, 9, 11},
		},
	}
	jobs := getPartMoveJobs(oldRing, newRing)
	require.EqualValues(t, 2, len(jobs))
	require.EqualValues(t, 0, jobs[0].Partition)
	require.EqualValues(t, 1, jobs[0].FromDevice.Id)
	require.EqualValues(t, 6, jobs[0].ToDevices[0].Id)
	require.EqualValues(t, 1, jobs[1].Partition)
	require.EqualValues(t, 10, jobs[1].FromDevice.Id)
	require.EqualValues(t, 11, jobs[1].ToDevices[0].Id)
}

func TestGetRestoreDeviceJobs(t *testing.T) {
	t.Parallel()
	ring := &priFakeRing{
		mapping: map[uint64][]int{
			0: {1, 2},
			1: {1, 3},
		},
	}
	jobs := getRestoreDeviceJobs(ring, "127.0.0.1", "drive1")
	require.EqualValues(t, 2, len(jobs))
	require.EqualValues(t, 0, jobs[0].Partition)
	require.EqualValues(t, 2, jobs[0].FromDevice.Id)
	require.EqualValues(t, 1, jobs[0].ToDevices[0].Id)
	require.EqualValues(t, 1, jobs[1].Partition)
	require.EqualValues(t, 3, jobs[1].FromDevice.Id)
	require.EqualValues(t, 1, jobs[1].ToDevices[0].Id)
}

func TestPriRepJobs(t *testing.T) {
	t.Parallel()
	handlerRan := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerRan = true
		require.Equal(t, "/priorityrep", r.URL.Path)
		var pri PriorityRepJob
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		if err := json.Unmarshal(data, &pri); err != nil {
			w.WriteHeader(400)
			return
		}
		require.EqualValues(t, 0, pri.Partition)
		require.EqualValues(t, "sda", pri.FromDevice.Device)
		require.EqualValues(t, 1, len(pri.ToDevices))
		require.EqualValues(t, "sdb", pri.ToDevices[0].Device)
	}))

	defer ts.Close()
	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(ports)
	jobs := []*PriorityRepJob{
		&PriorityRepJob{
			Partition:  0,
			FromDevice: &hummingbird.Device{Device: "sda", Ip: host, Port: port - 500, ReplicationIp: host, ReplicationPort: port - 500},
			ToDevices: []*hummingbird.Device{
				&hummingbird.Device{Device: "sdb"},
			},
		},
	}
	doPriRepJobs(jobs, 2, http.DefaultClient)
	require.Equal(t, true, handlerRan)
}

func TestDevLimiter(t *testing.T) {
	t.Parallel()
	job1 := &PriorityRepJob{
		FromDevice: &hummingbird.Device{Id: 0},
		ToDevices:  []*hummingbird.Device{&hummingbird.Device{Id: 1, Device: "sdb"}},
	}
	job2 := &PriorityRepJob{
		FromDevice: &hummingbird.Device{Id: 1},
		ToDevices:  []*hummingbird.Device{&hummingbird.Device{Id: 2, Device: "sdb"}},
	}
	job3 := &PriorityRepJob{
		FromDevice: &hummingbird.Device{Id: 1},
		ToDevices:  []*hummingbird.Device{&hummingbird.Device{Id: 0, Device: "sdb"}},
	}
	limiter := &devLimiter{inUse: make(map[int]int), max: 2, somethingFinished: make(chan struct{}, 1)}
	require.True(t, limiter.start(job1))
	require.True(t, limiter.start(job2))
	require.False(t, limiter.start(job3))
	limiter.finished(job1)
	require.True(t, limiter.start(job3))
}

func TestGetRescuePartsJobs(t *testing.T) {
	t.Parallel()
	objRing := &priFakeRing{
		mapping: map[uint64][]int{
			0: {1, 2, 3},
			1: {6, 7, 8},
		},
	}
	jobs := getRescuePartsJobs(objRing, []uint64{1})
	require.EqualValues(t, 3, len(jobs))

	require.EqualValues(t, 0, jobs[0].FromDevice.Id)
	require.EqualValues(t, 1, jobs[0].ToDevices[0].Id)
	require.EqualValues(t, 2, jobs[0].ToDevices[1].Id)
	require.EqualValues(t, 1, jobs[0].Partition)

	require.EqualValues(t, 1, len(jobs[1].ToDevices))
	require.EqualValues(t, 1, jobs[1].FromDevice.Id)
	require.EqualValues(t, 2, jobs[1].ToDevices[0].Id)
	require.EqualValues(t, 1, jobs[1].Partition)

	require.EqualValues(t, 1, len(jobs[2].ToDevices))
	require.EqualValues(t, 2, jobs[2].FromDevice.Id)
	require.EqualValues(t, 1, jobs[2].ToDevices[0].Id)
	require.EqualValues(t, 1, jobs[2].Partition)
}
