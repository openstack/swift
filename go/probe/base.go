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

package probe

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/objectserver"
)

// FakeRing simulates a ring with 4 nodes and one partition.  That partition always lives on nodes 0, 1, and 2, with node 3 being the handoff.
type FakeRing struct {
	devices []*hummingbird.Device
}

func (r *FakeRing) GetNodes(partition uint64) (response []*hummingbird.Device) {
	return r.devices[0:3]
}

func (r *FakeRing) GetJobNodes(partition uint64, localDevice int) (devs []*hummingbird.Device, handoff bool) {
	switch localDevice {
	case 0:
		return []*hummingbird.Device{r.devices[1], r.devices[2]}, false
	case 1:
		return []*hummingbird.Device{r.devices[0], r.devices[2]}, false
	case 2:
		return []*hummingbird.Device{r.devices[0], r.devices[1]}, false
	default:
		return r.devices[0:3], true
	}
}

func (r *FakeRing) GetPartition(account string, container string, object string) uint64 {
	return 0
}

func (r *FakeRing) LocalDevices(localPort int) (devs []*hummingbird.Device, err error) {
	for _, d := range r.devices {
		if d.Port == localPort {
			return []*hummingbird.Device{d}, nil
		}
	}
	return nil, nil
}

func (r *FakeRing) AllDevices() (devs []hummingbird.Device) {
	return nil
}

type fakeMoreNodes struct {
	dev *hummingbird.Device
}

func (m *fakeMoreNodes) Next() *hummingbird.Device {
	return m.dev
}

func (r *FakeRing) GetMoreNodes(partition uint64) hummingbird.MoreNodes {
	return &fakeMoreNodes{r.devices[3]}
}

// Environment encapsulates a temporary SAIO-style environment for the object server, replicator, and auditor
// and provides a few utility functions for manipulating it.
type Environment struct {
	driveRoots             []string
	servers                []*httptest.Server
	ports                  []int
	hosts                  []string
	replicators            []*objectserver.Replicator
	auditors               []*objectserver.AuditorDaemon
	ring                   hummingbird.Ring
	hashPrefix, hashSuffix string
}

// Close frees any resources associated with the Environment.
func (e *Environment) Close() {
	for _, s := range e.servers {
		s.Close()
	}
	for _, s := range e.driveRoots {
		os.RemoveAll(s)
	}
}

// FileLocations returns a list of file paths for the object's hash directory on all three underlying object servers.
func (e *Environment) FileLocations(account, container, obj string) (paths []string) {
	partition := e.ring.GetPartition(account, container, obj)
	vars := map[string]string{"account": account, "container": container, "obj": obj, "partition": strconv.Itoa(int(partition)), "device": "sda"}
	for i := 0; i < 4; i++ {
		path := objectserver.ObjHashDir(vars, e.driveRoots[i], e.hashPrefix, e.hashSuffix)
		paths = append(paths, path)
	}
	return
}

// PutObject uploads an object "/a/c/o" to the indicated server with X-Timestamp set to timestamp and body set to data.
func (e *Environment) PutObject(server int, timestamp string, data string) bool {
	body := bytes.NewBuffer([]byte(data))
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", e.hosts[server], e.ports[server]), body)
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", strconv.Itoa(len(data)))
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	return err == nil && resp.StatusCode == 201
}

// ObjExists returns a boolean indicating that it can fetch the named object and that its X-Timestamp matches the timestamp argument.
func (e *Environment) ObjExists(server int, timestamp string) bool {
	req, err := http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", e.hosts[server], e.ports[server]), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		return false
	}
	return resp.Header.Get("X-Timestamp") == timestamp
}

// NewEnvironment creates a new environment.  Arguments should be a series of key, value pairs that are added to the object server configuration file.
func NewEnvironment(settings ...string) *Environment {
	env := &Environment{ring: &FakeRing{devices: nil}}
	env.hashPrefix, env.hashSuffix, _ = hummingbird.GetHashPrefixAndSuffix()
	for i := 0; i < 4; i++ {
		driveRoot, _ := ioutil.TempDir("", "")
		os.MkdirAll(filepath.Join(driveRoot, "sda", "objects"), 0755)
		ts := httptest.NewServer(nil)
		u, _ := url.Parse(ts.URL)
		host, ports, _ := net.SplitHostPort(u.Host)
		port, _ := strconv.Atoi(ports)

		conf, _ := ioutil.TempFile("", "")
		conf.WriteString("[DEFAULT]\nmount_check=false\n")
		fmt.Fprintf(conf, "devices=%s\n", driveRoot)
		fmt.Fprintf(conf, "bind_port=%d\n", port)
		fmt.Fprintf(conf, "bind_ip=%s\n", host)
		for i := 0; i < len(settings); i += 2 {
			fmt.Fprintf(conf, "%s=%s\n", settings[i], settings[i+1])
		}
		conf.WriteString("[app:object-server]\n")
		conf.WriteString("[object-replicator]\n")
		conf.WriteString("[object-auditor]\n")
		defer conf.Close()
		defer os.RemoveAll(conf.Name())

		_, _, server, _, _ := objectserver.GetServer(conf.Name(), &flag.FlagSet{})
		ts.Config.Handler = server.GetHandler()
		replicator, _ := objectserver.NewReplicator(conf.Name(), &flag.FlagSet{})
		auditor, _ := objectserver.NewAuditor(conf.Name(), &flag.FlagSet{})
		replicator.(*objectserver.Replicator).Ring = env.ring
		env.ring.(*FakeRing).devices = append(env.ring.(*FakeRing).devices, &hummingbird.Device{
			Id: i, Device: "sda", Ip: host, Port: port, Region: 0, ReplicationIp: host, ReplicationPort: port, Weight: 1, Zone: i,
		})

		env.driveRoots = append(env.driveRoots, driveRoot)
		env.servers = append(env.servers, ts)
		env.ports = append(env.ports, port)
		env.hosts = append(env.hosts, host)
		env.replicators = append(env.replicators, replicator.(*objectserver.Replicator))
		env.auditors = append(env.auditors, auditor.(*objectserver.AuditorDaemon))
	}
	return env
}
