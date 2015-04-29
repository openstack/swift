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
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/openstack/swift/go/hummingbird"
)

type replicationLogSaver struct {
	logged []string
}

func (s *replicationLogSaver) Err(l string) error {
	s.logged = append(s.logged, l)
	return nil
}

func (s *replicationLogSaver) Info(l string) error {
	s.logged = append(s.logged, l)
	return nil
}

func (s *replicationLogSaver) Debug(l string) error {
	s.logged = append(s.logged, l)
	return nil
}

type FakeRing struct{}

func (r *FakeRing) GetNodes(partition uint64) (response []*hummingbird.Device) {
	return nil
}

func (r *FakeRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return nil, false
}

func (r *FakeRing) GetPartition(account string, container string, object string) uint64 {
	return 0
}

func (r *FakeRing) LocalDevices(localPort int) (devs []*hummingbird.Device) {
	return nil
}

func (r *FakeRing) GetMoreNodes(partition uint64) hummingbird.MoreNodes {
	return nil
}

func makeReplicator(settings ...string) *Replicator {
	conf, _ := ioutil.TempFile("", "")
	conf.WriteString("[object-replicator]\n")
	for i := 0; i < len(settings); i += 2 {
		fmt.Fprintf(conf, "%s=%s\n", settings[i], settings[i+1])
	}
	defer conf.Close()
	defer os.RemoveAll(conf.Name())
	replicator, _ := NewReplicator(conf.Name())
	replicator.(*Replicator).jobChan = make(chan *job, 1)
	return replicator.(*Replicator)
}

func newServer(handler http.Handler) (ts *httptest.Server, host string, port int) {
	ts = httptest.NewServer(handler)
	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ = strconv.Atoi(ports)
	return ts, host, port
}

func TestReplicatorSyncFile(t *testing.T) {
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := ioutil.ReadAll(r.Body)
		assert.Equal(t, r.ContentLength, int64(len("hello there!")))
		assert.Equal(t, string(data), "hello there!")
		assert.NotEqual(t, "", r.Header.Get("X-Attrs"))
		assert.Equal(t, "12345", r.Header.Get("X-Replication-Id"))
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	replicator := makeReplicator("devices", "")
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	file.WriteString("hello there!")
	WriteMetadata(file.Fd(), map[string]string{"name": "something"})
	assert.True(t, replicator.syncFile(file.Name(), filepath.Join("1", "fff", "ffffffffffffffffffffffffffffffff", "12345.678.data"), dev, "12345"))
}

func TestReplicatorSyncFileFails(t *testing.T) {
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	replicator := makeReplicator("devices", "")
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	file.WriteString("hello there!")
	WriteMetadata(file.Fd(), map[string]string{"name": "something"})
	assert.False(t, replicator.syncFile(file.Name(), filepath.Join("1", "fff", "ffffffffffffffffffffffffffffffff", "12345.678.data"), dev, "12345"))
}

func TestReplicatorSyncFileInvalidFile(t *testing.T) {
	dev := &hummingbird.Device{ReplicationIp: "127.0.0.1", ReplicationPort: 23456, Device: "sda"}
	replicator := makeReplicator("devices", "")
	assert.False(t, replicator.syncFile(filepath.Join("some", "nonexistent", "file"), filepath.Join("1", "fff", "ffffffffffffffffffffffffffffffff", "12345.678.data"), dev, "12345"))
}

func TestReplicatorSyncFileNotAFile(t *testing.T) {
	dev := &hummingbird.Device{ReplicationIp: "127.0.0.1", ReplicationPort: 23456, Device: "sda"}
	replicator := makeReplicator("devices", "")
	assert.False(t, replicator.syncFile("/", filepath.Join("1", "fff", "ffffffffffffffffffffffffffffffff", "12345.678.data"), dev, "12345"))
}

func TestReplicatorSyncFileNoMetadata(t *testing.T) {
	dev := &hummingbird.Device{ReplicationIp: "127.0.0.1", ReplicationPort: 23456, Device: "sda"}
	replicator := makeReplicator("devices", "")
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	file.WriteString("hello there!")
	assert.False(t, replicator.syncFile(file.Name(), filepath.Join("1", "fff", "ffffffffffffffffffffffffffffffff", "12345.678.data"), dev, "12345"))
}

func TestReplicatorSyncFileNoServer(t *testing.T) {
	dev := &hummingbird.Device{ReplicationIp: "127.0.0.1", ReplicationPort: 23456, Device: "sda"}
	replicator := makeReplicator("devices", "")
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	file.WriteString("hello there!")
	WriteMetadata(file.Fd(), map[string]string{"name": "something"})
	assert.False(t, replicator.syncFile(file.Name(), filepath.Join("1", "fff", "ffffffffffffffffffffffffffffffff", "12345.678.data"), dev, "12345"))
}

func TestReplicatorGetRemoteHashesWorks(t *testing.T) {
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "12345", r.Header.Get("X-Replication-Id"))
		assert.Equal(t, "/sda/1", r.URL.Path)
		assert.Equal(t, "REPLICATE", r.Method)
		w.WriteHeader(200)
		w.Write(hummingbird.PickleDumps(map[string]string{"abc": "somehash"}))
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	replicator := makeReplicator("devices", "")
	hashes, unmounted := replicator.getRemoteHashes(dev, "1", "12345")
	assert.False(t, unmounted)
	assert.IsType(t, map[interface{}]interface{}{}, hashes)
	assert.Equal(t, hashes["abc"], "somehash")
}

func TestReplicatorGetRemoteHashesUnmounted(t *testing.T) {
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(507)
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	replicator := makeReplicator("devices", "")
	_, unmounted := replicator.getRemoteHashes(dev, "1", "12345")
	assert.True(t, unmounted)
}

func TestReplicatorGetRemoteHashesNoServer(t *testing.T) {
	dev := &hummingbird.Device{ReplicationIp: "127.0.0.1", ReplicationPort: 23456, Device: "sda"}
	replicator := makeReplicator("devices", "")
	hashes, unmounted := replicator.getRemoteHashes(dev, "1", "12345")
	assert.False(t, unmounted)
	assert.Nil(t, hashes)
}

func TestReplicatorGetRemoteHashesInvalidPickle(t *testing.T) {
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("THIS IS NOT A PICKLE"))
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	replicator := makeReplicator("devices", "")
	hashes, unmounted := replicator.getRemoteHashes(dev, "1", "12345")
	assert.False(t, unmounted)
	assert.Nil(t, hashes)
}

func TestReplicatorEndReplication(t *testing.T) {
	called := false
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "REPLICATE", r.Method)
		assert.Equal(t, "12345", r.Header.Get("X-Replication-Id"))
		assert.Equal(t, "/sda/1/end", r.URL.Path)
		called = true
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	replicator := makeReplicator("devices", "")
	replicator.endReplication(dev, "1", "12345")
	assert.True(t, called)
}

func setupDirectory() string {
	dir, _ := ioutil.TempDir("", "")
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "00000000000000000000000000000abc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"name": "/a/c/o", "Content-Length": "0", "Content-Type": "text/plain", "X-Timestamp": "12345.00000", "ETag": ""})
	f, _ = os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "00000000000000000000000000000abc", "67890.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"name": "/a/c/o2", "Content-Length": "0", "Content-Type": "text/plain", "X-Timestamp": "12345.00000", "ETag": ""})
	return dir
}

func TestReplicatorReplicateLocal(t *testing.T) {
	started := false
	ended := false
	sync1 := false
	sync2 := false
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotEqual(t, r.Header.Get("X-Replication-Id"), "")
		if r.Method == "REPLICATE" && r.URL.Path == "/sda/1" {
			started = true
		} else if r.Method == "REPLICATE" && r.URL.Path == "/sda/1/end" {
			ended = true
		} else if r.Method == "SYNC" && r.URL.Path == "/sda/objects/1/abc/00000000000000000000000000000abc/67890.data" {
			assert.NotEqual(t, r.Header.Get("X-Attrs"), "")
			sync1 = true
		} else if r.Method == "SYNC" && r.URL.Path == "/sda/objects/1/abc/fffffffffffffffffffffffffffffabc/12345.data" {
			assert.NotEqual(t, r.Header.Get("X-Attrs"), "")
			sync2 = true
		}
		w.Write(hummingbird.PickleDumps(map[string]string{"abc": "somehash"}))
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("devices", driveRoot)
	replicator.replicateLocal(&job{partition: "1", dev: dev, objPath: driveRoot + "/" + dev.Device + "/objects"}, []*hummingbird.Device{dev}, nil)
	assert.True(t, started)
	assert.True(t, ended)
	assert.True(t, sync1)
	assert.True(t, sync2)
}

type FakeMoreNodes struct {
	host string
	port int
}

func (f FakeMoreNodes) Next() *hummingbird.Device {
	return &hummingbird.Device{ReplicationIp: f.host, ReplicationPort: f.port, Device: "sdb"}
}

func TestReplicatorReplicateLocalUsesHandoff(t *testing.T) {
	sdbHits := 0
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotEqual(t, r.Header.Get("X-Replication-Id"), "")
		if r.Method == "REPLICATE" && strings.HasPrefix(r.URL.Path, "/sda") {
			w.WriteHeader(507)
		} else if r.Method == "REPLICATE" && r.URL.Path == "/sdb/1" {
			sdbHits++
			w.WriteHeader(200)
			w.Write(hummingbird.PickleDumps(map[string]string{"abc": "somehash"}))
		}
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("devices", driveRoot)
	replicator.replicateLocal(&job{partition: "1", dev: dev, objPath: driveRoot + "/" + dev.Device + "/objects"},
		[]*hummingbird.Device{dev, dev, dev}, &FakeMoreNodes{host, port})
	assert.Equal(t, 3, sdbHits)
}

func TestReplicatorReplicateHandoffSyncPass(t *testing.T) {
	started := false
	ended := false
	sync1 := false
	sync2 := false
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotEqual(t, r.Header.Get("X-Replication-Id"), "")
		if r.Method == "REPLICATE" && r.URL.Path == "/sda/1" {
			started = true
		} else if r.Method == "REPLICATE" && r.URL.Path == "/sda/1/end" {
			ended = true
		} else if r.Method == "SYNC" && r.URL.Path == "/sda/objects/1/abc/00000000000000000000000000000abc/67890.data" {
			assert.NotEqual(t, r.Header.Get("X-Attrs"), "")
			sync1 = true
		} else if r.Method == "SYNC" && r.URL.Path == "/sda/objects/1/abc/fffffffffffffffffffffffffffffabc/12345.data" {
			assert.NotEqual(t, r.Header.Get("X-Attrs"), "")
			sync2 = true
		}
		w.Write(hummingbird.PickleDumps(map[string]string{"abc": "somehash"}))
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("devices", driveRoot)
	assert.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data")))
	assert.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "00000000000000000000000000000abc", "67890.data")))
	replicator.replicateHandoff(&job{partition: "1", dev: dev, objPath: filepath.Join(driveRoot, dev.Device, "objects")}, []*hummingbird.Device{dev})
	assert.True(t, started)
	assert.True(t, ended)
	assert.True(t, sync1)
	assert.True(t, sync2)
	assert.False(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data")))
	assert.False(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "00000000000000000000000000000abc", "67890.data")))
}

func TestReplicatorReplicateHandoffSyncFail(t *testing.T) {
	started := false
	ended := false
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotEqual(t, r.Header.Get("X-Replication-Id"), "")
		if r.Method == "REPLICATE" && r.URL.Path == "/sda/1" {
			started = true
		} else if r.Method == "REPLICATE" && r.URL.Path == "/sda/1/end" {
			ended = true
		} else if r.Method == "SYNC" {
			w.WriteHeader(500)
		}
		w.Write(hummingbird.PickleDumps(map[string]string{"abc": "somehash"}))
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("devices", driveRoot)
	replicator.replicateHandoff(&job{partition: "1", dev: dev, objPath: filepath.Join(driveRoot, dev.Device, "objects")}, []*hummingbird.Device{dev})
	assert.True(t, started)
	assert.True(t, ended)
	assert.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data")))
	assert.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "objects", "1", "abc", "00000000000000000000000000000abc", "67890.data")))
}

func TestCleanTemp(t *testing.T) {
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	os.MkdirAll(filepath.Join(driveRoot, "sda", "tmp"), 0777)
	ioutil.WriteFile(filepath.Join(driveRoot, "sda", "tmp", "oldfile"), []byte(""), 0666)
	ioutil.WriteFile(filepath.Join(driveRoot, "sda", "tmp", "newfile"), []byte(""), 0666)
	os.Chtimes(filepath.Join(driveRoot, "sda", "tmp", "oldfile"), time.Now().Add(0-48*time.Hour), time.Now().Add(0-48*time.Hour))
	replicator := makeReplicator("devices", driveRoot)
	dev := &hummingbird.Device{ReplicationIp: "", ReplicationPort: 0, Device: "sda"}
	replicator.cleanTemp(dev)
	assert.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "tmp", "newfile")))
	assert.False(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "tmp", "oldfile")))
}

func TestReplicatorReportStats(t *testing.T) {
	saved := &replicationLogSaver{}
	replicator := makeReplicator("devices", os.TempDir(), "ms_per_part", "1", "concurrency", "3")
	replicator.logger = saved
	reportStats := func(t time.Time) string {
		c := make(chan time.Time)
		done := make(chan bool)
		go func() {
			replicator.statsReporter(c)
			done <- true
		}()
		replicator.jobCountIncrement <- 100
		replicator.replicationCountIncrement <- 50
		replicator.partitionTimesAdd <- 10.0
		replicator.partitionTimesAdd <- 20.0
		replicator.partitionTimesAdd <- 15.0
		c <- t
		close(c)
		<-done
		return saved.logged[len(saved.logged)-1]
	}
	var remaining int
	var elapsed, rate float64
	replicator.startTime = time.Now()
	reportStats(time.Now().Add(100 * time.Second))
	cnt, err := fmt.Sscanf(saved.logged[0], "50/100 (50.00%%) partitions replicated in %fs (%f/sec, %dm remaining)", &elapsed, &rate, &remaining)
	assert.Nil(t, err)
	assert.Equal(t, 3, cnt)
	assert.Equal(t, 2, remaining)
	assert.Equal(t, saved.logged[1], "Partition times: max 20.0000s, min 10.0000s, med 15.0000s")
}

type FakeLocalRing struct {
	*FakeRing
	dev *hummingbird.Device
}

func (r *FakeLocalRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return []*hummingbird.Device{r.dev}, false
}

func TestReplicatorReplicateDeviceLocal(t *testing.T) {
	called := false
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "REPLICATE" {
			called = true
		}
		w.WriteHeader(500)
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("devices", driveRoot, "mount_check", "false")
	replicator.partGroup.Add(1)
	replicator.devGroup.Add(1)
	go replicator.partitionReplicator()
	statsTicker := time.NewTicker(StatsReportInterval)
	defer statsTicker.Stop()
	go replicator.statsReporter(statsTicker.C)
	replicator.partRateTicker = time.NewTicker(1)
	defer replicator.partRateTicker.Stop()
	replicator.Ring = &FakeLocalRing{dev: dev}
	replicator.replicateDevice(dev)
	close(replicator.jobChan)
	replicator.partGroup.Wait()
	assert.Equal(t, uint64(1), replicator.replicationCount)
	assert.True(t, called)
}

type FakeHandoffRing struct {
	*FakeRing
	dev *hummingbird.Device
}

func (r *FakeHandoffRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return []*hummingbird.Device{r.dev}, true
}

func TestReplicatorReplicateDeviceHandoff(t *testing.T) {
	called := false
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "REPLICATE" {
			called = true
		}
		w.WriteHeader(500)
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("devices", driveRoot, "mount_check", "false")
	replicator.partGroup.Add(1)
	replicator.devGroup.Add(1)
	go replicator.partitionReplicator()
	statsTicker := time.NewTicker(StatsReportInterval)
	defer statsTicker.Stop()
	go replicator.statsReporter(statsTicker.C)
	replicator.partRateTicker = time.NewTicker(1)
	defer replicator.partRateTicker.Stop()
	replicator.Ring = &FakeHandoffRing{dev: dev}
	replicator.replicateDevice(dev)
	close(replicator.jobChan)
	replicator.partGroup.Wait()
	assert.Equal(t, uint64(1), replicator.replicationCount)
	assert.True(t, called)
}

type FakeRunRing struct {
	*FakeRing
	dev *hummingbird.Device
}

func (r *FakeRunRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return []*hummingbird.Device{r.dev}, true
}

func (r *FakeRunRing) LocalDevices(localPort int) (devs []*hummingbird.Device) {
	return []*hummingbird.Device{r.dev}
}

func TestReplicatorRun(t *testing.T) {
	called := false
	ts, host, port := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "REPLICATE" {
			called = true
		}
		w.WriteHeader(500)
	}))
	defer ts.Close()
	dev := &hummingbird.Device{ReplicationIp: host, ReplicationPort: port, Device: "sda"}
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("devices", driveRoot, "mount_check", "false")
	replicator.partRateTicker = time.NewTicker(1)
	defer replicator.partRateTicker.Stop()
	replicator.Ring = &FakeRunRing{dev: dev}
	replicator.partitionTimes = append(replicator.partitionTimes, 10.0)
	replicator.Run()
	assert.Equal(t, uint64(1), replicator.replicationCount)
	assert.True(t, called)
}

func TestReplicatorInitFail(t *testing.T) {
	replicator, err := NewReplicator("nonexistentfile")
	assert.Nil(t, replicator)
	assert.NotNil(t, err)
}

func TestReplicatorVmDuration(t *testing.T) {
	replicator := makeReplicator("vm_test_mode", "true")
	assert.Equal(t, 2000*time.Millisecond, replicator.timePerPart)
}

type repmanLogSaver struct {
	logged []string
}

func (s *repmanLogSaver) Err(val string) error {
	s.logged = append(s.logged, val)
	return nil
}

func TestReplicationManagerLocalLimit(t *testing.T) {
	repman := NewReplicationManager(2, 10)
	assert.True(t, repman.BeginReplication("sda", "12345"))
	assert.True(t, repman.BeginReplication("sda", "12346"))
	assert.False(t, repman.BeginReplication("sda", "12347"))
	_, ok := repman.inUse["sda"]["12345"]
	assert.True(t, ok)
	_, ok = repman.inUse["sda"]["12346"]
	assert.True(t, ok)
	_, ok = repman.inUse["sda"]["12347"]
	assert.False(t, ok)
}

func TestReplicationManagerEjectsOld(t *testing.T) {
	repman := NewReplicationManager(2, 10)
	assert.True(t, repman.BeginReplication("sda", "12345"))
	repman.inUse["sda"]["12345"] = time.Now().Add(0 - (ReplicationSessionTimeout + 1))
	assert.True(t, repman.BeginReplication("sda", "12346"))
	assert.True(t, repman.BeginReplication("sda", "12347"))
}

func TestReplicationManagerGlobalLimit(t *testing.T) {
	repman := NewReplicationManager(2, 10)
	for i := 0; i < 10; i++ {
		assert.True(t, repman.BeginReplication(fmt.Sprintf("sda%d", i), fmt.Sprintf("1234%d", i)))
	}
	assert.False(t, repman.BeginReplication("sda10", "1234567"))
}

func TestReplicationManagerDone(t *testing.T) {
	repman := NewReplicationManager(2, 10)
	assert.True(t, repman.BeginReplication("sda", "12345"))
	repman.Done("sda", "12345")
	assert.True(t, repman.BeginReplication("sda", "12346"))
	assert.True(t, repman.BeginReplication("sda", "12347"))
}

func TestReplicationManagerUpdateSession(t *testing.T) {
	repman := NewReplicationManager(2, 10)
	assert.True(t, repman.BeginReplication("sda", "12345"))
	wasTime := repman.inUse["sda"]["12345"]
	repman.UpdateSession("sda", "12345")
	assert.True(t, repman.inUse["sda"]["12345"].Sub(wasTime) > 0)
}
