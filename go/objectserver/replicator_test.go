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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func (r *FakeRing) GetNodesInOrder(partition uint64) (response []*hummingbird.Device) {
	return nil
}

func (r *FakeRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return nil, false
}

func (r *FakeRing) GetPartition(account string, container string, object string) uint64 {
	return 0
}

func (r *FakeRing) LocalDevices(localPort int) (devs []*hummingbird.Device, err error) {
	return nil, nil
}

func (r *FakeRing) AllDevices() (devs []hummingbird.Device) {
	return nil
}

func (r *FakeRing) GetMoreNodes(partition uint64) hummingbird.MoreNodes {
	return nil
}

func makeReplicator(settings ...string) (*Replicator, error) {
	return makeReplicatorWithFlags(settings, &flag.FlagSet{})
}

func makeReplicatorWithFlags(settings []string, flags *flag.FlagSet) (*Replicator, error) {
	configString := "[object-replicator]\nmount_check=false\n"
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	conf, _ := hummingbird.StringConfig(configString)
	replicator, err := NewReplicator(conf, flags)
	if err != nil {
		return nil, err
	}
	rep := replicator.(*Replicator)
	rep.concurrencySem = make(chan struct{}, 1)
	rep.LoopSleepTime = 0
	return rep, nil
}

func newServer(handler http.Handler) (ts *httptest.Server, host string, port int) {
	ts = httptest.NewServer(handler)
	u, _ := url.Parse(ts.URL)
	host, ports, _ := net.SplitHostPort(u.Host)
	port, _ = strconv.Atoi(ports)
	return ts, host, port
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

type FakeMoreNodes struct {
	host string
	port int
}

func (f FakeMoreNodes) Next() *hummingbird.Device {
	return &hummingbird.Device{ReplicationIp: f.host, ReplicationPort: f.port, Device: "sdb"}
}

func TestCleanTemp(t *testing.T) {
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	os.MkdirAll(filepath.Join(driveRoot, "sda", "tmp"), 0777)
	ioutil.WriteFile(filepath.Join(driveRoot, "sda", "tmp", "oldfile"), []byte(""), 0666)
	ioutil.WriteFile(filepath.Join(driveRoot, "sda", "tmp", "newfile"), []byte(""), 0666)
	os.Chtimes(filepath.Join(driveRoot, "sda", "tmp", "oldfile"), time.Now().Add(0-48*time.Hour), time.Now().Add(0-48*time.Hour))
	replicator, err := makeReplicator("devices", driveRoot)
	require.Nil(t, err)
	dev := &hummingbird.Device{ReplicationIp: "", ReplicationPort: 0, Device: "sda"}
	replicator.cleanTemp(dev)
	assert.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "tmp", "newfile")))
	assert.False(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "tmp", "oldfile")))
}

func TestReplicatorReportStatsNotSetup(t *testing.T) {
	saved := &replicationLogSaver{}
	replicator, err := makeReplicator("devices", os.TempDir(), "ms_per_part", "1", "concurrency", "3")
	require.Nil(t, err)
	replicator.logger = saved
	reportStats := func(t time.Time) string {
		c := make(chan time.Time)
		done := make(chan bool)
		go func() {
			replicator.statsReporter(c)
			done <- true
		}()
		replicator.deviceProgressIncr <- deviceProgress{
			dev:             &hummingbird.Device{Device: "sda"},
			PartitionsTotal: 12}

		c <- t
		close(c)
		<-done
		return saved.logged[len(saved.logged)-1]
	}
	reportStats(time.Now().Add(100 * time.Second))
	assert.Equal(t, "Trying to increment progress and not present: sda", saved.logged[0])
}

func TestReplicatorReportStats(t *testing.T) {
	saved := &replicationLogSaver{}
	replicator, err := makeReplicator("devices", os.TempDir(), "ms_per_part", "1", "concurrency", "3")
	require.Nil(t, err)
	replicator.logger = saved

	replicator.deviceProgressMap["sda"] = &deviceProgress{
		dev: &hummingbird.Device{Device: "sda"}}
	replicator.deviceProgressMap["sdb"] = &deviceProgress{
		dev: &hummingbird.Device{Device: "sdb"}}

	reportStats := func(t time.Time) string {
		c := make(chan time.Time)
		done := make(chan bool)
		go func() {
			replicator.statsReporter(c)
			done <- true
		}()
		replicator.deviceProgressPassInit <- deviceProgress{
			dev:             &hummingbird.Device{Device: "sda"},
			PartitionsTotal: 10}
		replicator.deviceProgressIncr <- deviceProgress{
			dev:            &hummingbird.Device{Device: "sda"},
			PartitionsDone: 10}

		c <- t
		close(c)
		<-done
		return saved.logged[len(saved.logged)-1]
	}
	reportStats(time.Now().Add(100 * time.Second))
	assert.Equal(t, replicator.deviceProgressMap["sda"].PartitionsDone, uint64(10))
	assert.NotEqual(t, strings.Index(saved.logged[0], "10/10 (100.00%) partitions replicated in"), -1)
}

type FakeLocalRing struct {
	*FakeRing
	dev *hummingbird.Device
}

func (r *FakeLocalRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return []*hummingbird.Device{r.dev}, false
}

type FakeHandoffRing struct {
	*FakeRing
	dev *hummingbird.Device
}

func (r *FakeHandoffRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return []*hummingbird.Device{r.dev}, true
}

func TestReplicatorVmDuration(t *testing.T) {
	replicator, err := makeReplicator("vm_test_mode", "true")
	require.Nil(t, err)
	assert.Equal(t, 2000*time.Millisecond, replicator.timePerPart)
}

type repmanLogSaver struct {
	logged []string
}

func (s *repmanLogSaver) Err(val string) error {
	s.logged = append(s.logged, val)
	return nil
}

func TestGetFile(t *testing.T) {
	replicator, err := makeReplicator()
	require.Nil(t, err)
	file, err := ioutil.TempFile("", "")
	assert.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())
	file.Write([]byte("SOME DATA"))
	WriteMetadata(file.Fd(), map[string]string{
		"ETag":           "662411c1698ecc13dd07aee13439eadc",
		"X-Timestamp":    "1234567890.12345",
		"Content-Length": "9",
		"name":           "some name",
	})

	fp, xattrs, size, err := replicator.getFile(file.Name())
	fp.Close()
	require.Equal(t, size, int64(9))
	require.True(t, len(xattrs) > 0)
	assert.Nil(t, err)
}

func TestGetFileBadFile(t *testing.T) {
	replicator, err := makeReplicator()
	require.Nil(t, err)
	_, _, _, err = replicator.getFile("somenonexistentfile")
	require.NotNil(t, err)

	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	_, _, _, err = replicator.getFile(dir)
	require.NotNil(t, err)

	file, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())
	_, _, _, err = replicator.getFile(file.Name())
	require.NotNil(t, err)
}

func TestGetFileBadMetadata(t *testing.T) {
	replicator, err := makeReplicator()
	require.Nil(t, err)
	file, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())

	require.Nil(t, RawWriteMetadata(file.Fd(), []byte("HI")))
	_, _, _, err = replicator.getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, RawWriteMetadata(file.Fd(), []byte("\x80\x02U\x02HIq\x01.")))
	_, _, _, err = replicator.getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, RawWriteMetadata(file.Fd(), []byte("\x80\x02}q\x01K\x00U\x02hiq\x02s.")))
	_, _, _, err = replicator.getFile(file.Name())
	require.NotNil(t, err)

	require.Nil(t, RawWriteMetadata(file.Fd(), []byte("\x80\x02}q\x01U\x02hiq\x02K\x00s.")))
	_, _, _, err = replicator.getFile(file.Name())
	require.NotNil(t, err)

	dfile, err := os.Create(file.Name() + ".data")
	require.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())
	require.Nil(t, WriteMetadata(dfile.Fd(), nil))
	_, _, _, err = replicator.getFile(dfile.Name())
	require.NotNil(t, err)

	tfile, err := os.Create(file.Name() + ".ts")
	require.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())
	require.Nil(t, WriteMetadata(tfile.Fd(), nil))
	_, _, _, err = replicator.getFile(tfile.Name())
	require.NotNil(t, err)

	dfile, err = os.Create(file.Name() + ".data")
	require.Nil(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())
	require.Nil(t, WriteMetadata(dfile.Fd(), nil))
	_, _, _, err = replicator.getFile(dfile.Name())
	require.NotNil(t, err)
}

type FakeRepRing1 struct {
	*FakeRing
	ldev, rdev *hummingbird.Device
}

func (r *FakeRepRing1) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return []*hummingbird.Device{r.rdev}, false
}

func (r *FakeRepRing1) LocalDevices(localPort int) (devs []*hummingbird.Device, err error) {
	return []*hummingbird.Device{r.ldev}, nil
}

func TestReplicationLocal(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts2.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	ldev := &hummingbird.Device{ReplicationIp: ts.host, ReplicationPort: ts.port, Device: "sda"}
	rdev := &hummingbird.Device{ReplicationIp: ts2.host, ReplicationPort: ts2.port, Device: "sda"}
	replicator, err := makeReplicator("bind_port", fmt.Sprintf("%d", ts.port))
	require.Nil(t, err)
	replicator.driveRoot = ts.objServer.driveRoot
	replicator.Rings[0] = &FakeRepRing1{ldev: ldev, rdev: rdev}
	replicator.Run()

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts2.host, ts2.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
}

type FakeRepRing2 struct {
	*FakeRing
	ldev, rdev *hummingbird.Device
}

func (r *FakeRepRing2) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return []*hummingbird.Device{r.rdev}, true
}

func (r *FakeRepRing2) LocalDevices(localPort int) (devs []*hummingbird.Device, err error) {
	return []*hummingbird.Device{r.ldev}, nil
}

func TestReplicationHandoff(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts2.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)

	ldev := &hummingbird.Device{ReplicationIp: ts.host, ReplicationPort: ts.port, Device: "sda"}
	rdev := &hummingbird.Device{ReplicationIp: ts2.host, ReplicationPort: ts2.port, Device: "sda"}
	replicator, err := makeReplicator("bind_port", fmt.Sprintf("%d", ts.port))
	require.Nil(t, err)
	replicator.driveRoot = ts.objServer.driveRoot
	replicator.Rings[0] = &FakeRepRing2{ldev: ldev, rdev: rdev}
	replicator.Run()

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts2.host, ts2.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 404, resp.StatusCode)
}

func TestReplicationHandoffQuorumDelete(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts2.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	ldev := &hummingbird.Device{ReplicationIp: ts.host, ReplicationPort: ts.port, Device: "sda"}
	rdev := &hummingbird.Device{ReplicationIp: ts2.host, ReplicationPort: ts2.port, Device: "sda"}
	flags := flag.NewFlagSet("hbird flags", flag.ContinueOnError)
	flags.Bool("q", false, "boolean value")
	flags.Parse([]string{})
	replicator, _ := makeReplicatorWithFlags([]string{"bind_port", fmt.Sprintf("%d", ts.port)}, flags)
	require.False(t, replicator.quorumDelete)

	flags.Parse([]string{"-q"})
	replicator, _ = makeReplicatorWithFlags([]string{"bind_port", fmt.Sprintf("%d", ts.port)}, flags)
	require.True(t, replicator.quorumDelete)
	replicator.driveRoot = ts.objServer.driveRoot
	replicator.Rings[0] = &FakeRepRing2{ldev: ldev, rdev: rdev}
	replicator.Run()

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts2.host, ts2.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
}

func TestListObjFiles(t *testing.T) {
	repl, err := makeReplicator()
	require.Nil(t, err)
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e"), 0777)
	fp, err := os.Create(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"))
	require.Nil(t, err)
	defer fp.Close()
	objChan := make(chan string)
	cancel := make(chan struct{})
	var files []string
	go repl.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	for obj := range objChan {
		files = append(files, obj)
	}
	require.Equal(t, 1, len(files))
	require.Equal(t, filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"), files[0])

	os.RemoveAll(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"))
	objChan = make(chan string)
	files = nil
	go repl.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	for obj := range objChan {
		files = append(files, obj)
	}
	require.False(t, hummingbird.Exists(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e")))
	require.True(t, hummingbird.Exists(filepath.Join(dir, "objects", "1", "abc")))

	objChan = make(chan string)
	files = nil
	go repl.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	for obj := range objChan {
		files = append(files, obj)
	}
	require.False(t, hummingbird.Exists(filepath.Join(dir, "objects", "1", "abc")))
	require.True(t, hummingbird.Exists(filepath.Join(dir, "objects", "1")))

	objChan = make(chan string)
	files = nil
	go repl.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	for obj := range objChan {
		files = append(files, obj)
	}
	require.False(t, hummingbird.Exists(filepath.Join(dir, "objects", "1")))
	require.True(t, hummingbird.Exists(filepath.Join(dir, "objects")))
}

func TestCancelListObjFiles(t *testing.T) {
	repl, err := makeReplicator()
	require.Nil(t, err)
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e"), 0777)
	fp, err := os.Create(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"))
	require.Nil(t, err)
	fp.Close()
	objChan := make(chan string)
	cancel := make(chan struct{})
	// Oh no, nobody is reading from your channel and you are stuck!
	go repl.listObjFiles(objChan, cancel, filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	// so we cancel you and make sure you closed your channel, which you do on exit.
	close(cancel)
	time.Sleep(time.Millisecond)
	_, ok := <-objChan
	require.False(t, ok)
}

func TestPriorityRepHandler(t *testing.T) {
	t.Parallel()
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator, err := makeReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.driveRoot = driveRoot
	w := httptest.NewRecorder()
	job := &PriorityRepJob{
		Partition:  1,
		FromDevice: &hummingbird.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 5000, ReplicationIp: "127.0.0.1", ReplicationPort: 5000},
		ToDevices: []*hummingbird.Device{
			&hummingbird.Device{Id: 2, Device: "sdb"},
		},
	}
	jsonned, _ := json.Marshal(job)
	req, _ := http.NewRequest("POST", "/priorityrep", bytes.NewBuffer(jsonned))
	go func() {
		replicator.priorityRepHandler(w, req)
		require.EqualValues(t, 200, w.Code)
	}()
	pri := <-replicator.getPriRepChan(1)
	require.Equal(t, "1", strconv.FormatUint(pri.Partition, 10))
}

func TestPriorityRepHandler404(t *testing.T) {
	t.Parallel()
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator, err := makeReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.driveRoot = driveRoot
	w := httptest.NewRecorder()
	job := &PriorityRepJob{
		Partition:  0,
		FromDevice: &hummingbird.Device{Id: 1, Device: "sda", Ip: "127.0.0.1", Port: 5000, ReplicationIp: "127.0.0.1", ReplicationPort: 5000},
		ToDevices: []*hummingbird.Device{
			&hummingbird.Device{Id: 2, Device: "sdb"},
		},
	}
	jsonned, _ := json.Marshal(job)
	req, _ := http.NewRequest("POST", "/priorityrep", bytes.NewBuffer(jsonned))
	replicator.priorityRepHandler(w, req)
	require.EqualValues(t, 404, w.Code)
}

func TestRestartDevice(t *testing.T) {
	ts, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts.Close()

	ts2, err := makeObjectServer()
	assert.Nil(t, err)
	defer ts2.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	req, err = http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/1/a/c/o2", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	ldev := &hummingbird.Device{ReplicationIp: ts.host, ReplicationPort: ts.port, Device: "sda"}
	rdev := &hummingbird.Device{ReplicationIp: ts2.host, ReplicationPort: ts2.port, Device: "sda"}
	dp := &deviceProgress{
		dev:        ldev,
		StartDate:  time.Now(),
		LastUpdate: time.Now(),
	}

	saved := &replicationLogSaver{}
	repl, _ := makeReplicator()
	repl.Rings[0] = &FakeRepRing2{ldev: ldev, rdev: rdev}
	repl.logger = saved

	// set stuff up
	repl.driveRoot = ts.objServer.driveRoot
	myTicker := make(chan time.Time)
	repl.partRateTicker = time.NewTicker(repl.timePerPart)
	repl.partRateTicker.C = myTicker
	repl.concurrencySem = make(chan struct{}, 5)
	repl.deviceProgressMap["sda"] = dp

	repl.restartReplicateDevice(ldev)
	cancelChan := repl.cancelers["sda"]
	// precancel the run
	delete(repl.cancelers, "sda")
	close(cancelChan)
	//start replication for loop
	statsDp := <-repl.deviceProgressPassInit
	assert.Equal(t, uint64(2), statsDp.PartitionsTotal)
	// but got canceled
	statsDp = <-repl.deviceProgressIncr
	assert.Equal(t, uint64(1), statsDp.CancelCount)
	// start up everything again
	repl.restartReplicateDevice(ldev)
	//start replication for loop again
	<-repl.deviceProgressPassInit
	// 1st partition process
	myTicker <- time.Now()
	statsDp = <-repl.deviceProgressIncr
	assert.Equal(t, uint64(1), statsDp.PartitionsDone)
	// syncing file
	statsDp = <-repl.deviceProgressIncr
	assert.Equal(t, uint64(1), statsDp.FilesSent)

	// 2nd partition process
	myTicker <- time.Now()
	statsDp = <-repl.deviceProgressIncr
	assert.Equal(t, uint64(1), statsDp.PartitionsDone)
	statsDp = <-repl.deviceProgressIncr
	assert.Equal(t, uint64(1), statsDp.FilesSent)
	// 2nd partition was processed so cancel next run
	cancelChan = repl.cancelers["sda"]
	delete(repl.cancelers, "sda")
	close(cancelChan)
	// check that full replicate was tracked
	statsDp = <-repl.deviceProgressIncr
	assert.Equal(t, uint64(1), statsDp.FullReplicateCount)
	// starting final run
	statsDp = <-repl.deviceProgressPassInit
	assert.Equal(t, uint64(2), statsDp.PartitionsTotal)
	// but it got canceled so returning
	statsDp = <-repl.deviceProgressIncr
	assert.Equal(t, uint64(1), statsDp.CancelCount)
}

func TestRestartDevices(t *testing.T) {
	t.Parallel()
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator, _ := makeReplicator("bind_port", "1234", "check_mounts", "no")
	replicator.driveRoot = driveRoot
	ldev := &hummingbird.Device{ReplicationIp: "127.0.0.1", ReplicationPort: 6001, Device: "sda"}
	rdev := &hummingbird.Device{ReplicationIp: "127.0.0.2", ReplicationPort: 6001, Device: "sdb"}
	ring := &FakeRepRing2{ldev: ldev, rdev: rdev}
	replicator.Rings[0] = ring
	replicator.restartDevices()
	_, oka := replicator.cancelers["sda"]
	_, okb := replicator.cancelers["sdb"]
	require.True(t, oka)
	require.False(t, okb)
	ring.ldev, ring.rdev = ring.rdev, ring.ldev
	replicator.restartDevices()
	_, oka = replicator.cancelers["sda"]
	_, okb = replicator.cancelers["sdb"]
	require.True(t, okb)
	require.False(t, oka)
}

func TestSyncFileQuarantine(t *testing.T) {
	t.Parallel()
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator, err := makeReplicator("bind_port", "1234", "check_mounts", "no")
	require.Nil(t, err)
	replicator.driveRoot = driveRoot

	hashDir := filepath.Join(driveRoot, "sda", "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e")
	objFile := filepath.Join(hashDir, "12345.data")
	j := &job{policy: 0}

	require.Nil(t, os.MkdirAll(objFile, 0777)) // not a regular file
	replicator.syncFile(objFile, nil, j)
	assert.False(t, hummingbird.Exists(hashDir))

	os.MkdirAll(hashDir, 0777) // error reading metadata
	fp, err := os.Create(objFile)
	require.Nil(t, err)
	defer fp.Close()
	replicator.syncFile(objFile, nil, j)
	assert.False(t, hummingbird.Exists(hashDir))

	os.MkdirAll(hashDir, 0777) // unparseable pickle
	fp, err = os.Create(objFile)
	defer fp.Close()
	require.Nil(t, err)
	RawWriteMetadata(fp.Fd(), []byte("NOT A VALID PICKLE"))
	replicator.syncFile(objFile, nil, j)
	assert.False(t, hummingbird.Exists(hashDir))

	os.MkdirAll(hashDir, 0777) // wrong metadata type
	fp, err = os.Create(objFile)
	defer fp.Close()
	require.Nil(t, err)
	RawWriteMetadata(fp.Fd(), hummingbird.PickleDumps("hi"))
	replicator.syncFile(objFile, nil, j)
	assert.False(t, hummingbird.Exists(hashDir))

	os.MkdirAll(hashDir, 0777) // unparseable content-length
	fp, err = os.Create(objFile)
	defer fp.Close()
	require.Nil(t, err)
	badContentLengthMetdata := map[string]string{
		"Content-Type": "text/plain", "name": "/a/c/o", "ETag": "d41d8cd98f00b204e9800998ecf8427e",
		"X-Timestamp": "12345.12345", "Content-Length": "X"}
	RawWriteMetadata(fp.Fd(), hummingbird.PickleDumps(badContentLengthMetdata))
	replicator.syncFile(objFile, nil, j)
	assert.False(t, hummingbird.Exists(hashDir))

	os.MkdirAll(hashDir, 0777) // content-length doesn't match file size
	fp, err = os.Create(objFile)
	defer fp.Close()
	require.Nil(t, err)
	wrongContentLengthMetdata := map[string]string{
		"Content-Type": "text/plain", "name": "/a/c/o", "ETag": "d41d8cd98f00b204e9800998ecf8427e",
		"X-Timestamp": "12345.12345", "Content-Length": "50000"}
	RawWriteMetadata(fp.Fd(), hummingbird.PickleDumps(wrongContentLengthMetdata))
	replicator.syncFile(objFile, nil, j)
	assert.False(t, hummingbird.Exists(hashDir))
}
