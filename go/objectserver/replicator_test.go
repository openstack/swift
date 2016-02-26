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

func (r *FakeRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return nil, false
}

func (r *FakeRing) GetPartition(account string, container string, object string) uint64 {
	return 0
}

func (r *FakeRing) LocalDevices(localPort int) (devs []*hummingbird.Device, err error) {
	return nil, nil
}

func (r *FakeRing) GetMoreNodes(partition uint64) hummingbird.MoreNodes {
	return nil
}

func makeReplicator(settings ...string) *Replicator {
	return makeReplicatorWithFlags(settings, &flag.FlagSet{})
}

func makeReplicatorWithFlags(settings []string, flags *flag.FlagSet) *Replicator {
	conf, _ := ioutil.TempFile("", "")
	conf.WriteString("[object-replicator]\nmount_check=false\n")
	for i := 0; i < len(settings); i += 2 {
		fmt.Fprintf(conf, "%s=%s\n", settings[i], settings[i+1])
	}
	defer conf.Close()
	defer os.RemoveAll(conf.Name())
	replicator, _ := NewReplicator(conf.Name(), flags)
	rep := replicator.(*Replicator)
	rep.concurrencySem = make(chan struct{}, 1)
	return rep
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

type FakeHandoffRing struct {
	*FakeRing
	dev *hummingbird.Device
}

func (r *FakeHandoffRing) GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool) {
	return []*hummingbird.Device{r.dev}, true
}

func TestReplicatorInitFail(t *testing.T) {
	replicator, err := NewReplicator("nonexistentfile", &flag.FlagSet{})
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

func TestGetFile(t *testing.T) {
	replicator := makeReplicator()
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
	replicator := makeReplicator()
	_, _, _, err := replicator.getFile("somenonexistentfile")
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
	replicator := makeReplicator()
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
	replicator := makeReplicator("bind_port", fmt.Sprintf("%d", ts.port))
	replicator.driveRoot = ts.objServer.driveRoot
	replicator.Ring = &FakeRepRing1{ldev: ldev, rdev: rdev}
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

	ldev := &hummingbird.Device{ReplicationIp: ts.host, ReplicationPort: ts.port, Device: "sda"}
	rdev := &hummingbird.Device{ReplicationIp: ts2.host, ReplicationPort: ts2.port, Device: "sda"}
	replicator := makeReplicator("bind_port", fmt.Sprintf("%d", ts.port))
	replicator.driveRoot = ts.objServer.driveRoot
	replicator.Ring = &FakeRepRing2{ldev: ldev, rdev: rdev}
	replicator.Run()

	req, err = http.NewRequest("HEAD", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts2.host, ts2.port), nil)
	assert.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
}

func TestListObjFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e"), 0777)
	fp, err := os.Create(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"))
	require.Nil(t, err)
	defer fp.Close()
	files, err := listObjFiles(filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	require.Nil(t, err)
	require.Equal(t, 1, len(files))
	require.Equal(t, filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"), files[0])

	os.RemoveAll(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e", "12345.data"))
	files, err = listObjFiles(filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	require.False(t, hummingbird.Exists(filepath.Join(dir, "objects", "1", "abc", "d41d8cd98f00b204e9800998ecf8427e")))
	require.True(t, hummingbird.Exists(filepath.Join(dir, "objects", "1", "abc")))

	files, err = listObjFiles(filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	require.False(t, hummingbird.Exists(filepath.Join(dir, "objects", "1", "abc")))
	require.True(t, hummingbird.Exists(filepath.Join(dir, "objects", "1")))

	files, err = listObjFiles(filepath.Join(dir, "objects", "1"), func(string) bool { return true })
	require.False(t, hummingbird.Exists(filepath.Join(dir, "objects", "1")))
	require.True(t, hummingbird.Exists(filepath.Join(dir, "objects")))
}

func TestPriorityRepHandler(t *testing.T) {
	t.Parallel()
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("bind_port", "1234", "check_mounts", "no")
	replicator.driveRoot = driveRoot
	w := httptest.NewRecorder()
	job := &PriorityRepJob{
		JobType:    "handoff",
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
	require.Equal(t, "handoff", pri.JobType)
}

func TestPriorityRepHandler404(t *testing.T) {
	t.Parallel()
	driveRoot := setupDirectory()
	defer os.RemoveAll(driveRoot)
	replicator := makeReplicator("bind_port", "1234", "check_mounts", "no")
	replicator.driveRoot = driveRoot
	w := httptest.NewRecorder()
	job := &PriorityRepJob{
		JobType:    "handoff",
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
