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
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openstack/swift/go/hummingbird"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditHashPasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	f, _ = os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12346.ts"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"name": "somename", "X-Timestamp": ""})
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.Nil(t, err)
	assert.Equal(t, bytesProcessed, int64(12))
}

func TestAuditNonStringKey(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	metadata := map[interface{}]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "Content-Type": "", "X-Timestamp": "", "name": "", 3: "hi"}
	RawWriteMetadata(f.Fd(), hummingbird.PickleDumps(metadata))
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditNonStringValue(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	metadata := map[string]interface{}{"Content-Length": 12, "ETag": "d3ac5112fe464b81184352ccba743001", "Content-Type": "", "X-Timestamp": "", "name": ""}
	RawWriteMetadata(f.Fd(), hummingbird.PickleDumps(metadata))
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashDataMissingMetadata(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashTSMissingMetadata(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.ts"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"name": "somename"})
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashIncorrectContentLength(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "0", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashInvalidContentLength(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "X", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashBadHash(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "f3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(12))
}

func TestAuditHashBadFilename(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.xxx"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashNonfileInDir(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"), 0777)
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

func TestAuditHashNoMetadata(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	f.Write([]byte("testcontents"))
	bytesProcessed, err := auditHash(filepath.Join(dir, "fffffffffffffffffffffffffffffabc"), false)
	assert.NotNil(t, err)
	assert.Equal(t, bytesProcessed, int64(0))
}

type auditLogSaver struct {
	logged []string
}

func (s *auditLogSaver) Err(line string) error {
	s.logged = append(s.logged, line)
	return nil
}

func (s *auditLogSaver) Info(line string) error {
	s.logged = append(s.logged, line)
	return nil
}

func (s *auditLogSaver) Debug(line string) error {
	s.logged = append(s.logged, line)
	return nil
}

func makeAuditor(settings ...string) *Auditor {
	configString := "[object-auditor]\n"
	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	conf, _ := hummingbird.StringConfig(configString)
	auditorDaemon, _ := NewAuditor(conf, &flag.FlagSet{})
	auditorDaemon.(*AuditorDaemon).logger = &auditLogSaver{}
	return &Auditor{AuditorDaemon: auditorDaemon.(*AuditorDaemon), filesPerSecond: 1}
}

func TestFailsWithoutSection(t *testing.T) {
	conf, err := hummingbird.StringConfig("")
	require.Nil(t, err)
	auditorDaemon, err := NewAuditor(conf, &flag.FlagSet{})
	require.NotNil(t, err)
	assert.Nil(t, auditorDaemon)
	assert.True(t, strings.HasPrefix(err.Error(), "Unable to find object-auditor"))
}

func TestAuditSuffixNotDir(t *testing.T) {
	auditor := makeAuditor()
	auditor.logger = &auditLogSaver{}
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	errors := auditor.errors
	auditor.auditSuffix(file.Name())
	assert.True(t, strings.HasPrefix(auditor.logger.(*auditLogSaver).logged[0], "Error reading suffix dir"))
	assert.True(t, auditor.errors > errors)
}

func TestAuditSuffixPasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor()
	totalPasses := auditor.totalPasses
	auditor.auditSuffix(filepath.Join(dir, "abc"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditSuffixQuarantine(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.derp"))
	defer f.Close()
	auditor := makeAuditor()
	totalQuarantines := auditor.totalQuarantines
	auditor.auditSuffix(filepath.Join(dir, "objects", "1", "abc"))
	assert.Equal(t, totalQuarantines+1, auditor.totalQuarantines)
	quarfiles, err := ioutil.ReadDir(filepath.Join(dir, "quarantined"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(quarfiles))
}

func TestAuditSuffixSkipsBad(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "objects", "1", "abc", "notavalidhash"), 0777)
	auditor := makeAuditor()
	auditor.auditSuffix(filepath.Join(dir, "objects", "1", "abc"))
	assert.True(t, strings.HasPrefix(auditor.logger.(*auditLogSaver).logged[0], "Skipping invalid file in suffix"))
}

func TestAuditPartitionNotDir(t *testing.T) {
	auditor := makeAuditor()
	auditor.logger = &auditLogSaver{}
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	errors := auditor.errors
	auditor.auditPartition(file.Name())
	assert.True(t, strings.HasPrefix(auditor.logger.(*auditLogSaver).logged[0], "Error reading partition dir"))
	assert.True(t, auditor.errors > errors)
}

func TestAuditPartitionPasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor()
	totalPasses := auditor.totalPasses
	auditor.auditPartition(filepath.Join(dir, "1"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditPartitionSkipsBadData(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	os.MkdirAll(filepath.Join(dir, "1", "xyz"), 0777)
	f, _ := os.Create(filepath.Join(dir, "1", ".lock"))
	f.Close()
	f, _ = os.Create(filepath.Join(dir, "1", "hashes.pkl"))
	f.Close()
	f, _ = os.Create(filepath.Join(dir, "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor()
	totalPasses := auditor.totalPasses
	auditor.auditPartition(filepath.Join(dir, "1"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditDeviceNotDir(t *testing.T) {
	auditor := makeAuditor("mount_check", "false")
	auditor.logger = &auditLogSaver{}
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.RemoveAll(file.Name())
	errors := auditor.errors
	auditor.auditDevice(file.Name())
	assert.True(t, strings.HasPrefix(auditor.logger.(*auditLogSaver).logged[0], "Error reading objects dir"))
	assert.True(t, auditor.errors > errors)
}

func TestAuditDevicePasses(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor("mount_check", "false")
	totalPasses := auditor.totalPasses
	auditor.auditDevice(filepath.Join(dir, "sda"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditDeviceSkipsBadData(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "X"), 0777)
	f, _ := os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor("mount_check", "false")
	totalPasses := auditor.totalPasses
	auditor.auditDevice(filepath.Join(dir, "sda"))
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestAuditDeviceUnmounted(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1"), 0777)
	auditor := makeAuditor("mount_check", "true")
	auditor.auditDevice(filepath.Join(dir, "sda"))
	assert.True(t, strings.HasPrefix(auditor.logger.(*auditLogSaver).logged[0], "Skipping unmounted device"))
}

func TestFinalLog(t *testing.T) {
	auditor := makeAuditor()
	logger := &auditLogSaver{}
	auditor.logger = logger
	auditor.passStart = time.Now().Add(-60 * time.Second)
	auditor.totalQuarantines = 5
	auditor.totalErrors = 3
	auditor.totalPasses = 120
	auditor.totalBytes = 120000
	auditor.auditorType = "ALL"
	auditor.mode = "forever"
	auditor.finalLog()
	assert.Equal(t, "Object audit (ALL) \"forever\" mode completed: 60.00s. Total quarantined: 5, Total errors: 3, Total files/sec: 2.00, "+
		"Total bytes/sec: 2000.00, Auditing time: 0.00, Rate: 0.00", logger.logged[0])
}

func TestAuditRun(t *testing.T) {
	dir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(dir)
	os.MkdirAll(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc"), 0777)
	f, _ := os.Create(filepath.Join(dir, "sda", "objects", "1", "abc", "fffffffffffffffffffffffffffffabc", "12345.data"))
	defer f.Close()
	WriteMetadata(f.Fd(), map[string]string{"Content-Length": "12", "ETag": "d3ac5112fe464b81184352ccba743001", "name": "", "Content-Type": "", "X-Timestamp": ""})
	f.Write([]byte("testcontents"))
	auditor := makeAuditor("mount_check", "false")
	auditor.driveRoot = dir
	logger := &auditLogSaver{}
	auditor.logger = logger
	totalPasses := auditor.totalPasses
	auditor.run(OneTimeChan())
	assert.Equal(t, totalPasses+1, auditor.totalPasses)
	assert.Equal(t, int64(12), auditor.totalBytes)
}

func TestStatReport(t *testing.T) {
	auditor := makeAuditor("mount_check", "false")
	auditor.passStart = time.Now().Add(-120 * time.Second)
	auditor.lastLog = time.Now().Add(-120 * time.Second)
	auditor.passes = 120
	auditor.bytesProcessed = 120000
	auditor.quarantines = 17
	auditor.errors = 41
	auditor.statsReport()
	args := [13]float64{}
	var stra, strb string
	pargs := []interface{}{&stra, &strb}
	for i := range args {
		pargs = append(pargs, &args[i])
	}
	fmt.Sscanf(
		auditor.logger.(*auditLogSaver).logged[0],
		"Object audit (). Since %s %s %f %f:%f:%f %f: Locally: %f passed, %f quarantined, %f errors, files/sec: %f , bytes/sec: %f, Total time: %f, Auditing time: %f, Rate: %f",
		pargs...)
	assert.Equal(t, 120.0, args[5])
	assert.Equal(t, 17.0, args[6])
	assert.Equal(t, 41.0, args[7])
	assert.Equal(t, 1.0, args[8])
	assert.Equal(t, 1000.0, args[9])
	assert.Equal(t, 120.0, args[10])
}
