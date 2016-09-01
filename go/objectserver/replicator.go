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
	"bufio"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/justinas/alice"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/middleware"
)

var StatsReportInterval = 300 * time.Second
var TmpEmptyTime = 24 * time.Hour
var ReplicateDeviceTimeout = 4 * time.Hour

// Encapsulates a partition for replication.
type job struct {
	dev       *hummingbird.Device
	partition string
	objPath   string
	policy    int
}

type ReplicationData struct {
	dev    *hummingbird.Device
	conn   *RepConn
	hashes map[string]string
	err    error
}

type NoMoreNodes struct{}

func (n *NoMoreNodes) Next() *hummingbird.Device {
	return nil
}

type PriorityRepJob struct {
	Partition  uint64                `json:"partition"`
	FromDevice *hummingbird.Device   `json:"from_device"`
	ToDevices  []*hummingbird.Device `json:"to_devices"`
	Policy     int                   `json:"policy"`
}

type deviceProgress struct {
	PartitionsDone   uint64
	PartitionsTotal  uint64
	StartDate        time.Time
	LastUpdate       time.Time
	FilesSent        uint64
	BytesSent        uint64
	PriorityRepsDone uint64

	FullReplicateCount uint64
	CancelCount        uint64
	LastPassDuration   time.Duration
	LastPassUpdate     time.Time

	dev *hummingbird.Device
}

// Object replicator daemon object
type Replicator struct {
	concurrency    int
	checkMounts    bool
	driveRoot      string
	reconCachePath string
	bindIp         string
	logger         hummingbird.SysLogLike
	logLevel       string
	port           int
	devGroup       sync.WaitGroup
	partRateTicker *time.Ticker
	timePerPart    time.Duration
	LoopSleepTime  time.Duration
	quorumDelete   bool
	concurrencySem chan struct{}
	devices        map[string]bool
	partitions     map[string]bool
	priRepChans    map[int]chan PriorityRepJob
	priRepM        sync.Mutex
	reclaimAge     int64
	Rings          map[int]hummingbird.Ring

	hashPathPrefix   string
	hashPathSuffix   string
	reserve          int64
	replicationMan   *ReplicationManager
	replicateTimeout time.Duration

	once      bool
	cancelers map[string]chan struct{}

	/* stats accounting */
	deviceProgressMap      map[string]*deviceProgress
	deviceProgressIncr     chan deviceProgress
	deviceProgressPassInit chan deviceProgress
}

func (r *Replicator) LogError(format string, args ...interface{}) {
	r.logger.Err(fmt.Sprintf(format, args...))
}

func (r *Replicator) LogInfo(format string, args ...interface{}) {
	r.logger.Info(fmt.Sprintf(format, args...))
}

func (r *Replicator) LogDebug(format string, args ...interface{}) {
	r.logger.Debug(fmt.Sprintf(format, args...))
}

func (r *Replicator) LogPanics(m string) {
	if e := recover(); e != nil {
		r.LogError("%s: %s: %s", m, e, debug.Stack())
	}
}

// OneTimeChan returns a channel that will yield the current time once, then is closed.
func OneTimeChan() chan time.Time {
	c := make(chan time.Time, 1)
	c <- time.Now()
	close(c)
	return c
}

type quarantineFileError struct {
	msg string
}

func (q quarantineFileError) Error() string {
	return q.msg
}

func (r *Replicator) getFile(filePath string) (fp *os.File, xattrs []byte, size int64, err error) {
	fp, err = os.Open(filePath)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("unable to open file (%v): %s", err, filePath)
	}
	defer func() {
		if err != nil {
			fp.Close()
		}
	}()
	finfo, err := fp.Stat()
	if err != nil || !finfo.Mode().IsRegular() {
		return nil, nil, 0, quarantineFileError{"not a regular file"}
	}
	rawxattr, err := RawReadMetadata(fp.Fd())
	if err != nil || len(rawxattr) == 0 {
		return nil, nil, 0, quarantineFileError{"error reading xattrs"}
	}

	// Perform a mini-audit, since it's cheap and we can potentially avoid spreading bad data around.
	v, err := hummingbird.PickleLoads(rawxattr)
	if err != nil {
		return nil, nil, 0, quarantineFileError{"error unpickling xattrs"}
	}
	metadata, ok := v.(map[interface{}]interface{})
	if !ok {
		return nil, nil, 0, quarantineFileError{"invalid metadata type"}
	}
	for key, value := range metadata {
		if _, ok := key.(string); !ok {
			return nil, nil, 0, quarantineFileError{"invalid key in metadata"}
		}
		if _, ok := value.(string); !ok {
			return nil, nil, 0, quarantineFileError{"invalid value in metadata"}
		}
	}
	switch filepath.Ext(filePath) {
	case ".data":
		for _, reqEntry := range []string{"Content-Length", "Content-Type", "name", "ETag", "X-Timestamp"} {
			if _, ok := metadata[reqEntry]; !ok {
				return nil, nil, 0, quarantineFileError{".data missing required metadata"}
			}
		}
		if contentLength, err := strconv.ParseInt(metadata["Content-Length"].(string), 10, 64); err != nil || contentLength != finfo.Size() {
			return nil, nil, 0, quarantineFileError{"invalid content-length"}
		}
	case ".ts":
		for _, reqEntry := range []string{"name", "X-Timestamp"} {
			if _, ok := metadata[reqEntry]; !ok {
				return nil, nil, 0, quarantineFileError{".ts missing required metadata"}
			}
		}
	}
	return fp, rawxattr, finfo.Size(), nil
}

func (r *Replicator) beginReplication(dev *hummingbird.Device, partition string, hashes bool, policy int, rChan chan ReplicationData) {
	rc, err := NewRepConn(dev, partition, policy)
	if err != nil {
		r.LogError("[beginReplication] error creating new request: %v", err)
		rChan <- ReplicationData{dev: dev, conn: nil, hashes: nil, err: err}
		return
	}

	if err := rc.SendMessage(BeginReplicationRequest{Device: dev.Device, Partition: partition, NeedHashes: hashes}); err != nil {
		rChan <- ReplicationData{dev: dev, conn: nil, hashes: nil, err: err}
		return
	}
	var brr BeginReplicationResponse
	if err := rc.RecvMessage(&brr); err != nil {
		rChan <- ReplicationData{dev: dev, conn: nil, hashes: nil, err: err}
		return
	}
	rChan <- ReplicationData{dev: dev, conn: rc, hashes: brr.Hashes, err: nil}
}

func (r *Replicator) listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool) {
	defer close(objChan)
	suffixDirs, err := filepath.Glob(filepath.Join(partdir, "[a-f0-9][a-f0-9][a-f0-9]"))
	if err != nil {
		r.LogError("[listObjFiles] %v", err)
		return
	}
	if len(suffixDirs) == 0 {
		os.Remove(filepath.Join(partdir, ".lock"))
		os.Remove(filepath.Join(partdir, "hashes.pkl"))
		os.Remove(filepath.Join(partdir, "hashes.invalid"))
		os.Remove(partdir)
		return
	}
	for i := len(suffixDirs) - 1; i > 0; i-- { // shuffle suffixDirs list
		j := rand.Intn(i + 1)
		suffixDirs[j], suffixDirs[i] = suffixDirs[i], suffixDirs[j]
	}
	for _, suffDir := range suffixDirs {
		if !needSuffix(filepath.Base(suffDir)) {
			continue
		}
		hashDirs, err := filepath.Glob(filepath.Join(suffDir, "????????????????????????????????"))
		if err != nil {
			r.LogError("[listObjFiles] %v", err)
			return
		}
		if len(hashDirs) == 0 {
			os.Remove(suffDir)
			continue
		}
		for _, hashDir := range hashDirs {
			fileList, err := filepath.Glob(filepath.Join(hashDir, "*.[tdm]*"))
			if len(fileList) == 0 {
				os.Remove(hashDir)
				continue
			}
			if err != nil {
				r.LogError("[listObjFiles] %v", err)
				return
			}
			for _, objFile := range fileList {
				select {
				case objChan <- objFile:
				case <-cancel:
					return
				}
			}
		}
	}
}

type syncFileArg struct {
	conn *RepConn
	dev  *hummingbird.Device
}

func (r *Replicator) syncFile(objFile string, dst []*syncFileArg, j *job) (syncs int, insync int, err error) {
	var wrs []*syncFileArg
	lst := strings.Split(objFile, string(os.PathSeparator))
	relPath := filepath.Join(lst[len(lst)-5:]...)
	fp, xattrs, fileSize, err := r.getFile(objFile)
	if _, ok := err.(quarantineFileError); ok {
		hashDir := filepath.Dir(objFile)
		r.LogError("[syncFile] %s failed audit and is being quarantined: %s", hashDir, err.Error())
		QuarantineHash(hashDir)
		return 0, 0, nil
	} else if err != nil {
		return 0, 0, nil
	}
	defer fp.Close()

	// ask each server if we need to sync the file
	for _, sfa := range dst {
		var sfr SyncFileResponse
		thisPath := filepath.Join(sfa.dev.Device, relPath)
		sfa.conn.SendMessage(SyncFileRequest{Path: thisPath, Xattrs: hex.EncodeToString(xattrs), Size: fileSize})
		if err := sfa.conn.RecvMessage(&sfr); err != nil {
			continue
		} else if sfr.GoAhead {
			wrs = append(wrs, sfa)
		} else if sfr.NewerExists {
			insync++
			if os.Remove(objFile) == nil {
				InvalidateHash(filepath.Dir(objFile))
			}
		} else if sfr.Exists {
			insync++
		}
	}
	if len(wrs) == 0 { // nobody needed the file
		return
	}

	// send the file to servers
	scratch := make([]byte, 32768)
	var length int
	var totalRead int64
	for length, err = fp.Read(scratch); err == nil; length, err = fp.Read(scratch) {
		totalRead += int64(length)
		for index, sfa := range wrs {
			if sfa == nil {
				continue
			}
			if _, err := sfa.conn.Write(scratch[0:length]); err != nil {
				r.LogError("Failed to write to remoteDevice: %d, %v", sfa.dev.Id, err)
				wrs[index] = nil
			}
		}
	}
	if totalRead != fileSize {
		return 0, 0, fmt.Errorf("Failed to read the full file: %s, %v", objFile, err)
	}

	// get file upload results
	for _, sfa := range wrs {
		if sfa == nil {
			continue
		}
		var fur FileUploadResponse
		sfa.conn.Flush()
		if sfa.conn.RecvMessage(&fur) == nil {
			if fur.Success {
				syncs++
				insync++
				r.deviceProgressIncr <- deviceProgress{
					dev:       j.dev,
					FilesSent: 1,
					BytesSent: uint64(fileSize),
				}
			}
		}
	}
	return syncs, insync, nil
}

func (r *Replicator) replicateLocal(j *job, nodes []*hummingbird.Device, moreNodes hummingbird.MoreNodes) {
	defer r.LogPanics("PANIC REPLICATING LOCAL PARTITION")
	path := filepath.Join(j.objPath, j.partition)
	syncCount := 0
	remoteHashes := make(map[int]map[string]string)
	remoteConnections := make(map[int]*RepConn)
	startGetHashesRemote := time.Now()
	rChan := make(chan ReplicationData)
	for i := 0; i < len(nodes); i++ {
		go r.beginReplication(nodes[i], j.partition, true, j.policy, rChan)
	}
	for i := 0; i < len(nodes); i++ {
		rData := <-rChan
		if rData.err == nil {
			defer rData.conn.Close()
			remoteHashes[rData.dev.Id] = rData.hashes
			remoteConnections[rData.dev.Id] = rData.conn
		} else if rData.err == RepUnmountedError {
			if nextNode := moreNodes.Next(); nextNode != nil {
				go r.beginReplication(nextNode, j.partition, true, j.policy, rChan)
				nodes = append(nodes, nextNode)
			}
		}
	}
	if len(remoteHashes) == 0 {
		return
	}

	timeGetHashesRemote := float64(time.Now().Sub(startGetHashesRemote)) / float64(time.Second)
	startGetHashesLocal := time.Now()

	recalc := []string{}
	hashes, err := GetHashes(r.driveRoot, j.dev.Device, j.partition, recalc, r.reclaimAge, j.policy, r)
	if err != nil {
		r.LogError("[replicateLocal] error getting local hashes: %v", err)
		return
	}
	for suffix, localHash := range hashes {
		for _, remoteHash := range remoteHashes {
			if remoteHash[suffix] != "" && localHash != remoteHash[suffix] {
				recalc = append(recalc, suffix)
				break
			}
		}
	}
	hashes, err = GetHashes(r.driveRoot, j.dev.Device, j.partition, recalc, r.reclaimAge, j.policy, r)
	if err != nil {
		r.LogError("[replicateLocal] error recalculating local hashes: %v", err)
		return
	}
	timeGetHashesLocal := float64(time.Now().Sub(startGetHashesLocal)) / float64(time.Second)

	objChan := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go r.listObjFiles(objChan, cancel, path, func(suffix string) bool {
		for _, remoteHash := range remoteHashes {
			if hashes[suffix] != remoteHash[suffix] {
				return true
			}
		}
		return false
	})
	startSyncing := time.Now()
	for objFile := range objChan {
		toSync := make([]*syncFileArg, 0)
		suffix := filepath.Base(filepath.Dir(filepath.Dir(objFile)))
		for _, dev := range nodes {
			if rhashes, ok := remoteHashes[dev.Id]; ok && hashes[suffix] != rhashes[suffix] {
				if remoteConnections[dev.Id].Disconnected {
					continue
				}
				toSync = append(toSync, &syncFileArg{conn: remoteConnections[dev.Id], dev: dev})
			}
		}
		if syncs, _, err := r.syncFile(objFile, toSync, j); err == nil {
			syncCount += syncs
		} else {
			r.LogError("[syncFile] %v", err)
			return
		}
	}
	for _, conn := range remoteConnections {
		if !conn.Disconnected {
			conn.SendMessage(SyncFileRequest{Done: true})
		}
	}
	timeSyncing := float64(time.Now().Sub(startSyncing)) / float64(time.Second)
	if syncCount > 0 {
		r.LogInfo("[replicateLocal] Partition %s synced %d files (%.2fs / %.2fs / %.2fs)", path, syncCount, timeGetHashesRemote, timeGetHashesLocal, timeSyncing)
	}
}

func (r *Replicator) replicateHandoff(j *job, nodes []*hummingbird.Device) {
	defer r.LogPanics("PANIC REPLICATING HANDOFF PARTITION")
	path := filepath.Join(j.objPath, j.partition)
	syncCount := 0
	remoteAvailable := make(map[int]bool)
	remoteConnections := make(map[int]*RepConn)
	rChan := make(chan ReplicationData)
	nodesNeeded := len(nodes)
	for i := 0; i < nodesNeeded; i++ {
		go r.beginReplication(nodes[i], j.partition, false, j.policy, rChan)
	}
	for i := 0; i < nodesNeeded; i++ {
		rData := <-rChan
		if rData.err == nil {
			remoteAvailable[rData.dev.Id] = true
			remoteConnections[rData.dev.Id] = rData.conn
			defer rData.conn.Close()
		}
	}
	if len(remoteAvailable) == 0 {
		return
	}

	objChan := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go r.listObjFiles(objChan, cancel, path, func(string) bool { return true })
	for objFile := range objChan {
		toSync := make([]*syncFileArg, 0)
		for _, dev := range nodes {
			if remoteAvailable[dev.Id] && !remoteConnections[dev.Id].Disconnected {
				toSync = append(toSync, &syncFileArg{conn: remoteConnections[dev.Id], dev: dev})
			}
		}
		if syncs, insync, err := r.syncFile(objFile, toSync, j); err == nil {
			syncCount += syncs

			success := insync == len(nodes)
			if r.quorumDelete {
				success = insync >= len(nodes)/2+1
			}
			if success {
				os.Remove(objFile)
				os.Remove(filepath.Dir(objFile))
			}
		} else {
			r.LogError("[syncFile] %v", err)
		}
	}
	for _, conn := range remoteConnections {
		if !conn.Disconnected {
			conn.SendMessage(SyncFileRequest{Done: true})
		}
	}
	if syncCount > 0 {
		r.LogInfo("[replicateHandoff] Partition %s synced %d files", path, syncCount)
	}
}

// Clean up any old files in a device's tmp directories.
func (r *Replicator) cleanTemp(dev *hummingbird.Device) {
	tempDir := TempDirPath(r.driveRoot, dev.Device)
	if tmpContents, err := ioutil.ReadDir(tempDir); err == nil {
		for _, tmpEntry := range tmpContents {
			if time.Since(tmpEntry.ModTime()) > TmpEmptyTime {
				os.RemoveAll(filepath.Join(tempDir, tmpEntry.Name()))
			}
		}
	}
}

// start or restart replication on a device and set up canceler
func (r *Replicator) restartReplicateDevice(dev *hummingbird.Device) {
	r.devGroup.Add(1)
	if canceler, ok := r.cancelers[dev.Device]; ok {
		close(canceler)
	}
	r.cancelers[dev.Device] = make(chan struct{})
	go r.replicateDevice(dev, r.cancelers[dev.Device])
}

// Run replication on given device. For normal usage do not call directly, use "restartReplicateDevice"
func (r *Replicator) replicateDevice(dev *hummingbird.Device, canceler chan struct{}) {
	defer r.LogPanics(fmt.Sprintf("PANIC REPLICATING DEVICE: %s", dev.Device))
	defer r.devGroup.Done()
	var lastPassDuration time.Duration
	for {
		time.Sleep(r.LoopSleepTime)
		passStartTime := time.Now()
		if mounted, err := hummingbird.IsMount(filepath.Join(r.driveRoot, dev.Device)); r.checkMounts && (err != nil || mounted != true) {
			r.LogError("[replicateDevice] Drive not mounted: %s", dev.Device)
			break
		}
		if hummingbird.Exists(filepath.Join(r.driveRoot, dev.Device, "lock_device")) {
			break
		}

		r.cleanTemp(dev)
		jobList := make([]job, 0)

		for policy := range r.Rings {
			objPath := filepath.Join(r.driveRoot, dev.Device, PolicyDir(policy))
			if fi, err := os.Stat(objPath); err != nil || !fi.Mode().IsDir() {
				continue
			}
			policyPartitions, err := filepath.Glob(filepath.Join(objPath, "[0-9]*"))
			if err != nil {
				r.LogError("[replicateDevice] Error getting partition list: %s (%v)", objPath, err)
				continue
			}
			for _, partition := range policyPartitions {
				partition = filepath.Base(partition)
				if _, ok := r.partitions[partition]; len(r.partitions) > 0 && !ok {
					continue
				}
				if _, err := strconv.ParseUint(partition, 10, 64); err == nil {
					jobList = append(jobList,
						job{objPath: objPath,
							partition: partition,
							dev:       dev,
							policy:    policy})
				}
			}
		}

		numPartitions := uint64(len(jobList))
		if numPartitions == 0 {
			r.LogError("[replicateDevice] No objects found: %s", filepath.Join(r.driveRoot, dev.Device))
		}

		partitionsProcessed := uint64(0)

		r.deviceProgressPassInit <- deviceProgress{
			dev:              dev,
			PartitionsTotal:  numPartitions,
			LastPassDuration: lastPassDuration,
		}

		for i := len(jobList) - 1; i > 0; i-- { // shuffle job list
			j := rand.Intn(i + 1)
			jobList[j], jobList[i] = jobList[i], jobList[j]
		}

		for _, j := range jobList {
			select {
			case <-canceler:
				{
					r.deviceProgressIncr <- deviceProgress{
						dev:         dev,
						CancelCount: 1,
					}
					r.LogError("replicateDevice canceled for device: %s", dev.Device)
					return
				}
			default:
			}
			r.processPriorityJobs(dev.Id)

			func() {
				<-r.partRateTicker.C
				r.concurrencySem <- struct{}{}
				defer func() {
					<-r.concurrencySem
				}()
				r.deviceProgressIncr <- deviceProgress{
					dev:            dev,
					PartitionsDone: 1,
				}
				partitioni, err := strconv.ParseUint(j.partition, 10, 64)
				if err != nil {
					return
				}
				nodes, handoff := r.Rings[j.policy].GetJobNodes(partitioni, j.dev.Id)
				partitionsProcessed += 1
				if handoff {
					r.replicateHandoff(&j, nodes)
				} else {
					r.replicateLocal(&j, nodes, r.Rings[j.policy].GetMoreNodes(partitioni))
				}
			}()
		}
		if partitionsProcessed >= numPartitions {
			r.deviceProgressIncr <- deviceProgress{
				dev:                dev,
				FullReplicateCount: 1,
			}
			lastPassDuration = time.Since(passStartTime)
		}
		if r.once {
			break
		}
	}
}

// restartDevices should be called periodically from the statsReporter thread to make sure devices that should be replicating are.
func (r *Replicator) restartDevices() {
	shouldBeRunning := make(map[string]bool)
	deviceMap := map[string]*hummingbird.Device{}
	for _, ring := range r.Rings {
		devices, err := ring.LocalDevices(r.port)
		if err != nil {
			r.LogError("Error getting local devices: %v", err)
			return
		}
		for _, dev := range devices {
			deviceMap[dev.Device] = dev
		}
	}
	// launch replication for any new devices
	for _, dev := range deviceMap {
		shouldBeRunning[dev.Device] = true
		if _, ok := r.deviceProgressMap[dev.Device]; !ok {
			r.restartReplicateDevice(dev)
		}
	}
	// kill any replicators that are no longer in the ring
	for dev, canceler := range r.cancelers {
		if !shouldBeRunning[dev] {
			close(canceler)
			delete(r.cancelers, dev)
			delete(r.deviceProgressMap, dev)
		}
	}
	// re-launch any stalled replicators
	for _, dp := range r.deviceProgressMap {
		if time.Since(dp.LastUpdate) > ReplicateDeviceTimeout {
			r.restartReplicateDevice(dp.dev)
			continue
		}
	}
}

// Collect and log replication stats - runs in a goroutine launched by run(), runs for the duration of a replication pass.
func (r *Replicator) statsReporter(c <-chan time.Time) {
	for {
		select {
		case dp := <-r.deviceProgressPassInit:
			curDp, ok := r.deviceProgressMap[dp.dev.Device]
			if !ok {
				curDp = &deviceProgress{
					dev:        dp.dev,
					StartDate:  time.Now(),
					LastUpdate: time.Now(),
				}
				r.deviceProgressMap[dp.dev.Device] = curDp
			}

			curDp.StartDate = time.Now()
			curDp.LastUpdate = time.Now()
			curDp.PartitionsDone = 0
			curDp.PartitionsTotal = dp.PartitionsTotal
			curDp.FilesSent = 0
			curDp.BytesSent = 0
			curDp.PriorityRepsDone = 0
			curDp.LastPassDuration = dp.LastPassDuration
			if dp.LastPassDuration > 0 {
				curDp.LastPassUpdate = time.Now()
			}
		case dp := <-r.deviceProgressIncr:
			if curDp, ok := r.deviceProgressMap[dp.dev.Device]; !ok {
				r.LogError("Trying to increment progress and not present: %s", dp.dev.Device)
			} else {
				curDp.LastUpdate = time.Now()
				curDp.PartitionsDone += dp.PartitionsDone
				curDp.FilesSent += dp.FilesSent
				curDp.BytesSent += dp.BytesSent
				curDp.PriorityRepsDone += dp.PriorityRepsDone
				curDp.FullReplicateCount += dp.FullReplicateCount
				curDp.CancelCount += dp.CancelCount
			}
		case _, ok := <-c:
			if !ok {
				return
			}
			var totalParts uint64
			var doneParts uint64
			var bytesProcessed uint64
			var filesProcessed uint64
			var processingDuration time.Duration
			allHaveCompleted := true
			var totalDuration time.Duration
			var maxLastPassUpdate time.Time

			r.restartDevices()

			for _, dp := range r.deviceProgressMap {
				totalParts += dp.PartitionsTotal
				doneParts += dp.PartitionsDone
				bytesProcessed += dp.BytesSent
				filesProcessed += dp.FilesSent
				processingDuration += dp.LastUpdate.Sub(dp.StartDate)

				allHaveCompleted = allHaveCompleted && (dp.LastPassDuration > 0)
				totalDuration += processingDuration
				if maxLastPassUpdate.Before(dp.LastPassUpdate) {
					maxLastPassUpdate = dp.LastPassUpdate
				}
			}

			if doneParts > 0 {
				processingNsecs := float64(processingDuration.Nanoseconds())
				partsPerNsecond := float64(doneParts) / processingNsecs
				remaining := time.Duration(float64(totalParts-doneParts) / partsPerNsecond)
				var remainingStr string
				if remaining >= time.Hour {
					remainingStr = fmt.Sprintf("%.0fh", remaining.Hours())
				} else if remaining >= time.Minute {
					remainingStr = fmt.Sprintf("%.0fm", remaining.Minutes())
				} else {
					remainingStr = fmt.Sprintf("%.0fs", remaining.Seconds())
				}
				r.LogInfo("%d/%d (%.2f%%) partitions replicated in %.2f worker seconds (%.2f/sec, %v remaining)",
					doneParts, totalParts, float64(100*doneParts)/float64(totalParts),
					processingNsecs/float64(time.Second), partsPerNsecond*float64(time.Second), remainingStr)
			}

			if allHaveCompleted {
				// this is a little lame- i'd rather just drop this completely
				hummingbird.DumpReconCache(r.reconCachePath, "object",
					map[string]interface{}{
						"object_replication_time": float64(totalDuration) / float64(len(r.deviceProgressMap)) / float64(time.Second),
						"object_replication_last": float64(maxLastPassUpdate.UnixNano()) / float64(time.Second),
					})
			}
		}
	}
}

// Run replication passes for each device on the whole server.
func (r *Replicator) run() {
	statsTicker := time.NewTicker(StatsReportInterval)
	go r.statsReporter(statsTicker.C)

	r.partRateTicker = time.NewTicker(r.timePerPart)
	r.concurrencySem = make(chan struct{}, r.concurrency)
	deviceMap := map[string]*hummingbird.Device{}
	for _, ring := range r.Rings {
		devices, err := ring.LocalDevices(r.port)
		if err != nil {
			r.LogError("Error getting local devices: %v", err)
			return
		}
		for _, dev := range devices {
			deviceMap[dev.Device] = dev
		}
	}
	for _, dev := range deviceMap {
		if _, ok := r.devices[dev.Device]; ok || len(r.devices) == 0 {
			r.restartReplicateDevice(dev)
		}
	}
	r.devGroup.Wait()
}

// processPriorityJobs runs any pending priority jobs given the device's id
func (r *Replicator) processPriorityJobs(id int) {
	for {
		select {
		case pri := <-r.getPriRepChan(id):
			r.deviceProgressIncr <- deviceProgress{
				dev:              pri.FromDevice,
				PriorityRepsDone: 1,
			}

			func() {
				<-r.partRateTicker.C
				r.concurrencySem <- struct{}{}
				j := &job{
					dev:       pri.FromDevice,
					partition: strconv.FormatUint(pri.Partition, 10),
					objPath:   filepath.Join(r.driveRoot, pri.FromDevice.Device, "objects"),
					policy:    pri.Policy,
				}
				_, handoff := r.Rings[pri.Policy].GetJobNodes(pri.Partition, pri.FromDevice.Id)
				defer func() {
					<-r.concurrencySem
				}()
				toDevicesArr := make([]string, len(pri.ToDevices))
				for i, s := range pri.ToDevices {
					toDevicesArr[i] = fmt.Sprintf("%s:%d/%s", s.Ip, s.Port, s.Device)
				}
				jobType := "local"
				if handoff {
					jobType = "handoff"
				}
				r.LogInfo("PriorityReplicationJob. Partition: %d as %s from %s to %s", pri.Partition, jobType, pri.FromDevice.Device, strings.Join(toDevicesArr, ","))
				if handoff {
					r.replicateHandoff(j, pri.ToDevices)
				} else {
					r.replicateLocal(j, pri.ToDevices, &NoMoreNodes{})
				}
			}()
		default:
			return
		}
	}
}

// getPriRepChan synchronizes access to the r.priRepChans mapping
func (r *Replicator) getPriRepChan(id int) chan PriorityRepJob {
	r.priRepM.Lock()
	defer r.priRepM.Unlock()
	if _, ok := r.priRepChans[id]; !ok {
		r.priRepChans[id] = make(chan PriorityRepJob)
	}
	return r.priRepChans[id]
}

// ProgressReportHandler handles HTTP requests for current replication progress
func (r *Replicator) ProgressReportHandler(w http.ResponseWriter, req *http.Request) {
	_, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	data, err := json.Marshal(r.deviceProgressMap)
	if err != nil {
		r.LogError("Error Marshaling device progress: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
	return

}

// priorityRepHandler handles HTTP requests for priority replications jobs.
func (r *Replicator) priorityRepHandler(w http.ResponseWriter, req *http.Request) {
	var pri PriorityRepJob
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	if err := json.Unmarshal(data, &pri); err != nil {
		w.WriteHeader(400)
		return
	}
	if r.checkMounts {
		if mounted, err := hummingbird.IsMount(filepath.Join(r.driveRoot, pri.FromDevice.Device)); err != nil || mounted == false {
			w.WriteHeader(507)
			return
		}
	}
	if !hummingbird.Exists(filepath.Join(r.driveRoot, pri.FromDevice.Device, "objects", strconv.FormatUint(pri.Partition, 10))) {
		w.WriteHeader(404)
		return
	}
	timeout := time.NewTimer(time.Hour)
	defer timeout.Stop()
	select {
	case r.getPriRepChan(pri.FromDevice.Id) <- pri:
	case <-timeout.C:
	}
	w.WriteHeader(200)
}

func (r *Replicator) objReplicateHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)

	var recalculate []string
	if len(vars["suffixes"]) > 0 {
		recalculate = strings.Split(vars["suffixes"], "-")
	}
	policy, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policy = 0
	}
	hashes, err := GetHashes(r.driveRoot, vars["device"], vars["partition"], recalculate, r.reclaimAge, policy, hummingbird.GetLogger(request))
	if err != nil {
		hummingbird.GetLogger(request).LogError("Unable to get hashes for %s/%s", vars["device"], vars["partition"])
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(hummingbird.PickleDumps(hashes))
}

func (r *Replicator) objRepConnHandler(writer http.ResponseWriter, request *http.Request) {
	var conn net.Conn
	var rw *bufio.ReadWriter
	var err error
	var brr BeginReplicationRequest

	vars := hummingbird.GetVars(request)

	policy, err := strconv.Atoi(request.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policy = 0
	}

	writer.WriteHeader(http.StatusOK)
	if hijacker, ok := writer.(http.Hijacker); !ok {
		hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Writer not a Hijacker")
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	} else if conn, rw, err = hijacker.Hijack(); err != nil {
		hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Hijack failed")
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	rc := &RepConn{rw: rw, c: conn}
	if err := rc.RecvMessage(&brr); err != nil {
		hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Error receiving BeginReplicationRequest: %v", err)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if !r.replicationMan.Begin(brr.Device, r.replicateTimeout) {
		hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Timed out waiting for concurrency slot")
		writer.WriteHeader(503)
		return
	}
	defer r.replicationMan.Done(brr.Device)
	var hashes map[string]string
	if brr.NeedHashes {
		hashes, err = GetHashes(r.driveRoot, brr.Device, brr.Partition, nil, r.reclaimAge, policy, hummingbird.GetLogger(request))
		if err != nil {
			hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Error getting hashes: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	if err := rc.SendMessage(BeginReplicationResponse{Hashes: hashes}); err != nil {
		hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Error sending BeginReplicationResponse: %v", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	for {
		errType, err := func() (string, error) { // this is a closure so we can use defers inside
			var sfr SyncFileRequest
			if err := rc.RecvMessage(&sfr); err != nil {
				return "receiving SyncFileRequest", err
			}
			if sfr.Done {
				return "", replicationDone
			}
			tempDir := TempDirPath(r.driveRoot, vars["device"])
			fileName := filepath.Join(r.driveRoot, sfr.Path)
			hashDir := filepath.Dir(fileName)

			if ext := filepath.Ext(fileName); (ext != ".data" && ext != ".ts" && ext != ".meta") || len(filepath.Base(filepath.Dir(fileName))) != 32 {
				return "invalid file path", rc.SendMessage(SyncFileResponse{Msg: "bad file path"})
			}
			if hummingbird.Exists(fileName) {
				return "file exists", rc.SendMessage(SyncFileResponse{Exists: true, Msg: "exists"})
			}
			dataFile, metaFile := ObjectFiles(hashDir)
			if filepath.Base(fileName) < filepath.Base(dataFile) || filepath.Base(fileName) < filepath.Base(metaFile) {
				return "newer file exists", rc.SendMessage(SyncFileResponse{NewerExists: true, Msg: "newer exists"})
			}
			tempFile, err := NewAtomicFileWriter(tempDir, hashDir)
			if err != nil {
				return "creating file writer", err
			}
			defer tempFile.Abandon()
			if err := tempFile.Preallocate(sfr.Size, r.reserve); err != nil {
				return "preallocating space", err
			}
			if xattrs, err := hex.DecodeString(sfr.Xattrs); err != nil || len(xattrs) == 0 {
				return "parsing xattrs", rc.SendMessage(SyncFileResponse{Msg: "bad xattrs"})
			} else if err := RawWriteMetadata(tempFile.Fd(), xattrs); err != nil {
				return "writing metadata", err
			}
			if err := rc.SendMessage(SyncFileResponse{GoAhead: true, Msg: "go ahead"}); err != nil {
				return "sending go ahead", err
			}
			if _, err := hummingbird.CopyN(rc, sfr.Size, tempFile); err != nil {
				return "copying data", err
			}
			if err := tempFile.Save(fileName); err != nil {
				return "saving file", err
			}
			if dataFile != "" || metaFile != "" {
				HashCleanupListDir(hashDir, r.reclaimAge)
			}
			InvalidateHash(hashDir)
			err = rc.SendMessage(FileUploadResponse{Success: true, Msg: "YAY"})
			return "file done", err
		}()
		if err == replicationDone {
			return
		} else if err != nil {
			hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Error replicating: %s. %v", errType, err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

func (r *Replicator) LogRequest(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		newWriter := &hummingbird.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
		requestLogger := &hummingbird.RequestLogger{Request: request, Logger: r.logger, W: newWriter}
		defer requestLogger.LogPanics("LOGGING REQUEST")
		start := time.Now()
		hummingbird.SetLogger(request, requestLogger)
		next.ServeHTTP(newWriter, request)
		if (request.Method != "REPLICATE" && request.Method != "REPCONN") || r.logLevel == "DEBUG" {
			r.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
				request.RemoteAddr,
				time.Now().Format("02/Jan/2006:15:04:05 -0700"),
				request.Method,
				hummingbird.Urlencode(request.URL.Path),
				newWriter.Status,
				hummingbird.GetDefault(newWriter.Header(), "Content-Length", "-"),
				hummingbird.GetDefault(request.Header, "Referer", "-"),
				hummingbird.GetDefault(request.Header, "X-Trans-Id", "-"),
				hummingbird.GetDefault(request.Header, "User-Agent", "-"),
				time.Since(start).Seconds(),
				"-"))
		}
	}
	return http.HandlerFunc(fn)
}

func (r *Replicator) GetHandler() http.Handler {
	commonHandlers := alice.New(middleware.ClearHandler, r.LogRequest, middleware.ValidateRequest)
	router := hummingbird.NewRouter()
	router.Get("/priorityrep", commonHandlers.ThenFunc(r.priorityRepHandler))
	router.Get("/progress", commonHandlers.ThenFunc(r.ProgressReportHandler))
	for _, policy := range hummingbird.LoadPolicies() {
		router.HandlePolicy("REPCONN", "/:device/:partition", policy.Index, commonHandlers.ThenFunc(r.objRepConnHandler))
		router.HandlePolicy("REPLICATE", "/:device/:partition/:suffixes", policy.Index, commonHandlers.ThenFunc(r.objReplicateHandler))
		router.HandlePolicy("REPLICATE", "/:device/:partition", policy.Index, commonHandlers.ThenFunc(r.objReplicateHandler))
	}
	router.Get("/debug", http.DefaultServeMux)
	return router
}

func (r *Replicator) startWebServer() {
	for {
		if sock, err := hummingbird.RetryListen(r.bindIp, r.port); err != nil {
			r.LogError("Listen failed: %v", err)
		} else {
			http.Serve(sock, r.GetHandler())
		}
	}
}

// Run a single replication pass. (NOTE: we will prob get rid of this because of priorityRepl)
func (r *Replicator) Run() {
	r.once = true
	r.run()
}

// Run replication passes in a loop until forever.
func (r *Replicator) RunForever() {
	go r.startWebServer()
	r.run()
}

func NewReplicator(serverconf hummingbird.Config, flags *flag.FlagSet) (hummingbird.Daemon, error) {
	if !serverconf.HasSection("object-replicator") {
		return nil, fmt.Errorf("Unable to find object-auditor config section")
	}

	replicator := &Replicator{
		priRepChans:            make(map[int]chan PriorityRepJob),
		deviceProgressMap:      make(map[string]*deviceProgress),
		deviceProgressPassInit: make(chan deviceProgress),
		deviceProgressIncr:     make(chan deviceProgress),
		devices:                make(map[string]bool),
		partitions:             make(map[string]bool),
		cancelers:              make(map[string]chan struct{}),
		once:                   false,
		LoopSleepTime:          5 * time.Second,
	}

	var err error
	replicator.hashPathPrefix, replicator.hashPathSuffix, err = hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, fmt.Errorf("Unable to get hash prefix and suffix")
	}
	replicator.Rings = make(map[int]hummingbird.Ring)
	for _, policy := range hummingbird.LoadPolicies() {
		if policy.Type != "replication" {
			continue
		}
		if replicator.Rings[policy.Index], err = hummingbird.GetRing("object", replicator.hashPathPrefix, replicator.hashPathSuffix, policy.Index); err != nil {
			return nil, fmt.Errorf("Unable to load ring.")
		}
	}
	replicator.reserve = serverconf.GetInt("object-replicator", "fallocate_reserve", 0)
	replicator.replicationMan = NewReplicationManager(serverconf.GetLimit("object-replicator", "replication_limit", 3, 100))
	replicator.replicateTimeout = time.Minute // TODO(redbo): does this need to be configurable?

	replicator.reconCachePath = serverconf.GetDefault("object-auditor", "recon_cache_path", "/var/cache/swift")
	replicator.checkMounts = serverconf.GetBool("object-replicator", "mount_check", true)
	replicator.driveRoot = serverconf.GetDefault("object-replicator", "devices", "/srv/node")
	replicator.port = int(serverconf.GetInt("object-replicator", "bind_port", 6500))
	replicator.bindIp = serverconf.GetDefault("object-replicator", "bind_ip", "0.0.0.0")
	replicator.quorumDelete = serverconf.GetBool("object-replicator", "quorum_delete", false)
	replicator.reclaimAge = int64(serverconf.GetInt("object-replicator", "reclaim_age", int64(hummingbird.ONE_WEEK)))
	replicator.logLevel = serverconf.GetDefault("object-replicator", "log_level", "INFO")
	replicator.logger = hummingbird.SetupLogger(serverconf.GetDefault("object-replicator", "log_facility", "LOG_LOCAL0"), "object-replicator", "")
	if serverconf.GetBool("object-replicator", "vm_test_mode", false) {
		replicator.timePerPart = time.Duration(serverconf.GetInt("object-replicator", "ms_per_part", 2000)) * time.Millisecond
	} else {
		replicator.timePerPart = time.Duration(serverconf.GetInt("object-replicator", "ms_per_part", 750)) * time.Millisecond
	}
	replicator.concurrency = int(serverconf.GetInt("object-replicator", "concurrency", 1))
	devices_flag := flags.Lookup("devices")
	if devices_flag != nil {
		if devices := devices_flag.Value.(flag.Getter).Get().(string); len(devices) > 0 {
			for _, devName := range strings.Split(devices, ",") {
				replicator.devices[strings.TrimSpace(devName)] = true
			}
		}
	}
	partitions_flag := flags.Lookup("partitions")
	if partitions_flag != nil {
		if partitions := partitions_flag.Value.(flag.Getter).Get().(string); len(partitions) > 0 {
			for _, part := range strings.Split(partitions, ",") {
				replicator.partitions[strings.TrimSpace(part)] = true
			}
		}
	}
	if !replicator.quorumDelete {
		quorumFlag := flags.Lookup("q")
		if quorumFlag != nil && quorumFlag.Value.(flag.Getter).Get() == true {
			replicator.quorumDelete = true
		}
	}
	statsdHost := serverconf.GetDefault("object-replicator", "log_statsd_host", "")
	if statsdHost != "" {
		statsdPort := serverconf.GetInt("object-replicator", "log_statsd_port", 8125)
		// Go metrics collection pause interval in seconds
		statsdPause := serverconf.GetInt("object-replicator", "statsd_collection_pause", 10)
		basePrefix := serverconf.GetDefault("object-replicator", "log_statsd_metric_prefix", "")
		prefix := basePrefix + ".go.objectreplicator"
		go hummingbird.CollectRuntimeMetrics(statsdHost, statsdPort, statsdPause, prefix)
	}
	return replicator, nil
}

// ReplicationManager is used by the object server to limit replication concurrency
type ReplicationManager struct {
	lock         sync.Mutex
	devSem       map[string]chan struct{}
	totalSem     chan struct{}
	limitPerDisk int64
	limitOverall int64
}

// Begin gives or rejects permission for a new replication session on the given device.
func (r *ReplicationManager) Begin(device string, timeout time.Duration) bool {
	r.lock.Lock()
	devSem, ok := r.devSem[device]
	if !ok {
		devSem = make(chan struct{}, r.limitPerDisk)
		r.devSem[device] = devSem
	}
	r.lock.Unlock()
	timeoutTimer := time.NewTicker(timeout)
	defer timeoutTimer.Stop()
	loopTimer := time.NewTicker(time.Millisecond * 10)
	defer loopTimer.Stop()
	for {
		select {
		case devSem <- struct{}{}:
			select {
			case r.totalSem <- struct{}{}:
				return true
			case <-loopTimer.C:
				<-devSem
			}
		case <-timeoutTimer.C:
			return false
		}
	}
}

// Done marks the session completed, removing it from any accounting.
func (r *ReplicationManager) Done(device string) {
	r.lock.Lock()
	<-r.devSem[device]
	<-r.totalSem
	r.lock.Unlock()
}

func NewReplicationManager(limitPerDisk int64, limitOverall int64) *ReplicationManager {
	return &ReplicationManager{
		limitPerDisk: limitPerDisk,
		limitOverall: limitOverall,
		devSem:       make(map[string]chan struct{}),
		totalSem:     make(chan struct{}, limitOverall),
	}
}
