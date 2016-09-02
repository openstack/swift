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
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

var StatsReportInterval = 300 * time.Second
var TmpEmptyTime = 24 * time.Hour
var ReplicateDeviceTimeout = 4 * time.Hour

type PriorityRepJob struct {
	Partition  uint64                `json:"partition"`
	FromDevice *hummingbird.Device   `json:"from_device"`
	ToDevices  []*hummingbird.Device `json:"to_devices"`
	Policy     int                   `json:"policy"`
}

// minimal ring interface for replication
type replicationRing interface {
	GetJobNodes(partition uint64, localDevice int) (response []*hummingbird.Device, handoff bool)
	GetMoreNodes(partition uint64) hummingbird.MoreNodes
	LocalDevices(localPort int) (devs []*hummingbird.Device, err error)
}

type quarantineFileError struct {
	msg string
}

func (q quarantineFileError) Error() string {
	return q.msg
}

func deviceKey(dev *hummingbird.Device, policy int) string {
	if policy == 0 {
		return dev.Device
	}
	return fmt.Sprintf("%s-%d", dev.Device, policy)
}

func getFile(filePath string) (fp *os.File, xattrs []byte, size int64, err error) {
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

type ReplicationDeviceStats struct {
	Stats            map[string]int64
	LastCheckin      time.Time
	RunStarted       time.Time
	DeviceStarted    time.Time
	LastPassDuration time.Duration
	TotalPasses      int64
}

type ReplicationDevice interface {
	Replicate()
	ReplicateLoop()
	Key() string
	Cancel()
	PriorityReplicate(pri PriorityRepJob, timeout time.Duration) bool
	Stats() *ReplicationDeviceStats
}

type replicationDevice struct {
	// If you have a better way to make struct methods that are overridable for tests, please call my house.
	i interface {
		beginReplication(dev *hummingbird.Device, partition string, hashes bool, rChan chan beginReplicationResponse)
		listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool)
		syncFile(objFile string, dst []*syncFileArg) (syncs int, insync int, err error)
		replicateLocal(partition string, nodes []*hummingbird.Device, moreNodes hummingbird.MoreNodes)
		replicateHandoff(partition string, nodes []*hummingbird.Device)
		cleanTemp()
		listPartitions() ([]string, error)
		replicatePartition(partition string)
	}
	r      *Replicator
	dev    *hummingbird.Device
	policy int
	cancel chan struct{}
	priRep chan PriorityRepJob
	stats  ReplicationDeviceStats
}

func (rd *replicationDevice) Stats() *ReplicationDeviceStats {
	return &rd.stats
}

type statUpdate struct {
	deviceKey string
	stat      string
	value     int64
}

func (rd *replicationDevice) updateStat(stat string, amount int64) {
	rd.r.updateStat <- statUpdate{rd.Key(), stat, amount}
}

type beginReplicationResponse struct {
	dev    *hummingbird.Device
	conn   RepConn
	hashes map[string]string
	err    error
}

func (rd *replicationDevice) listObjFiles(objChan chan string, cancel chan struct{}, partdir string, needSuffix func(string) bool) {
	defer close(objChan)
	suffixDirs, err := filepath.Glob(filepath.Join(partdir, "[a-f0-9][a-f0-9][a-f0-9]"))
	if err != nil {
		rd.r.LogError("[listObjFiles] %v", err)
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
			rd.r.LogError("[listObjFiles] %v", err)
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
				rd.r.LogError("[listObjFiles] %v", err)
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
	conn RepConn
	dev  *hummingbird.Device
}

func (rd *replicationDevice) syncFile(objFile string, dst []*syncFileArg) (syncs int, insync int, err error) {
	var wrs []*syncFileArg
	lst := strings.Split(objFile, string(os.PathSeparator))
	relPath := filepath.Join(lst[len(lst)-5:]...)
	fp, xattrs, fileSize, err := getFile(objFile)
	if _, ok := err.(quarantineFileError); ok {
		hashDir := filepath.Dir(objFile)
		rd.r.LogError("[syncFile] %s failed audit and is being quarantined: %s", hashDir, err.Error())
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
				rd.r.LogError("Failed to write to remoteDevice: %d, %v", sfa.dev.Id, err)
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
				rd.updateStat("FilesSent", 1)
				rd.updateStat("BytesSent", fileSize)
			}
		}
	}
	return syncs, insync, nil
}

func (rd *replicationDevice) beginReplication(dev *hummingbird.Device, partition string, hashes bool, rChan chan beginReplicationResponse) {
	var brr BeginReplicationResponse
	if rc, err := NewRepConn(dev, partition, rd.policy); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else if err := rc.SendMessage(BeginReplicationRequest{Device: dev.Device, Partition: partition, NeedHashes: hashes}); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else if err := rc.RecvMessage(&brr); err != nil {
		rChan <- beginReplicationResponse{dev: dev, err: err}
	} else {
		rChan <- beginReplicationResponse{dev: dev, conn: rc, hashes: brr.Hashes}
	}
}

func (rd *replicationDevice) replicateLocal(partition string, nodes []*hummingbird.Device, moreNodes hummingbird.MoreNodes) {
	path := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy), partition)
	syncCount := 0
	startGetHashesRemote := time.Now()
	remoteHashes := make(map[int]map[string]string)
	remoteConnections := make(map[int]RepConn)
	rChan := make(chan beginReplicationResponse)
	for _, dev := range nodes {
		go rd.i.beginReplication(dev, partition, true, rChan)
	}
	for i := 0; i < len(nodes); i++ {
		rData := <-rChan
		if rData.err == nil {
			defer rData.conn.Close()
			remoteHashes[rData.dev.Id] = rData.hashes
			remoteConnections[rData.dev.Id] = rData.conn
		} else if rData.err == RepUnmountedError {
			if nextNode := moreNodes.Next(); nextNode != nil {
				go rd.i.beginReplication(nextNode, partition, true, rChan)
				nodes = append(nodes, nextNode)
			} else {
				break
			}
		}
	}
	if len(remoteHashes) == 0 {
		return
	}

	timeGetHashesRemote := float64(time.Now().Sub(startGetHashesRemote)) / float64(time.Second)
	startGetHashesLocal := time.Now()

	recalc := []string{}
	hashes, err := GetHashes(rd.r.deviceRoot, rd.dev.Device, partition, recalc, rd.r.reclaimAge, rd.policy, rd.r)
	if err != nil {
		rd.r.LogError("[replicateLocal] error getting local hashes: %v", err)
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
	hashes, err = GetHashes(rd.r.deviceRoot, rd.dev.Device, partition, recalc, rd.r.reclaimAge, rd.policy, rd.r)
	if err != nil {
		rd.r.LogError("[replicateLocal] error recalculating local hashes: %v", err)
		return
	}
	timeGetHashesLocal := float64(time.Now().Sub(startGetHashesLocal)) / float64(time.Second)

	objChan := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go rd.i.listObjFiles(objChan, cancel, path, func(suffix string) bool {
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
				if remoteConnections[dev.Id].Disconnected() {
					continue
				}
				toSync = append(toSync, &syncFileArg{conn: remoteConnections[dev.Id], dev: dev})
			}
		}
		if syncs, _, err := rd.i.syncFile(objFile, toSync); err == nil {
			syncCount += syncs
		} else {
			rd.r.LogError("[syncFile] %v", err)
			return
		}
	}
	for _, conn := range remoteConnections {
		if !conn.Disconnected() {
			conn.SendMessage(SyncFileRequest{Done: true})
		}
	}
	timeSyncing := float64(time.Now().Sub(startSyncing)) / float64(time.Second)
	if syncCount > 0 {
		rd.r.LogInfo("[replicateLocal] Partition %s synced %d files (%.2fs / %.2fs / %.2fs)", path, syncCount, timeGetHashesRemote, timeGetHashesLocal, timeSyncing)
	}
}

func (rd *replicationDevice) replicateHandoff(partition string, nodes []*hummingbird.Device) {
	path := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy), partition)
	syncCount := 0
	remoteConnections := make(map[int]RepConn)
	rChan := make(chan beginReplicationResponse)
	for _, dev := range nodes {
		go rd.i.beginReplication(dev, partition, false, rChan)
	}
	for i := 0; i < len(nodes); i++ {
		rData := <-rChan
		if rData.err == nil {
			defer rData.conn.Close()
			remoteConnections[rData.dev.Id] = rData.conn
		}
	}
	if len(remoteConnections) == 0 {
		return
	}

	objChan := make(chan string, 100)
	cancel := make(chan struct{})
	defer close(cancel)
	go rd.i.listObjFiles(objChan, cancel, path, func(string) bool { return true })
	for objFile := range objChan {
		toSync := make([]*syncFileArg, 0)
		for _, dev := range nodes {
			if remoteConnections[dev.Id] != nil && !remoteConnections[dev.Id].Disconnected() {
				toSync = append(toSync, &syncFileArg{conn: remoteConnections[dev.Id], dev: dev})
			}
		}
		if syncs, insync, err := rd.i.syncFile(objFile, toSync); err == nil {
			syncCount += syncs

			success := insync == len(nodes)
			if rd.r.quorumDelete {
				success = insync >= len(nodes)/2+1
			}
			if success {
				os.Remove(objFile)
				os.Remove(filepath.Dir(objFile))
			}
		} else {
			rd.r.LogError("[syncFile] %v", err)
		}
	}
	for _, conn := range remoteConnections {
		if !conn.Disconnected() {
			conn.SendMessage(SyncFileRequest{Done: true})
		}
	}
	if syncCount > 0 {
		rd.r.LogInfo("[replicateHandoff] Partition %s synced %d files", path, syncCount)
	}
}

func (rd *replicationDevice) Key() string {
	return deviceKey(rd.dev, rd.policy)
}

func (rd *replicationDevice) cleanTemp() {
	tempDir := TempDirPath(rd.r.deviceRoot, rd.dev.Device)
	if tmpContents, err := ioutil.ReadDir(tempDir); err == nil {
		for _, tmpEntry := range tmpContents {
			if time.Since(tmpEntry.ModTime()) > TmpEmptyTime {
				os.RemoveAll(filepath.Join(tempDir, tmpEntry.Name()))
			}
		}
	}
}

func (rd *replicationDevice) replicatePartition(partition string) {
	rd.r.concurrencySem <- struct{}{}
	defer func() {
		<-rd.r.concurrencySem
	}()
	partitioni, err := strconv.ParseUint(partition, 10, 64)
	if err != nil {
		return
	}
	nodes, handoff := rd.r.Rings[rd.policy].GetJobNodes(partitioni, rd.dev.Id)
	if handoff {
		rd.i.replicateHandoff(partition, nodes)
	} else {
		rd.i.replicateLocal(partition, nodes, rd.r.Rings[rd.policy].GetMoreNodes(partitioni))
	}
	rd.updateStat("PartitionsDone", 1)
}

func (rd *replicationDevice) listPartitions() ([]string, error) {
	objPath := filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy))
	partitions, err := filepath.Glob(filepath.Join(objPath, "[0-9]*"))
	if err != nil {
		return nil, err
	}
	partitionList := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		partition = filepath.Base(partition)
		if len(rd.r.partitions) > 0 && !rd.r.partitions[partition] {
			continue
		}
		if _, err := strconv.ParseUint(partition, 10, 64); err == nil {
			partitionList = append(partitionList, partition)
		}
	}
	for i := len(partitionList) - 1; i > 0; i-- { // shuffle partition list
		j := rand.Intn(i + 1)
		partitionList[j], partitionList[i] = partitionList[i], partitionList[j]
	}
	return partitionList, nil
}

func (rd *replicationDevice) Replicate() {
	defer rd.r.LogPanics(fmt.Sprintf("PANIC REPLICATING DEVICE: %s", rd.dev.Device))
	rd.updateStat("startRun", 1)
	if mounted, err := hummingbird.IsMount(filepath.Join(rd.r.deviceRoot, rd.dev.Device)); rd.r.checkMounts && (err != nil || mounted != true) {
		rd.r.LogError("[replicateDevice] Drive not mounted: %s", rd.dev.Device)
		return
	}
	if hummingbird.Exists(filepath.Join(rd.r.deviceRoot, rd.dev.Device, "lock_device")) {
		return
	}

	rd.i.cleanTemp()

	partitionList, err := rd.i.listPartitions()
	if err != nil {
		rd.r.LogError("[replicateDevice] Error getting partition list: %s (%v)", rd.dev.Device, err)
		return
	} else if len(partitionList) == 0 {
		rd.r.LogError("[replicateDevice] No partitions found: %s", filepath.Join(rd.r.deviceRoot, rd.dev.Device, PolicyDir(rd.policy)))
		return
	}
	rd.updateStat("PartitionsTotal", int64(len(partitionList)))

	for _, partition := range partitionList {
		rd.updateStat("checkin", 1)
		select {
		case <-rd.cancel:
			{
				rd.r.LogError("replicateDevice canceled for device: %s", rd.dev.Device)
				return
			}
		default:
		}
		rd.processPriorityJobs()
		rd.i.replicatePartition(partition)
	}
	rd.updateStat("FullReplicateCount", 1)
}

func (rd *replicationDevice) Cancel() {
	close(rd.cancel)
}

func (rd *replicationDevice) ReplicateLoop() {
	for {
		select {
		case <-rd.cancel:
			return
		default:
			rd.Replicate()
		}
		time.Sleep(rd.r.loopSleepTime)
	}
}

type NoMoreNodes struct{}

func (n *NoMoreNodes) Next() *hummingbird.Device {
	return nil
}

func (rd *replicationDevice) PriorityReplicate(pri PriorityRepJob, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case rd.priRep <- pri:
		return true
	case <-timer.C:
		return false
	}
}

// processPriorityJobs runs any pending priority jobs given the device's id
func (rd *replicationDevice) processPriorityJobs() {
	for {
		select {
		case pri := <-rd.priRep:
			func() {
				time.Sleep(rd.r.partSleepTime)
				rd.r.concurrencySem <- struct{}{}
				defer func() {
					<-rd.r.concurrencySem
				}()
				partition := strconv.FormatUint(pri.Partition, 10)
				_, handoff := rd.r.Rings[rd.policy].GetJobNodes(pri.Partition, pri.FromDevice.Id)
				toDevicesArr := make([]string, len(pri.ToDevices))
				for i, s := range pri.ToDevices {
					toDevicesArr[i] = fmt.Sprintf("%s:%d/%s", s.Ip, s.Port, s.Device)
				}
				jobType := "local"
				if handoff {
					jobType = "handoff"
				}
				rd.r.LogInfo("PriorityReplicationJob. Partition: %d as %s from %s to %s", pri.Partition, jobType, pri.FromDevice.Device, strings.Join(toDevicesArr, ","))
				if handoff {
					rd.i.replicateHandoff(partition, pri.ToDevices)
				} else {
					rd.i.replicateLocal(partition, pri.ToDevices, &NoMoreNodes{})
				}
			}()
			rd.updateStat("PriorityRepsDone", 1)
		default:
			return
		}
	}
}

var newReplicationDevice = func(dev *hummingbird.Device, policy int, r *Replicator) *replicationDevice {
	rd := &replicationDevice{
		r:      r,
		dev:    dev,
		policy: policy,
		cancel: make(chan struct{}),
		priRep: make(chan PriorityRepJob),
		stats: ReplicationDeviceStats{
			LastCheckin:   time.Now(),
			DeviceStarted: time.Now(),
			Stats: map[string]int64{
				"PartitionsDone":   0,
				"PartitionsTotal":  0,
				"FilesSent":        0,
				"BytesSent":        0,
				"PriorityRepsDone": 0,
			},
		},
	}
	rd.i = rd
	return rd
}

// Object replicator daemon object
type Replicator struct {
	checkMounts        bool
	deviceRoot         string
	reconCachePath     string
	logger             hummingbird.SysLogLike
	logLevel           string
	port               int
	bindIp             string
	Rings              map[int]replicationRing
	runningDevices     map[string]ReplicationDevice
	cancelCounts       map[string]int64
	runningDevicesLock sync.Mutex
	devices            map[string]bool
	partitions         map[string]bool
	concurrency        int
	concurrencySem     chan struct{}
	updateStat         chan statUpdate
	reclaimAge         int64
	quorumDelete       bool
	reserve            int64
	replicationMan     *ReplicationManager
	replicateTimeout   time.Duration
	onceDone           chan struct{}
	onceWaiting        int64
	loopSleepTime      time.Duration
	partSleepTime      time.Duration
}

func (r *Replicator) cancelStalledDevices() {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	for key, rd := range r.runningDevices {
		stats := rd.Stats()
		if time.Since(stats.LastCheckin) > ReplicateDeviceTimeout {
			rd.Cancel()
			r.cancelCounts[key] += 1
			delete(r.runningDevices, key)
		}
	}
}

func (r *Replicator) verifyRunningDevices() {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	expectedDevices := make(map[string]bool)
	for policy, ring := range r.Rings {
		ringDevices, err := ring.LocalDevices(r.port)
		if err != nil {
			r.LogError("Error getting local devices from ring: %v", err)
			return
		}
		// look for devices that aren't running but should be
		for _, dev := range ringDevices {
			expectedDevices[deviceKey(dev, policy)] = true
			if len(r.devices) > 0 && !r.devices[dev.Device] {
				continue
			}
			if _, ok := r.runningDevices[deviceKey(dev, policy)]; !ok {
				r.runningDevices[deviceKey(dev, policy)] = newReplicationDevice(dev, policy, r)
				go r.runningDevices[deviceKey(dev, policy)].ReplicateLoop()
			}
		}
	}
	// look for devices that are running but shouldn't be
	for key, rd := range r.runningDevices {
		if _, found := expectedDevices[key]; !found {
			rd.Cancel()
			delete(r.runningDevices, key)
		}
	}
}

func (r *Replicator) reportStats() {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	var totalDuration time.Duration
	var maxLastPass time.Time
	var doneParts, totalParts int64
	var processingTime float64
	allHaveCompleted := true
	for _, rd := range r.runningDevices {
		stats := rd.Stats()
		if stats.TotalPasses <= 1 {
			allHaveCompleted = false
		}
		if maxLastPass.Before(stats.RunStarted) {
			maxLastPass = stats.RunStarted
		}
		totalDuration += stats.LastPassDuration
		totalParts += stats.Stats["PartitionsTotal"]
		doneParts += stats.Stats["PartitionsDone"]
		processingTime += time.Since(stats.RunStarted).Seconds()
	}
	if processingTime > 0 {
		partsPerSecond := float64(doneParts) / processingTime
		remaining := time.Duration((1.0 / partsPerSecond) * float64(time.Second) *
			float64(totalParts-doneParts) / float64(len(r.runningDevices)))
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
			processingTime, partsPerSecond, remainingStr)
	}

	if allHaveCompleted {
		hummingbird.DumpReconCache(r.reconCachePath, "object",
			map[string]interface{}{
				"object_replication_time": float64(totalDuration) / float64(len(r.runningDevices)) / float64(time.Second),
				"object_replication_last": float64(maxLastPass.UnixNano()) / float64(time.Second),
			})
	}
}

func (r *Replicator) priorityReplicate(pri PriorityRepJob, timeout time.Duration) bool {
	r.runningDevicesLock.Lock()
	rd, ok := r.runningDevices[deviceKey(pri.FromDevice, pri.Policy)]
	r.runningDevicesLock.Unlock()
	if ok {
		return rd.PriorityReplicate(pri, timeout)
	}
	return false
}

func (r *Replicator) getDeviceProgress() map[string]map[string]interface{} {
	r.runningDevicesLock.Lock()
	defer r.runningDevicesLock.Unlock()
	deviceProgress := make(map[string]map[string]interface{})
	for key, device := range r.runningDevices {
		stats := device.Stats()
		deviceProgress[key] = map[string]interface{}{
			"StartDate":        stats.DeviceStarted,
			"LastUpdate":       stats.LastCheckin,
			"LastPassDuration": stats.LastPassDuration,
			"LastPassUpdate":   stats.RunStarted,
			"TotalPasses":      stats.TotalPasses,
			"CancelCount":      r.cancelCounts[key],
		}
		for k, v := range stats.Stats {
			deviceProgress[key][k] = v
		}
	}
	return deviceProgress
}

func (r *Replicator) runLoopCheck(reportTimer <-chan time.Time) {
	select {
	case update := <-r.updateStat:
		r.runningDevicesLock.Lock()
		defer r.runningDevicesLock.Unlock()
		if rd, ok := r.runningDevices[update.deviceKey]; ok {
			stats := rd.Stats()
			if update.stat == "checkin" {
				stats.LastCheckin = time.Now()
			} else if update.stat == "startRun" {
				stats.TotalPasses++
				stats.LastPassDuration = time.Since(stats.RunStarted)
				stats.RunStarted = time.Now()
				stats.LastCheckin = time.Now()
				for k := range stats.Stats {
					stats.Stats[k] = 0
				}
			} else {
				stats.Stats[update.stat] += update.value
			}
		}
	case <-reportTimer:
		r.cancelStalledDevices()
		r.verifyRunningDevices()
		r.reportStats()
	case <-r.onceDone:
		r.onceWaiting--
	}
}

// Run replication passes in a loop until forever.
func (r *Replicator) RunForever() {
	go r.startWebServer()
	reportTimer := time.NewTimer(StatsReportInterval)
	r.verifyRunningDevices()
	for {
		r.runLoopCheck(reportTimer.C)
	}
}

// Run a single replication pass. (NOTE: we will prob get rid of this because of priorityRepl)
func (r *Replicator) Run() {
	for policy, ring := range r.Rings {
		devices, err := ring.LocalDevices(r.port)
		if err != nil {
			r.LogError("Error getting local devices from ring: %v", err)
			return
		}
		for _, dev := range devices {
			rd := newReplicationDevice(dev, policy, r)
			key := rd.Key()
			r.runningDevices[key] = rd
			r.onceWaiting++
			go func(dev *hummingbird.Device, key string) {
				r.runningDevices[key].Replicate()
				r.onceDone <- struct{}{}
			}(dev, key)
		}
	}
	for r.onceWaiting > 0 {
		r.runLoopCheck(make(chan time.Time))
	}
	r.reportStats()
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

func NewReplicator(serverconf hummingbird.Config, flags *flag.FlagSet) (hummingbird.Daemon, error) {
	if !serverconf.HasSection("object-replicator") {
		return nil, fmt.Errorf("Unable to find object-replicator config section")
	}
	concurrency := int(serverconf.GetInt("object-replicator", "concurrency", 1))

	replicator := &Replicator{
		runningDevices:   make(map[string]ReplicationDevice),
		cancelCounts:     make(map[string]int64),
		reserve:          serverconf.GetInt("object-replicator", "fallocate_reserve", 0),
		replicationMan:   NewReplicationManager(serverconf.GetLimit("object-replicator", "replication_limit", 3, 100)),
		replicateTimeout: time.Minute, // TODO(redbo): does this need to be configurable?
		reconCachePath:   serverconf.GetDefault("object-replicator", "recon_cache_path", "/var/cache/swift"),
		checkMounts:      serverconf.GetBool("object-replicator", "mount_check", true),
		deviceRoot:       serverconf.GetDefault("object-replicator", "devices", "/srv/node"),
		port:             int(serverconf.GetInt("object-replicator", "bind_port", 6500)),
		bindIp:           serverconf.GetDefault("object-replicator", "bind_ip", "0.0.0.0"),
		quorumDelete:     serverconf.GetBool("object-replicator", "quorum_delete", false),
		reclaimAge:       int64(serverconf.GetInt("object-replicator", "reclaim_age", int64(hummingbird.ONE_WEEK))),
		logger:           hummingbird.SetupLogger(serverconf.GetDefault("object-replicator", "log_facility", "LOG_LOCAL0"), "object-replicator", ""),
		logLevel:         serverconf.GetDefault("object-replicator", "log_level", "INFO"),
		Rings:            make(map[int]replicationRing),
		concurrency:      concurrency,
		concurrencySem:   make(chan struct{}, concurrency),
		updateStat:       make(chan statUpdate),
		devices:          make(map[string]bool),
		partitions:       make(map[string]bool),
		onceDone:         make(chan struct{}),
		loopSleepTime:    time.Second * 30,
		partSleepTime:    time.Duration(serverconf.GetInt("object-replicator", "ms_per_part", 100)) * time.Millisecond,
	}

	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, fmt.Errorf("Unable to get hash prefix and suffix")
	}
	for _, policy := range hummingbird.LoadPolicies() {
		if policy.Type != "replication" {
			continue
		}
		if replicator.Rings[policy.Index], err = hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix, policy.Index); err != nil {
			return nil, fmt.Errorf("Unable to load ring.")
		}
	}
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
	if serverconf.GetBool("object-replicator", "vm_test_mode", false) { // slow down the replicator in saio mode
		replicator.partSleepTime = time.Duration(serverconf.GetInt("object-replicator", "ms_per_part", 500)) * time.Millisecond
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
