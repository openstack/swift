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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

var ReplicationSessionTimeout = 60 * time.Second
var RunForeverInterval = 30 * time.Second
var StatsReportInterval = 300 * time.Second
var TmpEmptyTime = 24 * time.Hour

// Encapsulates a partition for replication.
type job struct {
	dev       *hummingbird.Device
	partition string
	objPath   string
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
	JobType    string                `json:"job_type"`
	Partition  uint64                `json:"partition"`
	FromDevice *hummingbird.Device   `json:"from_device"`
	ToDevices  []*hummingbird.Device `json:"to_devices"`
}

// Object replicator daemon object
type Replicator struct {
	concurrency    int
	checkMounts    bool
	driveRoot      string
	reconCachePath string
	bindPort       int
	bindIp         string
	logger         hummingbird.SysLogLike
	port           int
	Ring           hummingbird.Ring
	devGroup       sync.WaitGroup
	partRateTicker *time.Ticker
	timePerPart    time.Duration
	concurrencySem chan struct{}
	devices        []string
	partitions     []string
	priRepChans    map[int]chan PriorityRepJob
	priRepM        sync.Mutex

	/* stats accounting */
	startTime                                                     time.Time
	replicationCount, jobCount, dataTransferred, filesTransferred uint64
	replicationCountIncrement, jobCountIncrement, dataTransferAdd chan uint64
	partitionTimes                                                sort.Float64Slice
	partitionTimesAdd                                             chan float64
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

var quarantineFileError = fmt.Errorf("Invalid file")

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
		return nil, nil, 0, quarantineFileError
	}
	rawxattr, err := RawReadMetadata(fp.Fd())
	if err != nil || len(rawxattr) == 0 {
		return nil, nil, 0, quarantineFileError
	}

	// Perform a mini-audit, since it's cheap and we can potentially avoid spreading bad data around.
	v, err := hummingbird.PickleLoads(rawxattr)
	if err != nil {
		return nil, nil, 0, quarantineFileError
	}
	metadata, ok := v.(map[interface{}]interface{})
	if !ok {
		return nil, nil, 0, quarantineFileError
	}
	for key, value := range metadata {
		if _, ok := key.(string); !ok {
			return nil, nil, 0, quarantineFileError
		}
		if _, ok := value.(string); !ok {
			return nil, nil, 0, quarantineFileError
		}
	}
	switch filepath.Ext(filePath) {
	case ".data":
		for _, reqEntry := range []string{"Content-Length", "Content-Type", "name", "ETag", "X-Timestamp"} {
			if _, ok := metadata[reqEntry]; !ok {
				return nil, nil, 0, quarantineFileError
			}
		}
		if contentLength, err := strconv.ParseInt(metadata["Content-Length"].(string), 10, 64); err != nil || contentLength != finfo.Size() {
			return nil, nil, 0, quarantineFileError
		}
	case ".ts":
		for _, reqEntry := range []string{"name", "X-Timestamp"} {
			if _, ok := metadata[reqEntry]; !ok {
				return nil, nil, 0, quarantineFileError
			}
		}
	}
	return fp, rawxattr, finfo.Size(), nil
}

func (r *Replicator) beginReplication(dev *hummingbird.Device, partition string, hashes bool, rChan chan ReplicationData) {
	rc, err := NewRepConn(dev.ReplicationIp, dev.ReplicationPort, dev.Device, partition)
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

func listObjFiles(partdir string, needSuffix func(string) bool) ([]string, error) {
	var objFiles []string
	suffixDirs, err := filepath.Glob(filepath.Join(partdir, "[a-f0-9][a-f0-9][a-f0-9]"))
	if err != nil {
		return nil, err
	}
	if len(suffixDirs) == 0 {
		os.Remove(filepath.Join(partdir, ".lock"))
		os.Remove(filepath.Join(partdir, "hashes.pkl"))
		os.Remove(partdir)
		return nil, nil
	}
	for _, suffDir := range suffixDirs {
		if !needSuffix(filepath.Base(suffDir)) {
			continue
		}
		hashDirs, err := filepath.Glob(filepath.Join(suffDir, "????????????????????????????????"))
		if err != nil {
			return nil, err
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
				return nil, err
			}
			for _, objFile := range fileList {
				objFiles = append(objFiles, objFile)
			}
		}
	}
	return objFiles, nil
}

type syncFileArg struct {
	conn *RepConn
	dev  *hummingbird.Device
}

func (r *Replicator) syncFile(objFile string, dst []*syncFileArg) (syncs int, insync int, err error) {
	var wrs []*syncFileArg
	lst := strings.Split(objFile, string(os.PathSeparator))
	relPath := filepath.Join(lst[len(lst)-5:]...)
	fp, xattrs, fileSize, err := r.getFile(objFile)
	if err == quarantineFileError {
		// TODO: quarantine
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
		for _, sfa := range wrs {
			sfa.conn.Write(scratch[0:length])
		}
	}
	if totalRead != fileSize {
		return 0, 0, fmt.Errorf("Failed to read the full file.")
	}

	// get file upload results
	for _, sfa := range wrs {
		var fur FileUploadResponse
		sfa.conn.Flush()
		if sfa.conn.RecvMessage(&fur) == nil {
			if fur.Success {
				r.dataTransferAdd <- uint64(fileSize)
				syncs++
				insync++
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
		go r.beginReplication(nodes[i], j.partition, true, rChan)
	}
	for i := 0; i < len(nodes); i++ {
		rData := <-rChan
		if rData.err == nil {
			defer rData.conn.Close()
			remoteHashes[rData.dev.Id] = rData.hashes
			remoteConnections[rData.dev.Id] = rData.conn
		} else if rData.err == RepUnmountedError {
			if nextNode := moreNodes.Next(); nextNode != nil {
				go r.beginReplication(nextNode, j.partition, true, rChan)
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
	hashes, herr := GetHashes(r.driveRoot, j.dev.Device, j.partition, recalc, r)
	if herr != nil {
		r.LogError("[replicateLocal] error getting local hashes: %v", herr)
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
	hashes, herr = GetHashes(r.driveRoot, j.dev.Device, j.partition, recalc, r)
	if herr != nil {
		r.LogError("[replicateLocal] error recalculating local hashes: %v", herr)
		return
	}
	timeGetHashesLocal := float64(time.Now().Sub(startGetHashesLocal)) / float64(time.Second)

	objFiles, err := listObjFiles(path, func(suffix string) bool {
		for _, remoteHash := range remoteHashes {
			if hashes[suffix] != remoteHash[suffix] {
				return true
			}
		}
		return false
	})
	if err != nil {
		r.LogError("[listObjFiles] %v", err)
	}
	startSyncing := time.Now()
	for _, objFile := range objFiles {
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
		if syncs, _, err := r.syncFile(objFile, toSync); err == nil {
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
		go r.beginReplication(nodes[i], j.partition, false, rChan)
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

	objFiles, err := listObjFiles(path, func(string) bool { return true })
	if err != nil {
		r.LogError("[listObjFiles] %v", err)
	}
	for _, objFile := range objFiles {
		toSync := make([]*syncFileArg, 0)
		for _, dev := range nodes {
			if remoteAvailable[dev.Id] && !remoteConnections[dev.Id].Disconnected {
				toSync = append(toSync, &syncFileArg{conn: remoteConnections[dev.Id], dev: dev})
			}
		}
		if syncs, insync, err := r.syncFile(objFile, toSync); err == nil {
			syncCount += syncs
			if insync == len(nodes) {
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
	tmpPath := filepath.Join(r.driveRoot, dev.Device, "tmp")
	if tmpContents, err := ioutil.ReadDir(tmpPath); err == nil {
		for _, tmpEntry := range tmpContents {
			if time.Since(tmpEntry.ModTime()) > TmpEmptyTime {
				os.RemoveAll(filepath.Join(tmpPath, tmpEntry.Name()))
			}
		}
	}
}

func (r *Replicator) replicateDevice(dev *hummingbird.Device) {
	defer r.LogPanics("PANIC REPLICATING DEVICE")
	defer r.devGroup.Done()

	r.cleanTemp(dev)

	if mounted, err := hummingbird.IsMount(filepath.Join(r.driveRoot, dev.Device)); r.checkMounts && (err != nil || mounted != true) {
		r.LogError("[replicateDevice] Drive not mounted: %s", dev.Device)
		return
	}
	objPath := filepath.Join(r.driveRoot, dev.Device, "objects")
	if fi, err := os.Stat(objPath); err != nil || !fi.Mode().IsDir() {
		r.LogError("[replicateDevice] No objects found: %s", objPath)
		return
	}
	partitionList, err := filepath.Glob(filepath.Join(objPath, "[0-9]*"))
	if err != nil {
		r.LogError("[replicateDevice] Error getting partition list: %s (%v)", objPath, err)
		return
	}
	for i := len(partitionList) - 1; i > 0; i-- { // shuffle partition list
		j := rand.Intn(i + 1)
		partitionList[j], partitionList[i] = partitionList[i], partitionList[j]
	}
	r.jobCountIncrement <- uint64(len(partitionList))
	for _, partition := range partitionList {
		if hummingbird.Exists(filepath.Join(r.driveRoot, dev.Device, "lock_device")) {
			break
		}
		r.processPriorityJobs(dev.Id)
		if len(r.partitions) > 0 {
			found := false
			for _, p := range r.partitions {
				if filepath.Base(partition) == p {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		if partitioni, err := strconv.ParseUint(filepath.Base(partition), 10, 64); err == nil {
			func() {
				<-r.partRateTicker.C
				r.concurrencySem <- struct{}{}
				r.replicationCountIncrement <- 1
				j := &job{objPath: objPath, partition: filepath.Base(partition), dev: dev}
				nodes, handoff := r.Ring.GetJobNodes(partitioni, j.dev.Id)
				partStart := time.Now()
				defer func() {
					<-r.concurrencySem
					r.partitionTimesAdd <- float64(time.Since(partStart)) / float64(time.Second)
				}()
				if handoff {
					r.replicateHandoff(j, nodes)
				} else {
					r.replicateLocal(j, nodes, r.Ring.GetMoreNodes(partitioni))
				}
			}()
		}
	}
}

// Collect and log replication stats - runs in a goroutine launched by run(), runs for the duration of a replication pass.
func (r *Replicator) statsReporter(c <-chan time.Time) {
	for {
		select {
		case dt := <-r.dataTransferAdd:
			r.dataTransferred += dt
			r.filesTransferred += 1
		case jobs := <-r.jobCountIncrement:
			r.jobCount += jobs
		case replicates := <-r.replicationCountIncrement:
			r.replicationCount += replicates
		case partitionTime := <-r.partitionTimesAdd:
			r.partitionTimes = append(r.partitionTimes, partitionTime)
		case now, ok := <-c:
			if !ok {
				return
			}
			if r.replicationCount > 0 {
				elapsed := float64(now.Sub(r.startTime)) / float64(time.Second)
				remaining := time.Duration(float64(now.Sub(r.startTime))/(float64(r.replicationCount)/float64(r.jobCount))) - now.Sub(r.startTime)
				var remainingStr string
				if remaining >= time.Hour {
					remainingStr = fmt.Sprintf("%.0fh", remaining.Hours())
				} else if remaining >= time.Minute {
					remainingStr = fmt.Sprintf("%.0fm", remaining.Minutes())
				} else {
					remainingStr = fmt.Sprintf("%.0fs", remaining.Seconds())
				}
				r.LogInfo("%d/%d (%.2f%%) partitions replicated in %.2fs (%.2f/sec, %v remaining)",
					r.replicationCount, r.jobCount, float64(100*r.replicationCount)/float64(r.jobCount),
					elapsed, float64(r.replicationCount)/elapsed, remainingStr)
			}
			if len(r.partitionTimes) > 0 {
				r.partitionTimes.Sort()
				r.LogInfo("Partition times: max %.4fs, min %.4fs, med %.4fs",
					r.partitionTimes[len(r.partitionTimes)-1], r.partitionTimes[0],
					r.partitionTimes[len(r.partitionTimes)/2])
			}
			if r.dataTransferred > 0 {
				elapsed := float64(now.Sub(r.startTime)) / float64(time.Second)
				r.LogInfo("Data synced: %d (%.2f kbps), files synced: %d",
					r.dataTransferred, ((float64(r.dataTransferred)/1024.0)*8.0)/elapsed, r.filesTransferred)
			}
		}
	}
}

// Run replication passes of the whole server until c is closed.
func (r *Replicator) run(c <-chan time.Time) {
	for _ = range c {
		r.partitionTimes = nil
		r.jobCount = 0
		r.replicationCount = 0
		r.dataTransferred = 0
		r.filesTransferred = 0
		r.startTime = time.Now()
		statsTicker := time.NewTicker(StatsReportInterval)
		go r.statsReporter(statsTicker.C)

		r.partRateTicker = time.NewTicker(r.timePerPart)
		r.concurrencySem = make(chan struct{}, r.concurrency)
		localDevices, err := r.Ring.LocalDevices(r.port)
		if err != nil {
			r.LogError("Error getting local devices: %v", err)
			continue
		}
		for _, dev := range localDevices {
			if len(r.devices) > 0 {
				found := false
				for _, d := range r.devices {
					if dev.Device == d {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			r.devGroup.Add(1)
			go r.replicateDevice(dev)
		}
		r.devGroup.Wait()
		r.partRateTicker.Stop()
		statsTicker.Stop()
		r.statsReporter(OneTimeChan())
		hummingbird.DumpReconCache(r.reconCachePath, "object",
			map[string]interface{}{
				"object_replication_time": float64(time.Since(r.startTime)) / float64(time.Second),
				"object_replication_last": float64(time.Now().UnixNano()) / float64(time.Second),
			})
	}
}

// processPriorityJobs runs any pending priority jobs given the device's id
func (r *Replicator) processPriorityJobs(id int) {
	for {
		select {
		case pri := <-r.getPriRepChan(id):
			r.jobCountIncrement <- 1
			func() {
				<-r.partRateTicker.C
				r.concurrencySem <- struct{}{}
				r.replicationCountIncrement <- 1
				j := &job{
					dev:       pri.FromDevice,
					partition: strconv.FormatUint(pri.Partition, 10),
					objPath:   filepath.Join(r.driveRoot, pri.FromDevice.Device, "objects"),
				}
				partStart := time.Now()
				defer func() {
					<-r.concurrencySem
					r.partitionTimesAdd <- float64(time.Since(partStart)) / float64(time.Second)
				}()
				if pri.JobType == "handoff" {
					r.replicateHandoff(j, pri.ToDevices)
				} else if pri.JobType == "local" {
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

// priorityRepHandler handles HTTP requests for priority replications jobs.
func (r *Replicator) priorityRepHandler(w http.ResponseWriter, req *http.Request) {
	var pri PriorityRepJob
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	if err := json.Unmarshal(data, &pri); err != nil || pri.JobType == "" {
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

func (r *Replicator) startWebServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/priorityrep", r.priorityRepHandler)
	mux.Handle("/debug/", http.DefaultServeMux)
	for {
		if sock, err := hummingbird.RetryListen(r.bindIp, r.bindPort); err != nil {
			r.LogError("Listen failed: %v", err)
		} else {
			http.Serve(sock, mux)
		}
	}
}

// Run a single replication pass.
func (r *Replicator) Run() {
	r.run(OneTimeChan())
}

// Run replication passes in a loop until forever.
func (r *Replicator) RunForever() {
	go r.startWebServer()
	r.run(time.Tick(RunForeverInterval))
}

func NewReplicator(conf string, flags *flag.FlagSet) (hummingbird.Daemon, error) {
	replicator := &Replicator{
		partitionTimesAdd:         make(chan float64),
		replicationCountIncrement: make(chan uint64),
		jobCountIncrement:         make(chan uint64),
		dataTransferAdd:           make(chan uint64),
		priRepChans:               make(map[int]chan PriorityRepJob),
	}
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, fmt.Errorf("Unable to get hash prefix and suffix")
	}
	serverconf, err := hummingbird.LoadIniFile(conf)
	if err != nil || !serverconf.HasSection("object-replicator") {
		return nil, fmt.Errorf("Unable to find replicator config: %s", conf)
	}
	replicator.reconCachePath = serverconf.GetDefault("object-auditor", "recon_cache_path", "/var/cache/swift")
	replicator.checkMounts = serverconf.GetBool("object-replicator", "mount_check", true)
	replicator.driveRoot = serverconf.GetDefault("object-replicator", "devices", "/srv/node")
	replicator.port = int(serverconf.GetInt("object-replicator", "bind_port", 6000))
	replicator.bindIp = serverconf.GetDefault("object-replicator", "bind_ip", "0.0.0.0")
	replicator.bindPort = int(serverconf.GetInt("object-replicator", "replicator_bind_port", int64(replicator.port+500)))
	replicator.logger = hummingbird.SetupLogger(serverconf.GetDefault("object-replicator", "log_facility", "LOG_LOCAL0"), "object-replicator", "")
	if serverconf.GetBool("object-replicator", "vm_test_mode", false) {
		replicator.timePerPart = time.Duration(serverconf.GetInt("object-replicator", "ms_per_part", 2000)) * time.Millisecond
	} else {
		replicator.timePerPart = time.Duration(serverconf.GetInt("object-replicator", "ms_per_part", 750)) * time.Millisecond
	}
	replicator.concurrency = int(serverconf.GetInt("object-replicator", "concurrency", 1))
	if replicator.Ring, err = hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix); err != nil {
		return nil, fmt.Errorf("Unable to load ring.")
	}
	devices_flag := flags.Lookup("devices")
	if devices_flag != nil {
		if devices := devices_flag.Value.(flag.Getter).Get().(string); len(devices) > 0 {
			replicator.devices = strings.Split(devices, ",")
		}
	}
	partitions_flag := flags.Lookup("partitions")
	if partitions_flag != nil {
		if partitions := partitions_flag.Value.(flag.Getter).Get().(string); len(partitions) > 0 {
			replicator.partitions = strings.Split(partitions, ",")
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
