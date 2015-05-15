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
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
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

// Object replicator daemon object
type Replicator struct {
	client         *http.Client
	concurrency    int
	checkMounts    bool
	driveRoot      string
	reconCachePath string
	logger         hummingbird.SysLogLike
	port           int
	Ring           hummingbird.Ring
	devGroup       sync.WaitGroup
	partGroup      sync.WaitGroup
	partRateTicker *time.Ticker
	timePerPart    time.Duration
	jobChan        chan *job

	/* stats accounting */
	startTime                                    time.Time
	replicationCount, jobCount                   uint64
	replicationCountIncrement, jobCountIncrement chan uint64
	partitionTimes                               sort.Float64Slice
	partitionTimesAdd                            chan float64
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

// Return a channel that will yield the current time once, then is closed.
func OneTimeChan() chan time.Time {
	c := make(chan time.Time, 1)
	c <- time.Now()
	close(c)
	return c
}

// Sync a single file to the remote device.
func (r *Replicator) syncFile(filePath string, relPath string, dev *hummingbird.Device, repid string) bool {
	if os.PathSeparator != '/' { // why am I pretending like someone might ever run this on windows?
		relPath = strings.Replace(relPath, string(os.PathSeparator), "/", -1)
	}
	fileUrl := fmt.Sprintf("http://%s:%d/%s/objects/%s", dev.ReplicationIp, dev.ReplicationPort, dev.Device, hummingbird.Urlencode(relPath))
	fp, err := os.Open(filePath)
	if err != nil {
		r.LogError("[syncFile] unable to open file (%v): %s", err, filePath)
		return false
	}
	defer fp.Close()
	finfo, err := fp.Stat()
	if err != nil || !finfo.Mode().IsRegular() {
		r.LogError("[syncFile] file is weird (%v): %s", err, filePath)
		return false
	}
	rawxattr, err := RawReadMetadata(fp.Fd())
	if err != nil || len(rawxattr) == 0 {
		r.LogError("[syncFile] error loading metadata (%v): %s", err, filePath)
		return false
	}

	// Perform a mini-audit, since it's cheap and we can potentially avoid spreading bad data around.
	v, err := hummingbird.PickleLoads(rawxattr)
	if err != nil {
		r.LogError("[syncFile] error parsing metadata (%v): %s", err, filePath)
		return false
	}
	metadata, ok := v.(map[interface{}]interface{})
	if !ok {
		r.LogError("[syncFile] error parsing metadata (not map): %s", filePath)
		return false
	}
	for key, value := range metadata {
		if _, ok := key.(string); !ok {
			r.LogError("[syncFile] metadata key not string (%v): %s", key, filePath)
			return false
		}
		if _, ok := value.(string); !ok {
			r.LogError("[syncFile] metadata value not string (%v): %s", value, filePath)
			return false
		}
	}
	switch filepath.Ext(filePath) {
	case ".data":
		for _, reqEntry := range []string{"Content-Length", "Content-Type", "name", "ETag", "X-Timestamp"} {
			if _, ok := metadata[reqEntry]; !ok {
				r.LogError("[syncFile] Required metadata entry %s not found in %s", reqEntry, filePath)
				return false
			}
		}
		if contentLength, err := strconv.ParseInt(metadata["Content-Length"].(string), 10, 64); err != nil || contentLength != finfo.Size() {
			r.LogError("[syncFile] Content-Length check failure: %s", filePath)
			return false
		}
	case ".ts":
		for _, reqEntry := range []string{"name", "X-Timestamp"} {
			if _, ok := metadata[reqEntry]; !ok {
				r.LogError("[syncFile] Required metadata entry %s not found in %s", reqEntry, filePath)
				return false
			}
		}
	}

	req, err := http.NewRequest("SYNC", fileUrl, fp)
	if err != nil {
		r.LogError("[syncFile] error creating new request: %v", err)
		return false
	}
	req.ContentLength = finfo.Size()
	req.Header.Add("X-Attrs", hex.EncodeToString(rawxattr))
	req.Header.Add("X-Replication-Id", repid)
	if finfo.Size() > 0 {
		req.Header.Add("Expect", "100-Continue")
	}
	resp, err := r.client.Do(req)
	if err != nil {
		r.LogError("[syncFile] error syncing file: %v", err)
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusConflict || (resp.StatusCode/100) == 2
}

// Request hashes from the remote side.  This also now begins a new "replication session".
func (r *Replicator) getRemoteHashes(dev *hummingbird.Device, partition string, repid string) (map[interface{}]interface{}, bool) {
	url := fmt.Sprintf("http://%s:%d/%s/%s", dev.ReplicationIp, dev.ReplicationPort, dev.Device, partition)
	req, err := http.NewRequest("REPLICATE", url, nil)
	if err != nil {
		r.LogError("[getRemoteHashes] error creating new request: %v", err)
		return nil, false
	}
	req.Header.Add("X-Replication-Id", repid)
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		r.LogError("[getRemoteHashes] error reading REPLICATE response: %v", err)
		return nil, false
	}
	if resp.StatusCode/100 != 2 {
		return nil, resp.StatusCode == 507
	}
	rhashes, err := hummingbird.PickleLoads(data)
	if err != nil {
		r.LogError("[getRemoteHashes] error parsing remote hashes pickle: %v", err)
		return nil, false
	}
	return rhashes.(map[interface{}]interface{}), false
}

// Notify the remote side that we're done with replication for this partition, so it knows it can accept new requests.
func (r *Replicator) endReplication(dev *hummingbird.Device, partition string, repid string) {
	url := fmt.Sprintf("http://%s:%d/%s/%s/end", dev.ReplicationIp, dev.ReplicationPort, dev.Device, partition)
	if req, err := http.NewRequest("REPLICATE", url, nil); err == nil {
		req.Header.Add("X-Replication-Id", repid)
		if resp, err := r.client.Do(req); err == nil {
			resp.Body.Close()
			return
		}
	}
}

// Replicate a partition that belongs on the local device.  Will replicate to handoff nodes if the remote side is unmounted.
func (r *Replicator) replicateLocal(j *job, nodes []*hummingbird.Device, moreNodes hummingbird.MoreNodes) {
	defer r.LogPanics("PANIC REPLICATING LOCAL PARTITION")
	path := filepath.Join(j.objPath, j.partition)
	repid := hummingbird.UUID()
	syncCount := 0
	remoteHashes := make(map[int]map[interface{}]interface{})
	for i := 0; i < len(nodes); i++ {
		if rhashes, remoteUnmounted := r.getRemoteHashes(nodes[i], j.partition, repid); rhashes != nil {
			remoteHashes[nodes[i].Id] = rhashes
			defer r.endReplication(nodes[i], j.partition, repid)
		} else if remoteUnmounted == true {
			if nextNode := moreNodes.Next(); nextNode != nil {
				nodes = append(nodes, nextNode)
			}
		}
	}

	if len(remoteHashes) == 0 {
		return
	}

	recalc := make([]string, 0)

	hashes, herr := GetHashes(r.driveRoot, j.dev.Device, j.partition, recalc, r)
	if herr != nil {
		r.LogError("[replicateLocal] error getting local hashes: %v", herr)
		return
	}

	// swift does this recalc operation, but I'm skeptical that it helps all that much.
	// I'm collecting some stats on how many syncs it saves.
	wrongCount := 0
	for suffix, localHash := range hashes {
		for _, remoteHash := range remoteHashes {
			if remoteHash[suffix] != nil && localHash != remoteHash[suffix] {
				recalc = append(recalc, suffix)
				wrongCount++
				break
			}
		}
	}
	hashes, herr = GetHashes(r.driveRoot, j.dev.Device, j.partition, recalc, r)
	if herr != nil {
		r.LogError("[replicateLocal] error recalculating local hashes: %v", herr)
		return
	}
	for _, suffix := range recalc { // temporary, just for stats
		for _, remoteHash := range remoteHashes {
			if remoteHash[suffix] != nil && hashes[suffix] != remoteHash[suffix] {
				wrongCount--
			}
		}
	}

	for suffix, localHash := range hashes {
		needSync := false
		for _, remoteHash := range remoteHashes {
			if localHash != remoteHash[suffix] {
				needSync = true
				break
			}
		}
		if !needSync {
			continue
		}
		hashDirs, err := filepath.Glob(filepath.Join(path, suffix, "????????????????????????????????"))
		if err != nil {
			continue
		}
		for _, hashDir := range hashDirs {
			fileList, err := filepath.Glob(filepath.Join(hashDir, "*.[tdm]*"))
			if err != nil {
				continue
			}
			for _, objFile := range fileList {
				relPath, _ := filepath.Rel(filepath.Dir(path), objFile)
				for _, dev := range nodes {
					if rhashes, ok := remoteHashes[dev.Id]; ok && localHash != rhashes[suffix] {
						r.syncFile(objFile, relPath, dev, repid)
						syncCount++
					}
				}
			}
		}
	}
	if syncCount > 0 || len(recalc) > 0 {
		r.LogInfo("[replicateLocal] Partition %s synced %d files, recalculated %d, saved %d",
			path, syncCount, len(recalc), wrongCount)
	}
}

// Replicate a partition that doesn't belong on the local device.
// Doesn't replicate to handoff nodes and removes objects as they're replicated successfully.
func (r *Replicator) replicateHandoff(j *job, nodes []*hummingbird.Device) {
	defer r.LogPanics("PANIC REPLICATING HANDOFF PARTITION")
	path := filepath.Join(j.objPath, j.partition)
	repid := hummingbird.UUID()
	syncCount := 0
	remoteDriveAvailable := make(map[int]bool)
	for _, dev := range nodes {
		if rhashes, _ := r.getRemoteHashes(dev, j.partition, repid); rhashes != nil {
			remoteDriveAvailable[dev.Id] = true
			defer r.endReplication(dev, j.partition, repid)
		}
	}
	if len(remoteDriveAvailable) == 0 {
		return
	}
	suffixDirs, err := filepath.Glob(filepath.Join(path, "[a-f0-9][a-f0-9][a-f0-9]"))
	if err != nil {
		r.LogError("[replicateHandoff] error globbing partition: %v", err)
		return
	}
	if len(suffixDirs) == 0 {
		os.RemoveAll(path)
	}
	for _, suffDir := range suffixDirs {
		hashDirs, err := filepath.Glob(filepath.Join(suffDir, "????????????????????????????????"))
		if err != nil {
			r.LogError("[replicateHandoff] error globbing suffix: %v", err)
			continue
		}
		if len(hashDirs) == 0 {
			os.RemoveAll(suffDir)
		}
		for _, hashDir := range hashDirs {
			fileList, err := filepath.Glob(filepath.Join(hashDir, "*.[tdm]*"))
			if err != nil {
				continue
			}
			fullyReplicated := true
			for _, objFile := range fileList {
				for _, dev := range nodes {
					relPath, _ := filepath.Rel(filepath.Dir(path), objFile)
					if !remoteDriveAvailable[dev.Id] {
						fullyReplicated = false
					} else if !r.syncFile(objFile, relPath, dev, repid) {
						fullyReplicated = false
					} else {
						syncCount++
					}
				}
			}
			if fullyReplicated {
				os.RemoveAll(hashDir)
			}
		}
	}
	if syncCount > 0 {
		r.LogInfo("[replicateHandoff] Partition %s synced %d files", path, syncCount)
	}
}

// Worker for partition replication - launched in a goroutine by run()
func (r *Replicator) partitionReplicator() {
	defer r.partGroup.Done()

	for j := range r.jobChan {
		<-r.partRateTicker.C
		partStart := time.Now()
		r.replicationCountIncrement <- 1
		partitioni, err := strconv.ParseUint(j.partition, 10, 64)
		if err != nil {
			continue
		}
		if nodes, handoff := r.Ring.GetJobNodes(partitioni, j.dev.Id); handoff {
			r.replicateHandoff(j, nodes)
		} else {
			moreNodes := r.Ring.GetMoreNodes(partitioni)
			r.replicateLocal(j, nodes, moreNodes)
		}
		r.partitionTimesAdd <- float64(time.Since(partStart)) / float64(time.Second)
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

// Shovel all partitions from the device onto the job channel, where a worker should take care of them.
func (r *Replicator) replicateDevice(dev *hummingbird.Device) {
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
		r.jobChan <- &job{objPath: objPath, partition: filepath.Base(partition), dev: dev}
	}
}

// Collect and log replication stats - runs in a goroutine launched by run(), runs for the duration of a replication pass.
func (r *Replicator) statsReporter(c <-chan time.Time) {
	for {
		select {
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
		}
	}
}

// Run replication passes of the whole server until c is closed.
func (r *Replicator) run(c <-chan time.Time) {
	for r.startTime = range c {
		r.partitionTimes = nil
		r.jobCount = 0
		r.replicationCount = 0
		statsTicker := time.NewTicker(StatsReportInterval)
		go r.statsReporter(statsTicker.C)

		r.jobChan = make(chan *job)
		r.partRateTicker = time.NewTicker(r.timePerPart)
		for i := 0; i < r.concurrency; i++ {
			r.partGroup.Add(1)
			go r.partitionReplicator()
		}
		localDevices, err := r.Ring.LocalDevices(r.port)
		if err != nil {
			r.LogError("Error getting local devices: %v", err)
			continue
		}
		for _, dev := range localDevices {
			r.devGroup.Add(1)
			go r.replicateDevice(dev)
		}
		r.devGroup.Wait()
		close(r.jobChan)
		r.partGroup.Wait()
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

// Run a single replication pass.
func (r *Replicator) Run() {
	r.run(OneTimeChan())
}

// Run replication passes in a loop until forever.
func (r *Replicator) RunForever() {
	r.run(time.Tick(RunForeverInterval))
}

func NewReplicator(conf string) (hummingbird.Daemon, error) {
	replicator := &Replicator{
		partitionTimesAdd: make(chan float64), replicationCountIncrement: make(chan uint64), jobCountIncrement: make(chan uint64),
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
	replicator.logger = hummingbird.SetupLogger(serverconf.GetDefault("object-replicator", "log_facility", "LOG_LOCAL0"), "object-replicator")
	if serverconf.GetBool("object-replicator", "vm_test_mode", false) {
		replicator.timePerPart = time.Duration(serverconf.GetInt("object-replicator", "ms_per_part", 2000)) * time.Millisecond
	} else {
		replicator.timePerPart = time.Duration(serverconf.GetInt("object-replicator", "ms_per_part", 25)) * time.Millisecond
	}
	replicator.concurrency = int(serverconf.GetInt("object-replicator", "concurrency", 1))
	if replicator.Ring, err = hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix); err != nil {
		return nil, fmt.Errorf("Unable to load ring.")
	}
	replicator.client = &http.Client{
		Timeout: time.Second * 300,
		Transport: &http.Transport{
			Dial:               (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).Dial,
			DisableCompression: true,
		},
	}
	return replicator, nil
}

// Used by the object server to limit replication concurrency
type ReplicationManager struct {
	inUse        map[string]map[string]time.Time
	lock         sync.Mutex
	limitPerDisk int64
	limitOverall int64
}

// Give or reject permission for a new replication session on the given device, identified by repid.
func (r *ReplicationManager) BeginReplication(device string, repid string) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.inUse[device] == nil {
		r.inUse[device] = make(map[string]time.Time)
	}
	inProgress := int64(0)
	for _, perDevice := range r.inUse {
		for rid, lastUpdate := range perDevice {
			if time.Since(lastUpdate) > ReplicationSessionTimeout {
				delete(perDevice, rid)
			} else {
				inProgress += 1
			}
		}
	}
	if inProgress >= r.limitOverall || len(r.inUse[device]) >= int(r.limitPerDisk) {
		return false
	} else {
		r.inUse[device][repid] = time.Now()
		return true
	}
}

// Note that the session is still in use, to keep it from timing out.
func (r *ReplicationManager) UpdateSession(device string, repid string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.inUse[device] != nil {
		r.inUse[device][repid] = time.Now()
	}
}

// Mark the session completed, remove it from any accounting.
func (r *ReplicationManager) Done(device string, repid string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.inUse[device], repid)
}

func NewReplicationManager(limitPerDisk int64, limitOverall int64) *ReplicationManager {
	return &ReplicationManager{limitPerDisk: limitPerDisk, limitOverall: limitOverall, inUse: make(map[string]map[string]time.Time)}
}
