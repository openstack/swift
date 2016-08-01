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

package hummingbird

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
)

func DumpReconCache(reconCachePath string, source string, cacheData map[string]interface{}) error {
	reconFile := filepath.Join(reconCachePath, source+".recon")

	if lock, err := LockPath(filepath.Dir(reconFile), 5*time.Second); err != nil {
		return err
	} else {
		defer lock.Close()
	}

	filedata, _ := ioutil.ReadFile(reconFile)
	var reconData = make(map[string]interface{})
	if filedata != nil && len(filedata) > 0 {
		var data interface{}
		if json.Unmarshal(filedata, &data) == nil {
			if _, ok := data.(map[string]interface{}); ok {
				reconData = data.(map[string]interface{})
			}
		}
	}
	for key, item := range cacheData {
		switch item := item.(type) {
		case map[string]interface{}:
			if len(item) == 0 {
				delete(reconData, key)
				continue
			}
			if _, ok := reconData[key].(map[string]interface{}); !ok {
				reconData[key] = make(map[string]interface{})
			}
			for itemk, itemv := range item {
				if itemvmap, ok := itemv.(map[string]interface{}); ok && len(itemvmap) == 0 {
					delete(reconData[key].(map[string]interface{}), itemk)
				} else if itemv == nil {
					delete(reconData[key].(map[string]interface{}), itemk)
				} else {
					reconData[key].(map[string]interface{})[itemk] = itemv
				}
			}
		case nil:
			delete(reconData, key)
		default:
			reconData[key] = item
		}
	}
	newdata, _ := json.Marshal(reconData)
	return WriteFileAtomic(reconFile, newdata, 0600)
}

// getMem dumps the contents of /proc/meminfo if it's available, otherwise it pulls what it can from gopsutil/mem
func getMem() interface{} {
	if fp, err := os.Open("/proc/meminfo"); err == nil {
		defer fp.Close()
		results := make(map[string]string)
		scanner := bufio.NewScanner(fp)
		for scanner.Scan() {
			vals := strings.Split(scanner.Text(), ":")
			results[strings.TrimSpace(vals[0])] = strings.TrimSpace(vals[1])
		}
		return results
	} else {
		vmem, err := mem.VirtualMemory()
		if err != nil {
			return nil
		}
		swap, err := mem.SwapMemory()
		if err != nil {
			return nil
		}
		return map[string]string{
			"MemTotal":  strconv.FormatUint(vmem.Total, 10),
			"MemFree":   strconv.FormatUint(vmem.Available, 10),
			"Buffers":   strconv.FormatUint(vmem.Buffers, 10),
			"Cached":    strconv.FormatUint(vmem.Cached, 10),
			"SwapTotal": strconv.FormatUint(swap.Total, 10),
			"SwapFree":  strconv.FormatUint(swap.Free, 10),
		}
	}
}

func getSockstats() interface{} {
	results := make(map[string]int64)

	fp, err := os.Open("/proc/net/sockstat")
	if err != nil {
		return nil
	}
	defer fp.Close()
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "TCP: inuse") {
			parts := strings.Split(line, " ")
			results["tcp_in_use"], _ = strconv.ParseInt(parts[2], 10, 64)
			results["orphan"], _ = strconv.ParseInt(parts[4], 10, 64)
			results["time_wait"], _ = strconv.ParseInt(parts[6], 10, 64)
			results["tcp_mem_allocated_bytes"], _ = strconv.ParseInt(parts[10], 10, 64)
			results["tcp_mem_allocated_bytes"] *= int64(os.Getpagesize())
		}
	}

	fp, err = os.Open("/proc/net/sockstat6")
	if err != nil {
		return nil
	}
	defer fp.Close()
	scanner = bufio.NewScanner(fp)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "TCP6: inuse") {
			parts := strings.Split(line, " ")
			results["tcp6_in_use"], _ = strconv.ParseInt(parts[2], 10, 64)
		}
	}

	return results
}

func getLoad() interface{} {
	results := make(map[string]interface{})
	avg, err := load.Avg()
	if err != nil {
		return nil
	}
	misc, err := load.Misc()
	if err != nil {
		return nil
	}
	pids, err := process.Pids()
	if err != nil {
		return nil
	}
	results["1m"] = avg.Load1
	results["5m"] = avg.Load5
	results["15m"] = avg.Load15
	results["tasks"] = fmt.Sprintf("%d/%d", misc.ProcsRunning, len(pids))
	// swift's recon puts the pid of the last created process in this field, which seems kind of useless.
	// I'm making it the number of processes, which seems like what it was meant to be.
	results["processes"] = len(pids)
	// also adding these two fields, since they might be useful.
	results["running"] = misc.ProcsRunning
	results["blocked"] = misc.ProcsBlocked
	return results
}

func getMounts() interface{} {
	results := make([]map[string]string, 0)
	partitions, err := disk.Partitions(true)
	if err != nil {
		return nil
	}
	for _, part := range partitions {
		results = append(results, map[string]string{"device": part.Device, "path": part.Mountpoint})
	}
	return results
}

func fromReconCache(source string, keys ...string) (interface{}, error) {
	results := make(map[string]interface{})
	for _, key := range keys {
		results[key] = nil
	}
	filedata, err := ioutil.ReadFile(fmt.Sprintf("/var/cache/swift/%s.recon", source))
	if err != nil {
		results["recon_error"] = fmt.Sprintf("Error: %s", err)
		return results, nil
	}
	var data interface{}
	json.Unmarshal(filedata, &data)
	switch data := data.(type) {
	case map[string]interface{}:
		for _, key := range keys {
			results[key] = data[key]
		}
	default:
		return nil, errors.New(fmt.Sprintf("Unexpected data type %T in recon file.", data))
	}
	return results, nil
}

func getUnmounted(driveRoot string) (interface{}, error) {
	unmounted := make([]map[string]interface{}, 0)
	dirInfo, err := os.Stat(driveRoot)
	if err != nil {
		return nil, err
	}
	fileInfo, _ := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	for _, info := range fileInfo {
		if info.Sys().(*syscall.Stat_t).Dev == dirInfo.Sys().(*syscall.Stat_t).Dev {
			unmounted = append(unmounted, map[string]interface{}{"device": info.Name(), "mounted": false})
		}
	}
	return unmounted, nil
}

func fileMD5(files ...string) (map[string]string, error) {
	response := make(map[string]string)
	for _, file := range files {
		fp, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer fp.Close()
		hash := md5.New()
		io.Copy(hash, fp)
		response[file] = fmt.Sprintf("%x", hash.Sum(nil))
	}
	return response, nil
}

func ListDevices(driveRoot string) (map[string][]string, error) {
	fileInfo, err := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	fileList := make([]string, 0)
	for _, info := range fileInfo {
		fileList = append(fileList, info.Name())
	}
	return map[string][]string{driveRoot: fileList}, nil
}

func quarantineCounts(driveRoot string) (map[string]interface{}, error) {
	qcounts := map[string]interface{}{"objects": 0, "containers": 0, "accounts": 0}
	deviceList, err := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	for _, info := range deviceList {
		for key, _ := range qcounts {
			stat, err := os.Stat(filepath.Join(driveRoot, info.Name(), "quarantined", key))
			if err == nil {
				qcounts[key] = stat.Sys().(*syscall.Stat_t).Nlink
			}
		}
	}
	return qcounts, nil
}

func diskUsage(driveRoot string) ([]map[string]interface{}, error) {
	devices := make([]map[string]interface{}, 0)
	dirInfo, err := os.Stat(driveRoot)
	if err != nil {
		return nil, err
	}
	fileInfo, _ := ioutil.ReadDir(driveRoot)
	if err != nil {
		return nil, err
	}
	for _, info := range fileInfo {
		if info.Sys().(*syscall.Stat_t).Dev == dirInfo.Sys().(*syscall.Stat_t).Dev {
			devices = append(devices, map[string]interface{}{"device": info.Name(), "mounted": false,
				"size": "", "used": "", "avail": ""})
		} else {
			var fsinfo syscall.Statfs_t
			err := syscall.Statfs(filepath.Join(driveRoot, info.Name()), &fsinfo)
			if err == nil {
				capacity := int64(fsinfo.Bsize) * int64(fsinfo.Blocks)
				used := int64(fsinfo.Bsize) * (int64(fsinfo.Blocks) - int64(fsinfo.Bavail))
				available := int64(fsinfo.Bsize) * int64(fsinfo.Bavail)
				devices = append(devices, map[string]interface{}{"device": info.Name(), "mounted": true,
					"size": capacity, "used": used, "avail": available})
			}
		}
	}
	return devices, nil
}

func ReconHandler(driveRoot string, writer http.ResponseWriter, request *http.Request) {
	var content interface{} = nil

	vars := GetVars(request)

	switch vars["method"] {
	case "mem":
		content = getMem()
	case "load":
		content = getLoad()
	case "async":
		var err error
		content, err = fromReconCache("object", "async_pending")
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "replication":
		var err error
		if vars["recon_type"] == "account" {
			content, err = fromReconCache("account", "replication_time", "replication_stats", "replication_last")
		} else if vars["recon_type"] == "container" {
			content, err = fromReconCache("container", "replication_time", "replication_stats", "replication_last")
		} else if vars["recon_type"] == "object" {
			content, err = fromReconCache("object", "object_replication_time", "object_replication_last")
		} else if vars["recon_type"] == "" {
			// handle old style object replication requests
			content, err = fromReconCache("object", "object_replication_time", "object_replication_last")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "devices":
		var err error
		content, err = ListDevices(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "updater":
		var err error
		if vars["recon_type"] == "container" {
			content, err = fromReconCache("container", "container_updater_sweep")
		} else if vars["recon_type"] == "object" {
			content, err = fromReconCache("object", "object_updater_sweep")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "auditor":
		var err error
		if vars["recon_type"] == "account" {
			content, err = fromReconCache("account", "account_audits_passed", "account_auditor_pass_completed", "account_audits_since", "account_audits_failed")
		} else if vars["recon_type"] == "container" {
			content, err = fromReconCache("container", "container_audits_passed", "container_auditor_pass_completed", "container_audits_since", "container_audits_failed")
		} else if vars["recon_type"] == "object" {
			content, err = fromReconCache("object", "object_auditor_stats_ALL", "object_auditor_stats_ZBF")
		}
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "expirer":
		var err error
		content, err = fromReconCache("object", "object_expiration_pass", "expired_last_pass")
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "mounted":
		content = getMounts()
	case "unmounted":
		var err error
		content, err = getUnmounted(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "ringmd5":
		var err error
		content, err = fileMD5("/etc/hummingbird/object.ring.gz", "/etc/hummingbird/container.ring.gz", "/etc/hummingbird/account.ring.gz")
		if err != nil {
			content, err = fileMD5("/etc/swift/object.ring.gz", "/etc/swift/container.ring.gz", "/etc/swift/account.ring.gz")
			if err != nil {
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
	case "swiftconfmd5":
		var err error
		content, err = fileMD5("/etc/hummingbird/hummingbird.conf")
		if err != nil {
			content, err = fileMD5("/etc/swift/swift.conf")
			if err != nil {
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
	case "quarantined":
		var err error
		content, err = quarantineCounts(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	case "sockstat":
		content = getSockstats()
	case "version":
		content = map[string]string{"version": "idunno"}
	case "diskusage":
		var err error
		content, err = diskUsage(driveRoot)
		if err != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	default:
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	if content == nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(200)
	serialized, _ := json.MarshalIndent(content, "", "  ")
	writer.Write(serialized)
}
