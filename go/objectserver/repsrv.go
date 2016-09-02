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
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/justinas/alice"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/middleware"
)

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

// ProgressReportHandler handles HTTP requests for current replication progress
func (r *Replicator) ProgressReportHandler(w http.ResponseWriter, req *http.Request) {
	data, err := json.Marshal(r.getDeviceProgress())
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
		if mounted, err := hummingbird.IsMount(filepath.Join(r.deviceRoot, pri.FromDevice.Device)); err != nil || mounted == false {
			w.WriteHeader(507)
			return
		}
	}
	if !hummingbird.Exists(filepath.Join(r.deviceRoot, pri.FromDevice.Device, "objects", strconv.FormatUint(pri.Partition, 10))) {
		w.WriteHeader(404)
		return
	}
	if r.priorityReplicate(pri, time.Hour) {
		w.WriteHeader(200)
	} else {
		w.WriteHeader(500)
	}
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
	hashes, err := GetHashes(r.deviceRoot, vars["device"], vars["partition"], recalculate, r.reclaimAge, policy, hummingbird.GetLogger(request))
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

	rc := NewIncomingRepConn(rw, conn)
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
		hashes, err = GetHashes(r.deviceRoot, brr.Device, brr.Partition, nil, r.reclaimAge, policy, hummingbird.GetLogger(request))
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
			tempDir := TempDirPath(r.deviceRoot, vars["device"])
			fileName := filepath.Join(r.deviceRoot, sfr.Path)
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
