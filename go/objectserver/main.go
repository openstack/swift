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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/syslog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/justinas/alice"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/middleware"
)

type ObjectServer struct {
	driveRoot        string
	hashPathPrefix   string
	hashPathSuffix   string
	checkEtags       bool
	checkMounts      bool
	allowedHeaders   map[string]bool
	logger           *syslog.Writer
	logLevel         string
	fallocateReserve int64
	diskInUse        *hummingbird.KeyedLimit
	replicationMan   *ReplicationManager
	expiringDivisor  int64
	updateClient     *http.Client
	replicateTimeout time.Duration
}

func (server *ObjectServer) ObjGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	headers := writer.Header()
	hashDir := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix)
	dataFile, metaFile := ObjectFiles(hashDir)
	if dataFile == "" || strings.HasSuffix(dataFile, ".ts") {
		if im := request.Header.Get("If-Match"); im != "" && strings.Contains(im, "*") {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		} else {
			hummingbird.StandardResponse(writer, http.StatusNotFound)
			return
		}
	}

	file, err := os.Open(dataFile)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer file.Close()

	metadata, err := OpenObjectMetadata(file.Fd(), metaFile)
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
		if !os.IsNotExist(err) && QuarantineHash(hashDir) == nil {
			InvalidateHash(hashDir)
		}
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	contentLength, err := strconv.ParseInt(metadata["Content-Length"], 10, 64)
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error getting the content length from content-length: %s", err.Error())
		http.Error(writer, "Invalid Content-Length header", http.StatusBadRequest)
		return
	}

	if stat, err := file.Stat(); err != nil || stat.Size() != contentLength {
		if QuarantineHash(hashDir) == nil {
			InvalidateHash(hashDir)
		}
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
	if deleteAt, ok := metadata["X-Delete-At"]; ok {
		if deleteTime, err := hummingbird.ParseDate(deleteAt); err == nil && deleteTime.Before(time.Now()) {
			hummingbird.StandardResponse(writer, http.StatusNotFound)
			return
		}
	}

	lastModified, err := hummingbird.ParseDate(metadata["X-Timestamp"])
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error getting timestamp from %s: %s", dataFile, err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	lastModifiedHeader := lastModified
	if lastModified.Nanosecond() > 0 { // for some reason, Last-Modified is ceil(X-Timestamp)
		lastModifiedHeader = lastModified.Truncate(time.Second).Add(time.Second)
	}
	headers.Set("Last-Modified", lastModifiedHeader.Format(time.RFC1123))
	headers.Set("ETag", "\""+metadata["ETag"]+"\"")
	xTimestamp, err := hummingbird.GetEpochFromTimestamp(metadata["X-Timestamp"])
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error getting the epoch time from x-timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	headers.Set("X-Timestamp", xTimestamp)
	for key, value := range metadata {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) ||
			strings.HasPrefix(key, "X-Object-Meta-") ||
			strings.HasPrefix(key, "X-Object-Sysmeta-") {
			headers.Set(key, value)
		}
	}

	if im := request.Header.Get("If-Match"); im != "" && !strings.Contains(im, metadata["ETag"]) && !strings.Contains(im, "*") {
		hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
		return
	}

	if inm := request.Header.Get("If-None-Match"); inm != "" && (strings.Contains(inm, metadata["ETag"]) || strings.Contains(inm, "*")) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	if ius, err := hummingbird.ParseDate(request.Header.Get("If-Unmodified-Since")); err == nil && lastModified.After(ius) {
		hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
		return
	}

	if ims, err := hummingbird.ParseDate(request.Header.Get("If-Modified-Since")); err == nil && lastModified.Before(ims) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	headers.Set("Accept-Ranges", "bytes")
	headers.Set("Content-Type", metadata["Content-Type"])
	headers.Set("Content-Length", metadata["Content-Length"])

	if rangeHeader := request.Header.Get("Range"); rangeHeader != "" {
		ranges, err := hummingbird.ParseRange(rangeHeader, contentLength)
		if err != nil {
			headers.Set("Content-Length", "0")
			writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		} else if ranges != nil && len(ranges) == 1 {
			headers.Set("Content-Length", strconv.FormatInt(int64(ranges[0].End-ranges[0].Start), 10))
			headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", ranges[0].Start, ranges[0].End-1, contentLength))
			writer.WriteHeader(http.StatusPartialContent)
			file.Seek(ranges[0].Start, os.SEEK_SET)
			hummingbird.CopyN(file, ranges[0].End-ranges[0].Start, writer)
			return
		} else if ranges != nil && len(ranges) > 1 {
			w := hummingbird.NewMultiWriter(writer)
			responseLength := int64(4 + len(w.Boundary()) + (len(w.Boundary())+len(metadata["Content-Type"])+47)*len(ranges))
			for _, rng := range ranges {
				responseLength += int64(len(fmt.Sprintf("%d-%d/%d", rng.Start, rng.End-1, contentLength))) + rng.End - rng.Start
			}
			headers.Set("Content-Length", strconv.FormatInt(responseLength, 10))
			headers.Set("Content-Type", "multipart/byteranges;boundary="+w.Boundary())
			writer.WriteHeader(http.StatusPartialContent)
			for _, rng := range ranges {
				part, _ := w.CreatePart(textproto.MIMEHeader{"Content-Type": []string{metadata["Content-Type"]},
					"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.End-1, contentLength)}})
				file.Seek(rng.Start, os.SEEK_SET)
				hummingbird.CopyN(file, rng.End-rng.Start, part)
			}
			w.Close()
			return
		}
	}
	writer.WriteHeader(http.StatusOK)
	if request.Method == "GET" {
		if server.checkEtags {
			hash := md5.New()
			hummingbird.Copy(file, writer, hash)
			if hex.EncodeToString(hash.Sum(nil)) != metadata["ETag"] && QuarantineHash(hashDir) == nil {
				InvalidateHash(hashDir)
			}
		} else {
			io.Copy(writer, file)
		}
	} else {
		writer.Write([]byte{})
	}
}

func (server *ObjectServer) ObjPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	outHeaders := writer.Header()
	if !hummingbird.ValidTimestamp(request.Header.Get("X-Timestamp")) {
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	if vars["obj"] == "" {
		http.Error(writer, fmt.Sprintf("Invalid path: %s", request.URL.Path), http.StatusBadRequest)
		return
	}
	if request.Header.Get("Content-Type") == "" {
		http.Error(writer, "No content type", http.StatusBadRequest)
		return
	}
	hashDir := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix)

	if deleteAt := request.Header.Get("X-Delete-At"); deleteAt != "" {
		if deleteTime, err := hummingbird.ParseDate(deleteAt); err != nil || deleteTime.Before(time.Now()) {
			http.Error(writer, "X-Delete-At in past", 400)
			return
		}
	}

	requestTimestamp, err := hummingbird.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error standardizing request X-Timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}

	dataFile, metaFile := ObjectFiles(hashDir)
	if dataFile != "" && !strings.HasSuffix(dataFile, ".ts") {
		if inm := request.Header.Get("If-None-Match"); inm == "*" {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
		if metadata, err := ObjectMetadata(dataFile, metaFile); err == nil {
			if requestTime, err := hummingbird.ParseDate(requestTimestamp); err == nil {
				if lastModified, err := hummingbird.ParseDate(metadata["X-Timestamp"]); err == nil && !requestTime.After(lastModified) {
					outHeaders.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
					hummingbird.StandardResponse(writer, http.StatusConflict)
					return
				}
			}
			if inm := request.Header.Get("If-None-Match"); inm != "*" && strings.Contains(inm, metadata["ETag"]) {
				hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
				return
			}
		}
	}

	fileName := filepath.Join(hashDir, fmt.Sprintf("%s.data", requestTimestamp))
	tempDir := TempDirPath(server.driveRoot, vars["device"])
	tempFile, err := NewAtomicFileWriter(tempDir, fileName)
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error creating temporary file in %s: %s", server.driveRoot, err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer tempFile.Abandon()
	if err := tempFile.Preallocate(request.ContentLength, server.fallocateReserve); err != nil {
		hummingbird.GetLogger(request).LogDebug("Unable to allocate space: %v", err)
		hummingbird.CustomErrorResponse(writer, 507, vars)
	}
	hash := md5.New()
	totalSize, err := hummingbird.Copy(request.Body, tempFile, hash)
	if err == io.ErrUnexpectedEOF {
		hummingbird.StandardResponse(writer, 499)
		return
	} else if err != nil {
		hummingbird.GetLogger(request).LogError("Error writing to file %s: %s", fileName, err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	metadata := map[string]string{
		"name":           "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"],
		"X-Timestamp":    requestTimestamp,
		"Content-Type":   request.Header.Get("Content-Type"),
		"Content-Length": strconv.FormatInt(totalSize, 10),
		"ETag":           hex.EncodeToString(hash.Sum(nil)),
	}
	for key := range request.Header {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) ||
			strings.HasPrefix(key, "X-Object-Meta-") ||
			strings.HasPrefix(key, "X-Object-Sysmeta-") {
			metadata[key] = request.Header.Get(key)
		}
	}
	requestEtag := strings.ToLower(request.Header.Get("ETag"))
	if requestEtag != "" && requestEtag != metadata["ETag"] {
		http.Error(writer, "Unprocessable Entity", 422)
		return
	}
	outHeaders.Set("ETag", metadata["ETag"])
	WriteMetadata(tempFile.Fd(), metadata)
	if tempFile.Save() != nil {
		hummingbird.GetLogger(request).LogError("Error saving object file: %s", fileName)
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	go func() {
		HashCleanupListDir(hashDir, hummingbird.GetLogger(request))
		if fd, err := syscall.Open(hashDir, syscall.O_DIRECTORY|os.O_RDONLY, 0666); err == nil {
			syscall.Fsync(fd)
			syscall.Close(fd)
		}
		InvalidateHash(hashDir)
	}()
	server.containerUpdates(request, metadata, request.Header.Get("X-Delete-At"), vars)
	hummingbird.StandardResponse(writer, http.StatusCreated)
}

func (server *ObjectServer) ObjDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	headers := writer.Header()
	requestTimestamp, err := hummingbird.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error standardizing request X-Timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	hashDir := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix)
	responseStatus := http.StatusNotFound

	dataFile, metaFile := ObjectFiles(hashDir)
	if ida := request.Header.Get("X-If-Delete-At"); ida != "" {
		_, err = strconv.ParseInt(ida, 10, 64)
		if err != nil {
			hummingbird.StandardResponse(writer, http.StatusBadRequest)
			return
		}
		if dataFile == "" {
			hummingbird.StandardResponse(writer, http.StatusNotFound)
			return
		}
		if !strings.HasSuffix(dataFile, ".data") {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
		metadata, err := ObjectMetadata(dataFile, metaFile)
		if err != nil {
			hummingbird.GetLogger(request).LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
			hummingbird.StandardResponse(writer, http.StatusInternalServerError)
			return
		}
		if _, ok := metadata["X-Delete-At"]; ok {
			if ida != metadata["X-Delete-At"] {
				hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
				return
			}
		} else {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
	}

	deleteAt := ""
	if dataFile != "" {
		if strings.HasSuffix(dataFile, ".data") {
			responseStatus = http.StatusNoContent
		}

		// TODO(redbo): I don't like that this function can call ObjectMetadata() twice on the same files.
		origMetadata, err := ObjectMetadata(dataFile, metaFile)
		if err == nil {
			if xda, ok := origMetadata["X-Delete-At"]; ok {
				deleteAt = xda
			}
			// compare the timestamps here
			if origTimestamp, ok := origMetadata["X-Timestamp"]; ok && origTimestamp >= requestTimestamp {
				headers.Set("X-Backend-Timestamp", origTimestamp)
				if strings.HasSuffix(dataFile, ".data") {
					hummingbird.StandardResponse(writer, http.StatusConflict)
					return
				} else {
					hummingbird.StandardResponse(writer, http.StatusNotFound)
					return
				}
			}
		} else if os.IsNotExist(err) {
			hummingbird.GetLogger(request).LogError("Listed data file now missing: %s", dataFile)
			responseStatus = http.StatusNotFound
		} else {
			hummingbird.GetLogger(request).LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
			if qerr := QuarantineHash(hashDir); qerr == nil {
				InvalidateHash(hashDir)
			}
			responseStatus = http.StatusNotFound
		}
	}

	fileName := filepath.Join(hashDir, fmt.Sprintf("%s.ts", requestTimestamp))
	tempDir := TempDirPath(server.driveRoot, vars["device"])
	tempFile, err := NewAtomicFileWriter(tempDir, fileName)
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error creating temporary file in %s: %s", server.driveRoot, err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer tempFile.Abandon()
	if err := tempFile.Preallocate(0, server.fallocateReserve); err != nil {
		hummingbird.GetLogger(request).LogDebug("Unable to allocate space: %v", err)
		hummingbird.CustomErrorResponse(writer, 507, vars)
	}
	metadata := map[string]string{
		"X-Timestamp": requestTimestamp,
		"name":        "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"],
	}
	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
	WriteMetadata(tempFile.Fd(), metadata)
	if tempFile.Save() != nil {
		hummingbird.GetLogger(request).LogError("Error saving tombstone file: %s -> %s", fileName)
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	go func() {
		HashCleanupListDir(hashDir, hummingbird.GetLogger(request))
		if fd, err := syscall.Open(hashDir, syscall.O_DIRECTORY|os.O_RDONLY, 0666); err == nil {
			syscall.Fsync(fd)
			syscall.Close(fd)
		}
		InvalidateHash(hashDir)
	}()
	server.containerUpdates(request, metadata, deleteAt, vars)
	hummingbird.StandardResponse(writer, responseStatus)
}

func (server *ObjectServer) ObjReplicateHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)

	var recalculate []string
	if len(vars["suffixes"]) > 0 {
		recalculate = strings.Split(vars["suffixes"], "-")
	}
	hashes, err := GetHashes(server.driveRoot, vars["device"], vars["partition"], recalculate, hummingbird.GetLogger(request))
	if err != nil {
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(hummingbird.PickleDumps(hashes))
}

var replicationDone = fmt.Errorf("Replication done")

func (server *ObjectServer) ObjRepConnHandler(writer http.ResponseWriter, request *http.Request) {
	var conn net.Conn
	var rw *bufio.ReadWriter
	var err error
	var brr BeginReplicationRequest

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
	if !server.replicationMan.Begin(brr.Device, server.replicateTimeout) {
		hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Timed out waiting for concurrency slot")
		writer.WriteHeader(503)
		return
	}
	defer server.replicationMan.Done(brr.Device)
	var hashes map[string]string
	if brr.NeedHashes {
		var herr *hummingbird.BackendError
		hashes, herr = GetHashes(server.driveRoot, brr.Device, brr.Partition, nil, hummingbird.GetLogger(request))
		if herr != nil {
			hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Error getting hashes: %v", herr)
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
		err := func() error { // this is a closure so we can use defers inside
			var sfr SyncFileRequest
			if err := rc.RecvMessage(&sfr); err != nil {
				return err
			}
			if sfr.Done {
				return replicationDone
			}
			fileName := filepath.Join(server.driveRoot, sfr.Path)
			hashDir := filepath.Dir(fileName)

			if ext := filepath.Ext(fileName); (ext != ".data" && ext != ".ts" && ext != ".meta") || len(filepath.Base(filepath.Dir(fileName))) != 32 {
				return rc.SendMessage(SyncFileResponse{Msg: "bad file path"})
			}
			if hummingbird.Exists(fileName) {
				return rc.SendMessage(SyncFileResponse{Exists: true, Msg: "exists"})
			}
			dataFile, metaFile := ObjectFiles(hashDir)
			if filepath.Base(fileName) < filepath.Base(dataFile) || filepath.Base(fileName) < filepath.Base(metaFile) {
				return rc.SendMessage(SyncFileResponse{NewerExists: true, Msg: "newer exists"})
			}
			tempDir := TempDirPath(server.driveRoot, brr.Device)
			tempFile, err := NewAtomicFileWriter(tempDir, fileName)
			if err != nil {
				return err
			}
			defer tempFile.Abandon()
			if err := tempFile.Preallocate(request.ContentLength, server.fallocateReserve); err != nil {
				hummingbird.GetLogger(request).LogDebug("Unable to allocate space: %v", err)
				return fmt.Errorf("Not enough free space.")
			}
			if xattrs, err := hex.DecodeString(sfr.Xattrs); err != nil || len(xattrs) == 0 {
				return rc.SendMessage(SyncFileResponse{Msg: "bad xattrs"})
			} else if err := RawWriteMetadata(tempFile.Fd(), xattrs); err != nil {
				return err
			}
			if err := rc.SendMessage(SyncFileResponse{GoAhead: true, Msg: "go ahead"}); err != nil {
				return err
			}
			if _, err := hummingbird.CopyN(rc, sfr.Size, tempFile); err != nil {
				return err
			}
			if tempFile.Save() != nil {
				return rc.SendMessage(FileUploadResponse{Msg: "upload failed"})
			}
			if dataFile != "" || metaFile != "" {
				HashCleanupListDir(hashDir, hummingbird.GetLogger(request))
			}
			InvalidateHash(hashDir)
			err = rc.SendMessage(FileUploadResponse{Success: true, Msg: "YAY"})
			return err
		}()
		if err == replicationDone {
			return
		} else if err != nil {
			hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Error replicating: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

func (server *ObjectServer) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
	return
}

func (server *ObjectServer) ReconHandler(writer http.ResponseWriter, request *http.Request) {
	hummingbird.ReconHandler(server.driveRoot, writer, request)
	return
}

func (server *ObjectServer) DiskUsageHandler(writer http.ResponseWriter, request *http.Request) {
	data, err := server.diskInUse.MarshalJSON()
	if err == nil {
		writer.WriteHeader(http.StatusOK)
		writer.Write(data)
	} else {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
	}
	return
}
func (server *ObjectServer) LogRequest(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		requestLogger := &hummingbird.RequestLogger{Request: request, Logger: server.logger}
		newWriter := &hummingbird.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
		defer requestLogger.LogPanics(newWriter)
		start := time.Now()
		hummingbird.SetLogger(request, requestLogger)
		next.ServeHTTP(newWriter, request)
		forceAcquire := request.Header.Get("X-Force-Acquire") == "true"
		if (request.Method != "REPLICATE" && request.Method != "REPCONN") || server.logLevel == "DEBUG" {
			extraInfo := "-"
			if forceAcquire {
				extraInfo = "FA"
			}
			server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
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
				extraInfo))
		}
	}
	return http.HandlerFunc(fn)
}

func (server *ObjectServer) AcquireDevice(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		parts := strings.Split(request.URL.Path, "/")
		if len(parts) > 1 {
			device := parts[1]
			devicePath := filepath.Join(server.driveRoot, device)
			if server.checkMounts {
				if mounted, err := hummingbird.IsMount(devicePath); err != nil || mounted != true {
					vars := map[string]string{"Method": request.Method, "device": device}
					hummingbird.CustomErrorResponse(writer, 507, vars)
					return
				}
			}

			forceAcquire := request.Header.Get("X-Force-Acquire") == "true"
			if concRequests := server.diskInUse.Acquire(device, forceAcquire); concRequests != 0 {
				writer.Header().Set("X-Disk-Usage", strconv.FormatInt(concRequests, 10))
				hummingbird.StandardResponse(writer, 503)
				return
			}
			defer server.diskInUse.Release(device)
		}
		next.ServeHTTP(writer, request)
	}
	return http.HandlerFunc(fn)
}

func (server *ObjectServer) updateDeviceLocks(seconds int64) {
	reloadTime := time.Duration(seconds) * time.Second
	for {
		time.Sleep(reloadTime)
		for _, key := range server.diskInUse.Keys() {
			lockPath := filepath.Join(server.driveRoot, key, "lock_device")
			if hummingbird.Exists(lockPath) {
				server.diskInUse.Lock(key)
			} else {
				server.diskInUse.Unlock(key)
			}
		}
	}
}

func (server *ObjectServer) GetHandler() http.Handler {
	commonHandlers := alice.New(middleware.ClearHandler, server.LogRequest, middleware.ValidateRequest)
	devicePrefixedHandlers := commonHandlers.Append(server.AcquireDevice)
	router := hummingbird.NewRouter()
	router.Get("/healthcheck", commonHandlers.ThenFunc(server.HealthcheckHandler))
	router.Get("/diskusage", commonHandlers.ThenFunc(server.DiskUsageHandler))
	router.Get("/recon/:method/:recon_type", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/recon/:method", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/:device/:partition/:account/:container/*obj", devicePrefixedHandlers.ThenFunc(server.ObjGetHandler))
	router.Head("/:device/:partition/:account/:container/*obj", devicePrefixedHandlers.ThenFunc(server.ObjGetHandler))
	router.Put("/:device/:partition/:account/:container/*obj", devicePrefixedHandlers.ThenFunc(server.ObjPutHandler))
	router.Delete("/:device/:partition/:account/:container/*obj", devicePrefixedHandlers.ThenFunc(server.ObjDeleteHandler))
	router.Replicate("/:device/:partition/:suffixes", devicePrefixedHandlers.ThenFunc(server.ObjReplicateHandler))
	router.Replicate("/:device/:partition", devicePrefixedHandlers.ThenFunc(server.ObjReplicateHandler))
	router.Handle("REPCONN", "/:device/:partition", devicePrefixedHandlers.ThenFunc(server.ObjRepConnHandler))
	router.Get("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/debug/pprof/:parm", http.DefaultServeMux)
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf("Invalid path: %s", r.URL.Path), http.StatusBadRequest)
	})
	return alice.New(middleware.GrepObject).Then(router)
}

func GetServer(conf string, flags *flag.FlagSet) (bindIP string, bindPort int, serv hummingbird.Server, logger hummingbird.SysLogLike, err error) {
	server := &ObjectServer{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "",
		allowedHeaders: map[string]bool{"Content-Disposition": true,
			"Content-Encoding":      true,
			"X-Delete-At":           true,
			"X-Object-Manifest":     true,
			"X-Static-Large-Object": true,
		},
	}
	server.hashPathPrefix, server.hashPathSuffix, err = hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return "", 0, nil, nil, err
	}

	serverconf, err := hummingbird.LoadIniFile(conf)
	if err != nil {
		return "", 0, nil, nil, errors.New(fmt.Sprintf("Unable to load %s", conf))
	}
	server.replicateTimeout = time.Minute // TODO(redbo): does this need to be configurable?
	server.driveRoot = serverconf.GetDefault("app:object-server", "devices", "/srv/node")
	server.checkMounts = serverconf.GetBool("app:object-server", "mount_check", true)
	server.checkEtags = serverconf.GetBool("app:object-server", "check_etags", false)
	server.fallocateReserve = serverconf.GetInt("app:object-server", "fallocate_reserve", 0)
	server.logLevel = serverconf.GetDefault("app:object-server", "log_level", "INFO")
	server.diskInUse = hummingbird.NewKeyedLimit(serverconf.GetLimit("app:object-server", "disk_limit", 25, 10000))
	server.expiringDivisor = serverconf.GetInt("app:object-server", "expiring_objects_container_divisor", 86400)
	bindIP = serverconf.GetDefault("app:object-server", "bind_ip", "0.0.0.0")
	bindPort = int(serverconf.GetInt("app:object-server", "bind_port", 6000))
	if allowedHeaders, ok := serverconf.Get("app:object-server", "allowed_headers"); ok {
		headers := strings.Split(allowedHeaders, ",")
		for i := range headers {
			server.allowedHeaders[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(headers[i]))] = true
		}
	}
	server.logger = hummingbird.SetupLogger(serverconf.GetDefault("app:object-server", "log_facility", "LOG_LOCAL1"), "object-server", "")
	server.replicationMan = NewReplicationManager(serverconf.GetLimit("app:object-server", "replication_limit", 3, 100))
	server.updateClient = &http.Client{Timeout: time.Second * 15}

	deviceLockUpdateSeconds := serverconf.GetInt("app:object-server", "device_lock_update_seconds", 0)
	if deviceLockUpdateSeconds > 0 {
		go server.updateDeviceLocks(deviceLockUpdateSeconds)
	}

	statsdHost := serverconf.GetDefault("app:object-server", "log_statsd_host", "")
	if statsdHost != "" {
		statsdPort := serverconf.GetInt("app:object-server", "log_statsd_port", 8125)
		// Go metrics collection pause interval in seconds
		statsdPause := serverconf.GetInt("app:object-server", "statsd_collection_pause", 10)
		basePrefix := serverconf.GetDefault("app:object-server", "log_statsd_metric_prefix", "")
		prefix := basePrefix + ".go.objectserver"
		go hummingbird.CollectRuntimeMetrics(statsdHost, statsdPort, statsdPause, prefix)
	}

	return bindIP, bindPort, server, server.logger, nil
}
