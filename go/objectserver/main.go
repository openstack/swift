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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/syslog"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

type ObjectHandler struct {
	driveRoot        string
	hashPathPrefix   string
	hashPathSuffix   string
	checkEtags       bool
	checkMounts      bool
	asyncFinalize    bool
	allowedHeaders   map[string]bool
	logger           *syslog.Writer
	logLevel         string
	fallocateReserve int64
	disableFallocate bool
	diskInUse        *hummingbird.KeyedLimit
	replicationMan   *ReplicationManager
}

func (server *ObjectHandler) ObjGetHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	headers := writer.Header()
	hashDir := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix)
	dataFile, metaFile := ObjectFiles(hashDir)
	if dataFile == "" || strings.HasSuffix(dataFile, ".ts") {
		if im := request.Header.Get("If-Match"); im != "" && strings.Contains(im, "*") {
			writer.StandardResponse(http.StatusPreconditionFailed)
			return
		} else {
			writer.StandardResponse(http.StatusNotFound)
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
		request.LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
		if !os.IsNotExist(err) && QuarantineHash(hashDir) == nil {
			InvalidateHash(hashDir)
		}
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	contentLength, err := strconv.ParseInt(metadata["Content-Length"].(string), 10, 64)

	if stat, err := file.Stat(); err != nil || stat.Size() != contentLength {
		if QuarantineHash(hashDir) == nil {
			InvalidateHash(hashDir)
		}
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"].(string))
	if deleteAt, ok := metadata["X-Delete-At"].(string); ok {
		if deleteTime, err := hummingbird.ParseDate(deleteAt); err == nil && deleteTime.Before(time.Now()) {
			writer.StandardResponse(http.StatusNotFound)
			return
		}
	}

	lastModified, err := hummingbird.ParseDate(metadata["X-Timestamp"].(string))
	if err != nil {
		request.LogError("Error getting timestamp from %s: %s", dataFile, err.Error())
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	lastModifiedHeader := lastModified
	if lastModified.Nanosecond() > 0 { // for some reason, Last-Modified is ceil(X-Timestamp)
		lastModifiedHeader = lastModified.Truncate(time.Second).Add(time.Second)
	}
	headers.Set("Last-Modified", lastModifiedHeader.Format(time.RFC1123))
	headers.Set("ETag", "\""+metadata["ETag"].(string)+"\"")
	xTimestamp, err := hummingbird.GetEpochFromTimestamp(metadata["X-Timestamp"].(string))
	if err != nil {
		request.LogError("Error getting the epoch time from x-timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	headers.Set("X-Timestamp", xTimestamp)
	for key, value := range metadata {
		if allowed, ok := server.allowedHeaders[key.(string)]; (ok && allowed) ||
			strings.HasPrefix(key.(string), "X-Object-Meta-") ||
			strings.HasPrefix(key.(string), "X-Object-Sysmeta-") {
			headers.Set(key.(string), value.(string))
		}
	}

	if im := request.Header.Get("If-Match"); im != "" && !strings.Contains(im, metadata["ETag"].(string)) && !strings.Contains(im, "*") {
		writer.StandardResponse(http.StatusPreconditionFailed)
		return
	}

	if inm := request.Header.Get("If-None-Match"); inm != "" && (strings.Contains(inm, metadata["ETag"].(string)) || strings.Contains(inm, "*")) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	if ius, err := hummingbird.ParseDate(request.Header.Get("If-Unmodified-Since")); err == nil && lastModified.After(ius) {
		writer.StandardResponse(http.StatusPreconditionFailed)
		return
	}

	if ims, err := hummingbird.ParseDate(request.Header.Get("If-Modified-Since")); err == nil && lastModified.Before(ims) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	headers.Set("Accept-Ranges", "bytes")
	headers.Set("Content-Type", metadata["Content-Type"].(string))
	headers.Set("Content-Length", metadata["Content-Length"].(string))

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
			io.CopyN(writer, file, ranges[0].End-ranges[0].Start)
			return
		} else if ranges != nil && len(ranges) > 1 {
			w := multipart.NewWriter(writer)
			responseLength := int64(6 + len(w.Boundary()) + (len(w.Boundary())+len(metadata["Content-Type"].(string))+47)*len(ranges))
			for _, rng := range ranges {
				responseLength += int64(len(fmt.Sprintf("%d-%d/%d", rng.Start, rng.End-1, contentLength))) + rng.End - rng.Start
			}
			headers.Set("Content-Length", strconv.FormatInt(responseLength, 10))
			headers.Set("Content-Type", "multipart/byteranges;boundary="+w.Boundary())
			writer.WriteHeader(http.StatusPartialContent)
			for _, rng := range ranges {
				part, _ := w.CreatePart(textproto.MIMEHeader{"Content-Type": []string{metadata["Content-Type"].(string)},
					"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.End-1, contentLength)}})
				file.Seek(rng.Start, os.SEEK_SET)
				io.CopyN(part, file, rng.End-rng.Start)
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
			if hex.EncodeToString(hash.Sum(nil)) != metadata["ETag"].(string) && QuarantineHash(hashDir) == nil {
				InvalidateHash(hashDir)
			}
		} else {
			io.Copy(writer, file)
		}
	} else {
		writer.Write([]byte{})
	}
}

func (server *ObjectHandler) ObjPutHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
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
		request.LogError("Error standardizing request X-Timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}

	dataFile, metaFile := ObjectFiles(hashDir)
	if dataFile != "" && !strings.HasSuffix(dataFile, ".ts") {
		if inm := request.Header.Get("If-None-Match"); inm == "*" {
			writer.StandardResponse(http.StatusPreconditionFailed)
			return
		}
		if metadata, err := ObjectMetadata(dataFile, metaFile); err == nil {
			if requestTime, err := hummingbird.ParseDate(requestTimestamp); err == nil {
				if lastModified, err := hummingbird.ParseDate(metadata["X-Timestamp"].(string)); err == nil && !requestTime.After(lastModified) {
					outHeaders.Set("X-Backend-Timestamp", metadata["X-Timestamp"].(string))
					writer.StandardResponse(http.StatusConflict)
					return
				}
			}
			if inm := request.Header.Get("If-None-Match"); inm != "*" && strings.Contains(inm, metadata["ETag"].(string)) {
				writer.StandardResponse(http.StatusPreconditionFailed)
				return
			}
		}
	}

	fileName := hashDir + "/" + requestTimestamp + ".data"
	tempFile, err := ObjTempFile(vars, server.driveRoot, "PUT")
	if err != nil {
		request.LogError("Error creating temporary file in %s: %s", server.driveRoot, err.Error())
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	defer func() { // cleanup if we don't finish
		if writer.Status != http.StatusCreated {
			tempFile.Close()
			os.RemoveAll(tempFile.Name())
		}
	}()
	if freeSpace, err := FreeDiskSpace(tempFile.Fd()); err == nil && server.fallocateReserve > 0 && freeSpace-request.ContentLength < server.fallocateReserve {
		request.LogError("Hummingbird Not enough space available: %d available, %d requested", freeSpace, request.ContentLength)
		writer.CustomErrorResponse(507, vars)
		return
	}
	if !server.disableFallocate && request.ContentLength > 0 {
		syscall.Fallocate(int(tempFile.Fd()), 1, 0, request.ContentLength)
	}
	hash := md5.New()
	totalSize, err := hummingbird.Copy(request.Body, tempFile, hash)
	if err != nil {
		request.LogError("Error writing to file %s: %s", tempFile.Name(), err.Error())
		writer.StandardResponse(http.StatusInternalServerError)
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
	requestEtag := request.Header.Get("ETag")
	if requestEtag != "" && requestEtag != metadata["ETag"] {
		http.Error(writer, "Unprocessable Entity", 422)
		return
	}
	outHeaders.Set("ETag", metadata["ETag"])
	WriteMetadata(tempFile.Fd(), metadata)
	if !server.asyncFinalize { // for "super safe mode", this should happen before the rename.
		tempFile.Sync()
	}

	if os.MkdirAll(hashDir, 0770) != nil || os.Rename(tempFile.Name(), fileName) != nil {
		request.LogError("Error renaming object file: %s -> %s", tempFile.Name(), fileName)
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	finalize := func() {
		if server.asyncFinalize { // for "fast, lazy mode", this can happen after the rename.
			tempFile.Sync()
		}
		tempFile.Close()
		HashCleanupListDir(hashDir, request)
		if fd, err := syscall.Open(hashDir, syscall.O_DIRECTORY|os.O_RDONLY, 0666); err == nil {
			syscall.Fsync(fd)
			syscall.Close(fd)
		}
		InvalidateHash(hashDir)
		UpdateContainer(metadata, request, vars, hashDir)
		if request.Header.Get("X-Delete-At") != "" || request.Header.Get("X-Delete-After") != "" {
			vars["driveRoot"] = server.driveRoot
			vars["hashPathPrefix"] = server.hashPathPrefix
			vars["hashPathSuffix"] = server.hashPathSuffix
			UpdateDeleteAt(request, vars, hashDir)
		}
	}
	if server.asyncFinalize {
		go finalize()
	} else {
		finalize()
	}
	writer.StandardResponse(http.StatusCreated)
}

func (server *ObjectHandler) ObjDeleteHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	headers := writer.Header()
	requestTimestamp, err := hummingbird.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		request.LogError("Error standardizing request X-Timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	hashDir := ObjHashDir(vars, server.driveRoot, server.hashPathPrefix, server.hashPathSuffix)
	responseStatus := http.StatusNotFound

	dataFile, metaFile := ObjectFiles(hashDir)
	if ida := request.Header.Get("X-If-Delete-At"); ida != "" {
		_, err = strconv.ParseInt(ida, 10, 64)
		if err != nil {
			writer.StandardResponse(http.StatusBadRequest)
			return
		}
		if dataFile == "" {
			writer.StandardResponse(http.StatusNotFound)
			return
		}
		if !strings.HasSuffix(dataFile, ".data") {
			writer.StandardResponse(http.StatusPreconditionFailed)
			return
		}
		metadata, err := ObjectMetadata(dataFile, metaFile)
		if err != nil {
			request.LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
			writer.StandardResponse(http.StatusInternalServerError)
			return
		}
		if _, ok := metadata["X-Delete-At"]; ok {
			if ida != metadata["X-Delete-At"] {
				writer.StandardResponse(http.StatusPreconditionFailed)
				return
			}
		}
	}

	if dataFile != "" {
		if strings.HasSuffix(dataFile, ".data") {
			responseStatus = http.StatusNoContent
		}

		// TODO(redbo): I don't like that this function can call ObjectMetadata() twice on the same files.
		origMetadata, err := ObjectMetadata(dataFile, metaFile)
		if err == nil {
			// compare the timestamps here
			if origTimestamp, ok := origMetadata["X-Timestamp"]; ok && origTimestamp.(string) >= requestTimestamp {
				headers.Set("X-Backend-Timestamp", origTimestamp.(string))
				if strings.HasSuffix(dataFile, ".data") {
					writer.StandardResponse(http.StatusConflict)
					return
				} else {
					writer.StandardResponse(http.StatusNotFound)
					return
				}
			}
		} else if os.IsNotExist(err) {
			request.LogError("Listed data file now missing: %s", dataFile)
			responseStatus = http.StatusNotFound
		} else {
			request.LogError("Error getting metadata from (%s, %s): %s", dataFile, metaFile, err.Error())
			if qerr := QuarantineHash(hashDir); qerr == nil {
				InvalidateHash(hashDir)
			}
			responseStatus = http.StatusNotFound
		}
	}

	fileName := hashDir + "/" + requestTimestamp + ".ts"
	tempFile, err := ObjTempFile(vars, server.driveRoot, "DELETE")
	if err != nil {
		request.LogError("Error creating temporary file in %s: %s", server.driveRoot, err.Error())
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	metadata := map[string]string{
		"X-Timestamp": requestTimestamp,
		"name":        "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"],
	}
	WriteMetadata(tempFile.Fd(), metadata)
	if !server.asyncFinalize {
		tempFile.Sync()
	}
	if os.MkdirAll(hashDir, 0770) != nil || os.Rename(tempFile.Name(), fileName) != nil {
		request.LogError("Error renaming tombstone file: %s -> %s", tempFile.Name(), fileName)
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
	finalize := func() {
		if server.asyncFinalize {
			tempFile.Sync()
		}
		tempFile.Close()
		HashCleanupListDir(hashDir, request)
		if fd, err := syscall.Open(hashDir, syscall.O_DIRECTORY|os.O_RDONLY, 0666); err == nil {
			syscall.Fsync(fd)
			syscall.Close(fd)
		}
		InvalidateHash(hashDir)
		UpdateContainer(metadata, request, vars, hashDir)
		if request.Header.Get("X-Delete-At") != "" || request.Header.Get("X-Delete-After") != "" {
			vars["driveRoot"] = server.driveRoot
			vars["hashPathPrefix"] = server.hashPathPrefix
			vars["hashPathSuffix"] = server.hashPathSuffix
			UpdateDeleteAt(request, vars, hashDir)
		}
	}
	if server.asyncFinalize {
		go finalize()
	} else {
		finalize()
	}
	writer.StandardResponse(responseStatus)
}

func (server *ObjectHandler) ObjReplicateHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	repid := request.Header.Get("X-Replication-Id")
	if repid == "" { // if they didn't give us a repid, make one up so we can account for them
		repid = request.RemoteAddr + "/" + vars["device"] + "/" + vars["partition"]
	}
	if vars["suffixes"] == "" {
		if !server.replicationMan.BeginReplication(vars["device"], repid) {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	} else {
		server.replicationMan.Done(vars["device"], repid)
		if vars["suffixes"] == "end" {
			writer.WriteHeader(http.StatusOK)
			return
		}
	}

	var recalculate []string
	if len(vars["suffixes"]) > 0 {
		recalculate = strings.Split(vars["suffixes"], "-")
	}
	hashes, err := GetHashes(server.driveRoot, vars["device"], vars["partition"], recalculate, request)
	if err != nil {
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(hummingbird.PickleDumps(hashes))
}

func (server *ObjectHandler) ObjSyncHandler(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, vars map[string]string) {
	repid := request.Header.Get("X-Replication-Id")
	server.replicationMan.UpdateSession(vars["device"], repid)
	fileName := filepath.Clean(server.driveRoot + strings.Replace(request.URL.Path, "/", string(os.PathSeparator), -1))
	hashDir := filepath.Dir(fileName)

	if ext := filepath.Ext(fileName); (ext != ".data" && ext != ".ts" && ext != ".meta") || len(filepath.Base(filepath.Dir(fileName))) != 32 {
		writer.StandardResponse(http.StatusBadRequest)
		return
	}
	if hummingbird.Exists(fileName) {
		writer.StandardResponse(http.StatusConflict)
		return
	}
	dataFile, metaFile := ObjectFiles(hashDir)
	if filepath.Base(dataFile) >= filepath.Base(fileName) || metaFile == fileName {
		writer.StandardResponse(http.StatusConflict)
		return
	}
	tempFile, err := ObjTempFile(vars, server.driveRoot, "SYNC")
	if err != nil {
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	defer func() {
		tempFile.Close()
		if writer.Status != http.StatusCreated {
			os.RemoveAll(tempFile.Name())
		}
	}()
	if freeSpace, err := FreeDiskSpace(tempFile.Fd()); err != nil && freeSpace-request.ContentLength < server.fallocateReserve {
		writer.CustomErrorResponse(507, vars)
		return
	}
	if xattrs, err := hex.DecodeString(request.Header.Get("X-Attrs")); err != nil || len(xattrs) == 0 {
		writer.StandardResponse(http.StatusBadRequest)
		return
	} else if RawWriteMetadata(tempFile.Fd(), xattrs) != nil {
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	if !server.disableFallocate && request.ContentLength > 0 {
		syscall.Fallocate(int(tempFile.Fd()), 1, 0, request.ContentLength)
	}
	if _, err := hummingbird.Copy(request.Body, tempFile); err != nil {
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	tempFile.Sync()
	if os.MkdirAll(hashDir, 0770) != nil || os.Rename(tempFile.Name(), fileName) != nil {
		writer.StandardResponse(http.StatusInternalServerError)
		return
	}
	if dataFile != "" || metaFile != "" {
		HashCleanupListDir(hashDir, request)
	}
	InvalidateHash(hashDir)
	writer.StandardResponse(http.StatusCreated)
}

func (server *ObjectHandler) LogRequest(writer *hummingbird.WebWriter, request *hummingbird.WebRequest, extraInfo string) {
	go server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
		request.RemoteAddr,
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		request.Method,
		hummingbird.Urlencode(request.URL.Path),
		writer.Status,
		hummingbird.GetDefault(writer.Header(), "Content-Length", "-"),
		hummingbird.GetDefault(request.Header, "Referer", "-"),
		hummingbird.GetDefault(request.Header, "X-Trans-Id", "-"),
		hummingbird.GetDefault(request.Header, "User-Agent", "-"),
		time.Since(request.Start).Seconds(),
		extraInfo))
}

func (server *ObjectHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/healthcheck" {
		writer.Header().Set("Content-Length", "2")
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("OK"))
		return
	} else if strings.HasPrefix(request.URL.Path, "/recon/") {
		hummingbird.ReconHandler(server.driveRoot, writer, request)
		return
	} else if request.URL.Path == "/diskusage" {
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
	parts := strings.SplitN(request.URL.Path, "/", 6)
	vars := make(map[string]string)
	if len(parts) > 1 {
		vars["device"] = parts[1]
		if len(parts) > 2 {
			vars["partition"] = parts[2]
			if len(parts) > 3 {
				vars["account"] = parts[3]
				vars["suffixes"] = parts[3]
				if len(parts) > 4 {
					vars["container"] = parts[4]
					if len(parts) > 5 {
						vars["obj"] = parts[5]
					}
				}
			}
		}
	}

	newWriter := &hummingbird.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
	newRequest := &hummingbird.WebRequest{
		Request:       request,
		TransactionId: request.Header.Get("X-Trans-Id"),
		XTimestamp:    request.Header.Get("X-Timestamp"),
		Start:         time.Now(),
		Logger:        server.logger}
	defer newRequest.LogPanics(newWriter)
	forceAcquire := request.Header.Get("X-Force-Acquire") == "true"
	if server.logLevel == "DEBUG" || (request.Method != "SYNC" && request.Method != "REPLICATE") {
		extraInfo := "-"
		if forceAcquire {
			extraInfo = "FA"
		}
		defer server.LogRequest(newWriter, newRequest, extraInfo) // log the request after return
	}

	if !newRequest.ValidateRequest() {
		newWriter.StandardResponse(400)
		return
	}

	if server.checkMounts {
		devicePath := server.driveRoot + "/" + vars["device"]
		if mounted, err := hummingbird.IsMount(devicePath); err != nil || mounted != true {
			vars["Method"] = request.Method
			newWriter.CustomErrorResponse(507, vars)
			return
		}
	}

	if concRequests := server.diskInUse.Acquire(vars["device"], forceAcquire); concRequests > 0 {
		newWriter.Header().Set("X-Disk-Usage", strconv.FormatInt(concRequests, 10))
		newWriter.StandardResponse(503)
		return
	}
	defer server.diskInUse.Release(vars["device"])

	switch request.Method {
	case "GET":
		server.ObjGetHandler(newWriter, newRequest, vars)
	case "HEAD":
		server.ObjGetHandler(newWriter, newRequest, vars)
	case "PUT":
		server.ObjPutHandler(newWriter, newRequest, vars)
	case "DELETE":
		server.ObjDeleteHandler(newWriter, newRequest, vars)
	case "REPLICATE":
		server.ObjReplicateHandler(newWriter, newRequest, vars)
	case "SYNC":
		server.ObjSyncHandler(newWriter, newRequest, vars)
	default:
		newWriter.StandardResponse(http.StatusMethodNotAllowed)
	}
}

func GetServer(conf string) (string, int, http.Handler, *syslog.Writer, error) {
	handler := &ObjectHandler{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "",
		allowedHeaders: map[string]bool{"Content-Disposition": true,
			"Content-Encoding":      true,
			"X-Delete-At":           true,
			"X-Object-Manifest":     true,
			"X-Static-Large-Object": true,
		},
	}
	var err error
	handler.hashPathPrefix, handler.hashPathSuffix, err = hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return "", 0, nil, nil, err
	}

	serverconf, err := hummingbird.LoadIniFile(conf)
	if err != nil {
		return "", 0, nil, nil, errors.New(fmt.Sprintf("Unable to load %s", conf))
	}
	handler.driveRoot = serverconf.GetDefault("app:object-server", "devices", "/srv/node")
	handler.checkMounts = serverconf.GetBool("app:object-server", "mount_check", true)
	handler.asyncFinalize = serverconf.GetBool("app:object-server", "async_finalize", false)
	handler.checkEtags = serverconf.GetBool("app:object-server", "check_etags", false)
	handler.disableFallocate = serverconf.GetBool("app:object-server", "disable_fallocate", false)
	handler.fallocateReserve = serverconf.GetInt("app:object-server", "fallocate_reserve", 0)
	handler.logLevel = serverconf.GetDefault("app:object-server", "log_level", "INFO")
	handler.diskInUse = hummingbird.NewKeyedLimit(serverconf.GetLimit("app:object-server", "disk_limit", 25, 10000))
	bindIP := serverconf.GetDefault("app:object-server", "bind_ip", "0.0.0.0")
	bindPort := serverconf.GetInt("app:object-server", "bind_port", 6000)
	if allowedHeaders, ok := serverconf.Get("app:object-server", "allowed_headers"); ok {
		headers := strings.Split(allowedHeaders, ",")
		for i := range headers {
			handler.allowedHeaders[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(headers[i]))] = true
		}
	}
	handler.logger = hummingbird.SetupLogger(serverconf.GetDefault("app:object-server", "log_facility", "LOG_LOCAL1"), "object-server")
	handler.replicationMan = NewReplicationManager(serverconf.GetLimit("app:object-server", "replication_limit", 3, 100))

	return bindIP, int(bindPort), handler, handler.logger, nil
}
