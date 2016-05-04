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
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

// SwiftObject implements an Object that is compatible with Swift's object server.
type SwiftObject struct {
	file         *os.File
	afw          AtomicFileWriter
	hashDir      string
	tempDir      string
	dataFile     string
	metaFile     string
	workingClass string
	metadata     map[string]string
	reserve      int64
	reclaimAge   int64
}

// Metadata returns the object's metadata.
func (o *SwiftObject) Metadata() map[string]string {
	return o.metadata
}

// ContentLength parses and returns the Content-Length for the object.
func (o *SwiftObject) ContentLength() int64 {
	if contentLength, err := strconv.ParseInt(o.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

// Quarantine removes the object's underlying files to the Quarantined directory on the device.
func (o *SwiftObject) Quarantine() error {
	o.Close()
	if QuarantineHash(o.hashDir) == nil {
		return InvalidateHash(o.hashDir)
	}
	return nil
}

// Exists returns true if the object exists, that is if it has a .data file.
func (o *SwiftObject) Exists() bool {
	return strings.HasSuffix(o.dataFile, ".data")
}

// Copy copies all data from the underlying .data file to the given writers.
func (o *SwiftObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if len(dsts) == 1 {
		return io.Copy(dsts[0], o.file)
	} else {
		return hummingbird.Copy(o.file, dsts...)
	}
}

// CopyRange copies data in the range of start to end from the underlying .data file to the writer.
func (o *SwiftObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	if _, err := o.file.Seek(start, os.SEEK_SET); err != nil {
		return 0, err
	}
	return hummingbird.CopyN(o.file, end-start, w)
}

// Repr returns a string that identifies the object in some useful way, used for logging.
func (o *SwiftObject) Repr() string {
	if o.dataFile != "" && o.metaFile != "" {
		return fmt.Sprintf("SwiftObject(%s, %s)", o.dataFile, o.metaFile)
	} else if o.dataFile != "" {
		return fmt.Sprintf("SwiftObject(%s)", o.dataFile)
	}
	return fmt.Sprintf("SwiftObject(%s)", o.hashDir)
}

func (o *SwiftObject) newFile(class string, size int64) (io.Writer, error) {
	var err error
	o.Close()
	if o.afw, err = NewAtomicFileWriter(o.tempDir, o.hashDir); err != nil {
		return nil, fmt.Errorf("Error creating temp file: %v", err)
	}
	if err := o.afw.Preallocate(size, o.reserve); err != nil {
		o.afw.Abandon()
		return nil, DriveFullError
	}
	o.workingClass = class
	return o.afw, nil
}

// SetData is called to set the object's data.  It takes a size (if available, otherwise set to zero).
func (o *SwiftObject) SetData(size int64) (io.Writer, error) {
	return o.newFile("data", size)
}

// Commit commits an open data file to disk, given the metadata.
func (o *SwiftObject) Commit(metadata map[string]string) error {
	defer o.afw.Abandon()
	timestamp, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("No timestamp in metadata")
	}
	if err := WriteMetadata(o.afw.Fd(), metadata); err != nil {
		return fmt.Errorf("Error writing metadata: %v", err)
	}
	fileName := filepath.Join(o.hashDir, fmt.Sprintf("%s.%s", timestamp, o.workingClass))
	o.afw.Save(fileName)
	go func() {
		HashCleanupListDir(o.hashDir, o.reclaimAge)
		if dir, err := os.OpenFile(o.hashDir, os.O_RDONLY, 0666); err == nil {
			dir.Sync()
			dir.Close()
		}
		InvalidateHash(o.hashDir)
	}()
	return nil
}

// Delete deletes the object.
func (o *SwiftObject) Delete(metadata map[string]string) error {
	if _, err := o.newFile("ts", 0); err != nil {
		return err
	} else {
		defer o.Close()
		return o.Commit(metadata)
	}
}

// Close releases any resources used by the instance of SwiftObject
func (o *SwiftObject) Close() error {
	if o.afw != nil {
		defer o.afw.Abandon()
		o.afw = nil
	}
	if o.file != nil {
		defer o.file.Close()
		o.file = nil
	}
	return nil
}

type SwiftObjectFactory struct {
	driveRoot        string
	hashPathPrefix   string
	hashPathSuffix   string
	reserve          int64
	replicationMan   *ReplicationManager
	replicateTimeout time.Duration
	reclaimAge       int64
}

// New returns an instance of SwiftObject with the given parameters. Metadata is read in and if needData is true, the file is opened.
func (f *SwiftObjectFactory) New(vars map[string]string, needData bool) (Object, error) {
	var err error
	sor := &SwiftObject{reclaimAge: f.reclaimAge, reserve: f.reserve}
	sor.hashDir = ObjHashDir(vars, f.driveRoot, f.hashPathPrefix, f.hashPathSuffix)
	sor.tempDir = TempDirPath(f.driveRoot, vars["device"])
	sor.dataFile, sor.metaFile = ObjectFiles(sor.hashDir)
	if sor.Exists() {
		var stat os.FileInfo
		if needData {
			if sor.file, err = os.Open(sor.dataFile); err != nil {
				return nil, err
			}
			if sor.metadata, err = OpenObjectMetadata(sor.file.Fd(), sor.metaFile); err != nil {
				sor.Quarantine()
				return nil, fmt.Errorf("Error getting metadata: %v", err)
			}
		} else {
			if sor.metadata, err = ObjectMetadata(sor.dataFile, sor.metaFile); err != nil {
				sor.Quarantine()
				return nil, fmt.Errorf("Error getting metadata: %v", err)
			}
		}
		if sor.file != nil {
			if stat, err = sor.file.Stat(); err != nil {
				sor.Close()
				return nil, fmt.Errorf("Error statting file: %v", err)
			}
		} else if stat, err = os.Stat(sor.dataFile); err != nil {
			return nil, fmt.Errorf("Error statting file: %v", err)
		}
		if contentLength, err := strconv.ParseInt(sor.metadata["Content-Length"], 10, 64); err != nil {
			sor.Quarantine()
			return nil, fmt.Errorf("Unable to parse content-length: %s", sor.metadata["Content-Length"])
		} else if stat.Size() != contentLength {
			sor.Quarantine()
			return nil, fmt.Errorf("File size doesn't match content-length: %d vs %d", stat.Size(), contentLength)
		}
	}
	return sor, nil
}

func (f *SwiftObjectFactory) objReplicateHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)

	var recalculate []string
	if len(vars["suffixes"]) > 0 {
		recalculate = strings.Split(vars["suffixes"], "-")
	}
	hashes, err := GetHashes(f.driveRoot, vars["device"], vars["partition"], recalculate, f.reclaimAge, hummingbird.GetLogger(request))
	if err != nil {
		hummingbird.GetLogger(request).LogError("Unable to get hashes for %s/%s", vars["device"], vars["partition"])
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(hummingbird.PickleDumps(hashes))
}

var replicationDone = fmt.Errorf("Replication done")

func (f *SwiftObjectFactory) objRepConnHandler(writer http.ResponseWriter, request *http.Request) {
	var conn net.Conn
	var rw *bufio.ReadWriter
	var err error
	var brr BeginReplicationRequest

	vars := hummingbird.GetVars(request)

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
	if !f.replicationMan.Begin(brr.Device, f.replicateTimeout) {
		hummingbird.GetLogger(request).LogError("[ObjRepConnHandler] Timed out waiting for concurrency slot")
		writer.WriteHeader(503)
		return
	}
	defer f.replicationMan.Done(brr.Device)
	var hashes map[string]string
	if brr.NeedHashes {
		var herr *hummingbird.BackendError
		hashes, herr = GetHashes(f.driveRoot, brr.Device, brr.Partition, nil, f.reclaimAge, hummingbird.GetLogger(request))
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
		errType, err := func() (string, error) { // this is a closure so we can use defers inside
			var sfr SyncFileRequest
			if err := rc.RecvMessage(&sfr); err != nil {
				return "receiving SyncFileRequest", err
			}
			if sfr.Done {
				return "", replicationDone
			}
			tempDir := TempDirPath(f.driveRoot, vars["device"])
			fileName := filepath.Join(f.driveRoot, sfr.Path)
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
			if err := tempFile.Preallocate(sfr.Size, f.reserve); err != nil {
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
				HashCleanupListDir(hashDir, f.reclaimAge)
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

// RegisterHandlers registers custom routes needed by the swift object engine.
func (f *SwiftObjectFactory) RegisterHandlers(addRoute func(method, path string, handler http.HandlerFunc)) {
	addRoute("REPLICATE", "/:device/:partition/:suffixes", f.objReplicateHandler)
	addRoute("REPLICATE", "/:device/:partition", f.objReplicateHandler)
	addRoute("REPCONN", "/:device/:partition", f.objRepConnHandler)
}

// SwiftEngineConstructor creates a SwiftObjectFactory given the object server configs.
func SwiftEngineConstructor(config hummingbird.IniFile, flags *flag.FlagSet) (ObjectEngine, error) {
	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	reserve := config.GetInt("app:object-server", "fallocate_reserve", 0)
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, errors.New("Unable to load hashpath prefix and suffix")
	}
	replicationMan := NewReplicationManager(config.GetLimit("app:object-server", "replication_limit", 3, 100))
	replicateTimeout := time.Minute // TODO(redbo): does this need to be configurable?
	reclaimAge := int64(config.GetInt("app:object-server", "reclaim_age", int64(hummingbird.ONE_WEEK)))
	return &SwiftObjectFactory{
		driveRoot:        driveRoot,
		hashPathPrefix:   hashPathPrefix,
		hashPathSuffix:   hashPathSuffix,
		reserve:          reserve,
		replicationMan:   replicationMan,
		replicateTimeout: replicateTimeout,
		reclaimAge:       reclaimAge}, nil
}

func init() {
	RegisterObjectEngine("swift", SwiftEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ ObjectEngineConstructor = SwiftEngineConstructor
var _ Object = &SwiftObject{}
var _ ObjectEngine = &SwiftObjectFactory{}
