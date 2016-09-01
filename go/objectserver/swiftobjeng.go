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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	reserve        int64
	reclaimAge     int64
	policy         int
}

// New returns an instance of SwiftObject with the given parameters. Metadata is read in and if needData is true, the file is opened.
func (f *SwiftObjectFactory) New(vars map[string]string, needData bool) (Object, error) {
	var err error
	sor := &SwiftObject{reclaimAge: f.reclaimAge, reserve: f.reserve}
	sor.hashDir = ObjHashDir(vars, f.driveRoot, f.hashPathPrefix, f.hashPathSuffix, f.policy)
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

var replicationDone = fmt.Errorf("Replication done")

// SwiftEngineConstructor creates a SwiftObjectFactory given the object server configs.
func SwiftEngineConstructor(config hummingbird.Config, policy *hummingbird.Policy, flags *flag.FlagSet) (ObjectEngine, error) {
	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	reserve := config.GetInt("app:object-server", "fallocate_reserve", 0)
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, errors.New("Unable to load hashpath prefix and suffix")
	}
	reclaimAge := int64(config.GetInt("app:object-server", "reclaim_age", int64(hummingbird.ONE_WEEK)))
	return &SwiftObjectFactory{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		reserve:        reserve,
		reclaimAge:     reclaimAge,
		policy:         policy.Index}, nil
}

func init() {
	RegisterObjectEngine("replication", SwiftEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ ObjectEngineConstructor = SwiftEngineConstructor
var _ Object = &SwiftObject{}
var _ ObjectEngine = &SwiftObjectFactory{}
