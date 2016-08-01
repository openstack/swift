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
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/xattr"
)

const METADATA_CHUNK_SIZE = 65536

var LockPathError = errors.New("Error locking path")
var PathNotDirError = errors.New("Path is not a directory")

// AtomicFileWriter saves a new file atomically.
type AtomicFileWriter interface {
	// Write writes the data to the underlying file.
	Write([]byte) (int, error)
	// Fd returns the file's underlying file descriptor.
	Fd() uintptr
	// Save atomically writes the file to its destination.
	Save(string) error
	// Abandon removes any resources associated with this file.
	Abandon() error
	// Preallocate pre-allocates space on disk, given the expected file size and disk reserve size.
	Preallocate(int64, int64) error
}

func PolicyDir(policy int) string {
	if policy == 0 {
		return "objects"
	}
	return fmt.Sprintf("objects-%d", policy)
}

func UnPolicyDir(dir string) (int, error) {
	if dir == "objects" {
		return 0, nil
	}
	var policy int
	if n, err := fmt.Sscanf(dir, "objects-%d", &policy); n == 1 && err == nil {
		return policy, nil
	}
	return 0, fmt.Errorf("Unable to parse policy from dir")
}

func RawReadMetadata(fileNameOrFd interface{}) ([]byte, error) {
	var pickledMetadata []byte
	offset := 0
	for index := 0; ; index += 1 {
		var metadataName string
		// get name of next xattr
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = "user.swift.metadata" + strconv.Itoa(index)
		}
		// get size of xattr
		length, err := xattr.Getxattr(fileNameOrFd, metadataName, nil)
		if err != nil || length <= 0 {
			break
		}
		// grow buffer to hold xattr
		for cap(pickledMetadata) < offset+length {
			pickledMetadata = append(pickledMetadata, 0)
		}
		pickledMetadata = pickledMetadata[0 : offset+length]
		if _, err := xattr.Getxattr(fileNameOrFd, metadataName, pickledMetadata[offset:]); err != nil {
			return nil, err
		}
		offset += length
	}
	return pickledMetadata, nil
}

func ReadMetadata(fileNameOrFd interface{}) (map[string]string, error) {
	pickledMetadata, err := RawReadMetadata(fileNameOrFd)
	if err != nil {
		return nil, err
	}
	v, err := hummingbird.PickleLoads(pickledMetadata)
	if err != nil {
		return nil, err
	}
	if v, ok := v.(map[interface{}]interface{}); ok {
		metadata := make(map[string]string, len(v))
		for mk, mv := range v {
			var mks, mvs string
			if mks, ok = mk.(string); !ok {
				return nil, fmt.Errorf("Metadata key not string: %v", mk)
			} else if mvs, ok = mv.(string); !ok {
				return nil, fmt.Errorf("Metadata value not string: %v", mv)
			}
			metadata[mks] = mvs
		}
		return metadata, nil
	}
	return nil, fmt.Errorf("Unpickled metadata not correct type")
}

func RawWriteMetadata(fd uintptr, buf []byte) error {
	for index := 0; len(buf) > 0; index++ {
		var metadataName string
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = "user.swift.metadata" + strconv.Itoa(index)
		}
		writelen := METADATA_CHUNK_SIZE
		if len(buf) < writelen {
			writelen = len(buf)
		}
		if _, err := xattr.Setxattr(fd, metadataName, buf[0:writelen]); err != nil {
			return err
		}
		buf = buf[writelen:len(buf)]
	}
	return nil
}

func WriteMetadata(fd uintptr, v map[string]string) error {
	return RawWriteMetadata(fd, hummingbird.PickleDumps(v))
}

func QuarantineHash(hashDir string) error {
	// FYI- this does not invalidate the hash like swift's version. Please
	// do that yourself
	//          objects      partition    suffix       hash
	objsDir := filepath.Dir(filepath.Dir(filepath.Dir(hashDir)))
	driveDir := filepath.Dir(objsDir)
	quarantineDir := filepath.Join(driveDir, "quarantined", filepath.Base(objsDir))
	if err := os.MkdirAll(quarantineDir, 0755); err != nil {
		return err
	}
	hash := filepath.Base(hashDir)
	destDir := filepath.Join(quarantineDir, hash+"-"+hummingbird.UUID())
	if err := os.Rename(hashDir, destDir); err != nil {
		return err
	}
	return nil
}

// InvalidateHash invalidates the hashdir's suffix hash, indicating it needs to be recalculated.
func InvalidateHash(hashDir string) error {
	suffDir := filepath.Dir(hashDir)
	partitionDir := filepath.Dir(suffDir)

	if partitionLock, err := hummingbird.LockPath(partitionDir, 10*time.Second); err != nil {
		return err
	} else {
		defer partitionLock.Close()
	}
	fp, err := os.OpenFile(filepath.Join(partitionDir, "hashes.invalid"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fmt.Fprintf(fp, "%s\n", filepath.Base(suffDir))
	return err
}

func HashCleanupListDir(hashDir string, reclaimAge int64) ([]string, error) {
	fileList, err := hummingbird.ReadDirNames(hashDir)
	returnList := []string{}
	if err != nil {
		if os.IsNotExist(err) {
			return returnList, nil
		}
		if hummingbird.IsNotDir(err) {
			return returnList, PathNotDirError
		}
		return returnList, err
	}
	deleteRest := false
	deleteRestMeta := false
	if len(fileList) == 1 {
		filename := fileList[0]
		if strings.HasSuffix(filename, ".ts") {
			withoutSuffix := strings.Split(filename, ".")[0]
			if strings.Contains(withoutSuffix, "_") {
				withoutSuffix = strings.Split(withoutSuffix, "_")[0]
			}
			timestamp, _ := strconv.ParseFloat(withoutSuffix, 64)
			if time.Now().Unix()-int64(timestamp) > reclaimAge {
				os.RemoveAll(hashDir + "/" + filename)
				return returnList, nil
			}
		}
		returnList = append(returnList, filename)
	} else {
		for index := len(fileList) - 1; index >= 0; index-- {
			filename := fileList[index]
			if deleteRest {
				os.RemoveAll(hashDir + "/" + filename)
			} else {
				if strings.HasSuffix(filename, ".meta") {
					if deleteRestMeta {
						os.RemoveAll(hashDir + "/" + filename)
						continue
					}
					deleteRestMeta = true
				}
				if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
					// TODO: check .ts time for expiration
					deleteRest = true
				}
				returnList = append(returnList, filename)
			}
		}
	}
	return returnList, nil
}

func RecalculateSuffixHash(suffixDir string, reclaimAge int64) (string, error) {
	// the is hash_suffix in swift
	h := md5.New()

	hashList, err := hummingbird.ReadDirNames(suffixDir)
	if err != nil {
		if hummingbird.IsNotDir(err) {
			return "", PathNotDirError
		}
		return "", err
	}
	for _, fullHash := range hashList {
		hashPath := suffixDir + "/" + fullHash
		fileList, err := HashCleanupListDir(hashPath, reclaimAge)
		if err != nil {
			if err == PathNotDirError {
				if QuarantineHash(hashPath) == nil {
					InvalidateHash(hashPath)
				}
				continue
			}
			return "", err
		}
		if len(fileList) > 0 {
			for _, fileName := range fileList {
				io.WriteString(h, fileName)
			}
		} else {
			os.Remove(hashPath) // leaves the suffix (swift removes it but who cares)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func GetHashes(driveRoot string, device string, partition string, recalculate []string, reclaimAge int64, policy int, logger hummingbird.LoggingContext) (map[string]string, error) {
	partitionDir := filepath.Join(driveRoot, device, PolicyDir(policy), partition)
	pklFile := filepath.Join(partitionDir, "hashes.pkl")
	invalidFile := filepath.Join(partitionDir, "hashes.invalid")

	modified := false
	hashes := make(map[string]string, 4096)
	lsForSuffixes := true
	if data, err := ioutil.ReadFile(pklFile); err == nil {
		if v, err := hummingbird.PickleLoads(data); err == nil {
			if pickledHashes, ok := v.(map[interface{}]interface{}); ok {
				lsForSuffixes = false
				for suff, hash := range pickledHashes {
					if hashes[suff.(string)], ok = hash.(string); !ok {
						hashes[suff.(string)] = ""
					}
				}
			}
		}
	}
	// check occasionally to see if there are any suffixes not in the hashes.pkl
	if !lsForSuffixes && len(hashes) < 4096 {
		lsForSuffixes = rand.Int31n(10) == 0
	}
	if lsForSuffixes {
		// couldn't load hashes pickle, start building new one
		suffs, _ := hummingbird.ReadDirNames(partitionDir)

		for _, suffName := range suffs {
			if len(suffName) == 3 && hashes[suffName] == "" {
				hashes[suffName] = ""
			}
		}
	}
	for _, suffix := range recalculate {
		if len(suffix) == 3 {
			hashes[suffix] = ""
		}
	}
	mtime := int64(-1)
	if ivf, err := os.OpenFile(invalidFile, os.O_RDWR, 0660); err == nil {
		defer ivf.Close()
		if fileInfo, err := ivf.Stat(); err == nil {
			mtime = fileInfo.ModTime().Unix()
			scanner := bufio.NewScanner(ivf)
			for scanner.Scan() {
				if suff := scanner.Text(); len(suff) == 3 && strings.Trim(suff, "0123456789abcdef") == "" {
					hashes[suff] = ""
				}
			}
		}
	}

	for suffix, hash := range hashes {
		if hash == "" {
			modified = true
			suffixDir := filepath.Join(partitionDir, suffix)
			recalc_hash, err := RecalculateSuffixHash(suffixDir, reclaimAge)
			switch err {
			case nil:
				hashes[suffix] = recalc_hash
			case PathNotDirError:
				delete(hashes, suffix)
			default:
				logger.LogError("Error hashing suffix: %s/%s (%v)", partitionDir, suffix, err)
			}
		}
	}
	if modified {
		partitionLock, err := hummingbird.LockPath(partitionDir, 10*time.Second)
		defer partitionLock.Close()
		if err != nil {
			return nil, LockPathError
		} else {
			fileInfo, err := os.Stat(invalidFile)
			if lsForSuffixes || os.IsNotExist(err) || mtime == fileInfo.ModTime().Unix() {
				tempDir := TempDirPath(driveRoot, device)
				if tempFile, err := NewAtomicFileWriter(tempDir, partitionDir); err == nil {
					defer tempFile.Abandon()
					tempFile.Write(hummingbird.PickleDumps(hashes))
					tempFile.Save(pklFile)
				}
				os.Truncate(invalidFile, 0)
				return hashes, nil
			}
			logger.LogError("Made recursive call to GetHashes: %s", partitionDir)
			partitionLock.Close()
			return GetHashes(driveRoot, device, partition, recalculate, reclaimAge, policy, logger)
		}
	}
	return hashes, nil
}

func ObjHashDir(vars map[string]string, driveRoot string, hashPathPrefix string, hashPathSuffix string, policy int) string {
	h := md5.New()
	io.WriteString(h, hashPathPrefix+"/"+vars["account"]+"/"+vars["container"]+"/"+vars["obj"]+hashPathSuffix)
	hexHash := hex.EncodeToString(h.Sum(nil))
	suffix := hexHash[29:32]
	return filepath.Join(driveRoot, vars["device"], PolicyDir(policy), vars["partition"], suffix, hexHash)
}

func ObjectFiles(directory string) (string, string) {
	fileList, err := hummingbird.ReadDirNames(directory)
	metaFile := ""
	if err != nil {
		return "", ""
	}
	for index := len(fileList) - 1; index >= 0; index-- {
		filename := fileList[index]
		if strings.HasSuffix(filename, ".meta") {
			metaFile = filename
		}
		if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
			if metaFile != "" {
				return filepath.Join(directory, filename), filepath.Join(directory, metaFile)
			} else {
				return filepath.Join(directory, filename), ""
			}
		}
	}
	return "", ""
}

func applyMetaFile(metaFile string, datafileMetadata map[string]string) (map[string]string, error) {
	if metadata, err := ReadMetadata(metaFile); err != nil {
		return nil, err
	} else {
		for k, v := range datafileMetadata {
			if k == "Content-Length" || k == "Content-Type" || k == "deleted" || k == "ETag" || strings.HasPrefix(k, "X-Object-Sysmeta-") {
				metadata[k] = v
			}
		}
		return metadata, nil
	}
}

func OpenObjectMetadata(fd uintptr, metaFile string) (map[string]string, error) {
	datafileMetadata, err := ReadMetadata(fd)
	if err != nil {
		return nil, err
	}
	if metaFile != "" {
		return applyMetaFile(metaFile, datafileMetadata)
	}
	return datafileMetadata, nil
}

func ObjectMetadata(dataFile string, metaFile string) (map[string]string, error) {
	datafileMetadata, err := ReadMetadata(dataFile)
	if err != nil {
		return nil, err
	}
	if metaFile != "" {
		return applyMetaFile(metaFile, datafileMetadata)
	}
	return datafileMetadata, nil
}

func TempDirPath(driveRoot string, device string) string {
	return filepath.Join(driveRoot, device, "tmp")
}
