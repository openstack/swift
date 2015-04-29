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
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

const METADATA_CHUNK_SIZE = 65536

func GetXAttr(fileNameOrFd interface{}, attr string, value []byte) (int, error) {
	switch v := fileNameOrFd.(type) {
	case string:
		return syscall.Getxattr(v, attr, value)
	case uintptr:
		return hummingbird.FGetXattr(v, attr, value)
	}
	return 0, &hummingbird.BackendError{Err: errors.New("Invalid fileNameOrFd"), Code: hummingbird.UnhandledError}
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
		length, _ := GetXAttr(fileNameOrFd, metadataName, nil)
		if length <= 0 {
			break
		}
		// grow buffer to hold xattr
		for cap(pickledMetadata) < offset+length {
			pickledMetadata = append(pickledMetadata, 0)
		}
		pickledMetadata = pickledMetadata[0 : offset+length]
		if _, err := GetXAttr(fileNameOrFd, metadataName, pickledMetadata[offset:]); err != nil {
			return nil, err
		}
		offset += length
	}
	return pickledMetadata, nil
}

func ReadMetadata(fileNameOrFd interface{}) (map[interface{}]interface{}, error) {
	pickledMetadata, err := RawReadMetadata(fileNameOrFd)
	if err != nil {
		return nil, err
	}
	v, err := hummingbird.PickleLoads(pickledMetadata)
	if err != nil {
		return nil, err
	}
	return v.(map[interface{}]interface{}), nil
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
		if _, err := hummingbird.FSetXattr(fd, metadataName, buf[0:writelen]); err != nil {
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
	hash := filepath.Base(hashDir)
	//          drive        objects      partition    suffix       hash
	driveDir := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(hashDir))))
	// TODO: this will need to be slightly more complicated once policies
	quarantineDir := filepath.Join(driveDir, "quarantined", "objects")
	if err := os.MkdirAll(quarantineDir, 0770); err != nil {
		return err
	}
	destDir := filepath.Join(quarantineDir, hash+"-"+hummingbird.UUID())
	if err := os.Rename(hashDir, destDir); err != nil {
		return err
	}
	return nil
}

func InvalidateHash(hashDir string) {
	// TODO: return errors
	suffDir := filepath.Dir(hashDir)
	partitionDir := filepath.Dir(suffDir)
	partitionLock, err := hummingbird.LockPath(partitionDir, 10)
	if err != nil {
		return
	}
	defer partitionLock.Close()
	pklFile := partitionDir + "/hashes.pkl"
	data, err := ioutil.ReadFile(pklFile)
	if err != nil {
		return
	}
	v, _ := hummingbird.PickleLoads(data)
	suffixDirSplit := strings.Split(suffDir, "/")
	suffix := suffixDirSplit[len(suffixDirSplit)-1]
	if current, ok := v.(map[interface{}]interface{})[suffix]; ok && (current == nil || current == "") {
		return
	}
	v.(map[interface{}]interface{})[suffix] = nil
	hummingbird.WriteFileAtomic(pklFile, hummingbird.PickleDumps(v), 0600)
}

func HashCleanupListDir(hashDir string, logger hummingbird.LoggingContext) ([]string, *hummingbird.BackendError) {
	fileList, err := hummingbird.ReadDirNames(hashDir)
	returnList := []string{}
	if err != nil {

		if os.IsNotExist(err) {
			return returnList, nil
		}
		if hummingbird.IsNotDir(err) {
			return returnList, &hummingbird.BackendError{Err: err, Code: hummingbird.PathNotDirErrorCode}
		}
		return returnList, &hummingbird.BackendError{Err: err, Code: hummingbird.OsErrorCode}
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
			if time.Now().Unix()-int64(timestamp) > int64(hummingbird.ONE_WEEK) {
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

func RecalculateSuffixHash(suffixDir string, logger hummingbird.LoggingContext) (string, *hummingbird.BackendError) {
	// the is hash_suffix in swift
	h := md5.New()

	hashList, err := hummingbird.ReadDirNames(suffixDir)
	if err != nil {
		if hummingbird.IsNotDir(err) {
			return "", &hummingbird.BackendError{Err: err, Code: hummingbird.PathNotDirErrorCode}
		}
		return "", &hummingbird.BackendError{Err: err, Code: hummingbird.OsErrorCode}
	}
	for _, fullHash := range hashList {
		hashPath := suffixDir + "/" + fullHash
		fileList, err := HashCleanupListDir(hashPath, logger)
		if err != nil {
			if err.Code == hummingbird.PathNotDirErrorCode {
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

func GetHashes(driveRoot string, device string, partition string, recalculate []string, logger hummingbird.LoggingContext) (map[string]string, *hummingbird.BackendError) {
	partitionDir := filepath.Join(driveRoot, device, "objects", partition)
	pklFile := filepath.Join(partitionDir, "hashes.pkl")

	modified := false
	mtime := int64(-1)
	hashes := make(map[string]string, 4096)
	lsForSuffixes := true
	if data, err := ioutil.ReadFile(pklFile); err == nil {
		if v, err := hummingbird.PickleLoads(data); err == nil {
			if pickledHashes, ok := v.(map[interface{}]interface{}); ok {
				if fileInfo, err := os.Stat(pklFile); err == nil {
					mtime = fileInfo.ModTime().Unix()
					lsForSuffixes = false
					for suff, hash := range pickledHashes {
						if hashes[suff.(string)], ok = hash.(string); !ok {
							hashes[suff.(string)] = ""
						}
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
	for suffix, hash := range hashes {
		if hash == "" {
			modified = true
			suffixDir := driveRoot + "/" + device + "/objects/" + partition + "/" + suffix
			recalc_hash, err := RecalculateSuffixHash(suffixDir, logger)
			if err == nil {
				hashes[suffix] = recalc_hash
			} else {
				switch {
				case err.Code == hummingbird.PathNotDirErrorCode:
					delete(hashes, suffix)
				case err.Code == hummingbird.OsErrorCode:
					logger.LogError("Error hashing suffix: %s/%s (%s)", partitionDir, suffix, "asdf")
				}
			}
		}
	}
	if modified {
		partitionLock, err := hummingbird.LockPath(partitionDir, 10)
		defer partitionLock.Close()
		if err != nil {
			return nil, &hummingbird.BackendError{Err: err, Code: hummingbird.LockPathError}
		} else {
			fileInfo, err := os.Stat(pklFile)
			if lsForSuffixes || os.IsNotExist(err) || mtime == fileInfo.ModTime().Unix() {
				hummingbird.WriteFileAtomic(pklFile, hummingbird.PickleDumps(hashes), 0600)
				return hashes, nil
			}
			logger.LogError("Made recursive call to GetHashes: %s", partitionDir)
			return GetHashes(driveRoot, device, partition, recalculate, logger)
		}
	}
	return hashes, nil
}

func ObjHashDir(vars map[string]string, driveRoot string, hashPathPrefix string, hashPathSuffix string) string {
	h := md5.New()
	io.WriteString(h, hashPathPrefix+"/"+vars["account"]+"/"+vars["container"]+"/"+vars["obj"]+hashPathSuffix)
	hexHash := hex.EncodeToString(h.Sum(nil))
	suffix := hexHash[29:32]
	return filepath.Join(driveRoot, vars["device"], "objects", vars["partition"], suffix, hexHash)
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

func ObjTempFile(vars map[string]string, driveRoot, prefix string) (*os.File, error) {
	tempDir := driveRoot + "/" + vars["device"] + "/" + "tmp"
	if err := os.MkdirAll(tempDir, 0770); err != nil {
		return nil, err
	}
	return ioutil.TempFile(tempDir, prefix)
}

func applyMetaFile(metaFile string, datafileMetadata map[interface{}]interface{}) (map[interface{}]interface{}, error) {
	if metadata, err := ReadMetadata(metaFile); err != nil {
		return nil, err
	} else {
		for k, v := range datafileMetadata {
			if k == "Content-Length" || k == "Content-Type" || k == "deleted" || k == "ETag" || strings.HasPrefix(k.(string), "X-Object-Sysmeta-") {
				metadata[k] = v
			}
		}
		return metadata, nil
	}
}

func OpenObjectMetadata(fd uintptr, metaFile string) (map[interface{}]interface{}, error) {
	pickledMetadata, err := RawReadMetadata(fd)
	if err != nil {
		return nil, err
	}
	v, err := hummingbird.PickleLoads(pickledMetadata)
	if err != nil {
		return nil, err
	}
	datafileMetadata, ok := v.(map[interface{}]interface{})
	if !ok {
		return nil, errors.New("Metadata unpickled to wrong type.")
	}
	if metaFile != "" {
		return applyMetaFile(metaFile, datafileMetadata)
	}
	return datafileMetadata, nil
}

func ObjectMetadata(dataFile string, metaFile string) (map[interface{}]interface{}, error) {
	datafileMetadata, err := ReadMetadata(dataFile)
	if err != nil {
		return nil, err
	}
	if metaFile != "" {
		return applyMetaFile(metaFile, datafileMetadata)
	}
	return datafileMetadata, nil
}

func FreeDiskSpace(fd uintptr) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(int(fd), &st); err != nil {
		return 0, err
	} else {
		return int64(st.Frsize) * int64(st.Bavail), nil
	}
}
