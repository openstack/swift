// Copyright (c) 2010-2012 OpenStack Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"strings"
)

// returns true is dirPath is mounted, false otherwise
func isMounted(dirPath string) (bool, error) {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		elems := strings.Split(scanner.Text(), " ")
		if dirPath == elems[1] {
			return true, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return false, err
	}

	return false, nil
}

// Returns true if path exists and is a directory, false otherwise
func dirExists(dirPath string) (bool, error) {
	stat, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	if stat.IsDir() {
		return true, nil
	}
	return false, nil
}

// Returns the baseDir string from rootDir
func getBaseDirName(rootDir string, policyIdx int) string {
	if policyIdx == 0 {
		return rootDir
	}

	return fmt.Sprintf("%s-%d", rootDir, policyIdx)
}

// Create a file on the filesystem that will signal to the object-server
// that the KV cannot be used. (done along with check_mount)
func CreateDirtyFile(dirtyFilePath string) (err error) {
	f, err := os.Create(dirtyFilePath)
	if err != nil {
		return
	}
	f.Close()
	return
}

// global variable, lest the file will be autoclosed and the lock released when
// the lockSocket() function returns.
var lockFile *os.File

// Acquire a lock on a file to protect the RPC socket.
// Does not block and will return an error if the lock cannot be acquired
// There is no explicit unlock, it will be unlocked when the process stops
func lockSocket(socketPath string) (err error) {
	lockFilePath := fmt.Sprintf("%s.lock", socketPath)

	lockFile, err = os.OpenFile(lockFilePath, os.O_WRONLY|os.O_CREATE, 00600)
	if err != nil {
		return
	}

	err = unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	return
}
