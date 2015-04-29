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
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

func FGetXattr(fd uintptr, attr string, value []byte) (int, error) {
	attrp, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return 0, err
	}
	if len(value) == 0 {
		if r0, _, e1 := syscall.Syscall6(syscall.SYS_FGETXATTR, fd, uintptr(unsafe.Pointer(attrp)), 0, 0, 0, 0); e1 == 0 {
			return int(r0), nil
		} else {
			return 0, e1
		}
	} else {
		valuep := unsafe.Pointer(&value[0])
		if r0, _, e1 := syscall.Syscall6(syscall.SYS_FGETXATTR, fd, uintptr(unsafe.Pointer(attrp)), uintptr(valuep), uintptr(len(value)), 0, 0); e1 == 0 {
			return int(r0), nil
		} else {
			return int(r0), e1
		}
	}
}

func FSetXattr(fd uintptr, attr string, value []byte) (int, error) {
	attrp, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return 0, err
	}
	valuep := unsafe.Pointer(&value[0])
	r0, _, e1 := syscall.Syscall6(syscall.SYS_FSETXATTR, fd, uintptr(unsafe.Pointer(attrp)), uintptr(valuep), uintptr(len(value)), 0, 0)
	if e1 != 0 {
		err = e1
	}
	return int(r0), nil
}

func Getpwnam(username string) (uint32, uint32, error) {
	file, err := os.Open("/etc/passwd")
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ":")
		if len(parts) > 6 && parts[0] == username {
			uid, err := strconv.ParseUint(parts[2], 0, 32)
			if err != nil {
				return 0, 0, fmt.Errorf("Error parsing uid from /etc/passwd")
			}
			gid, err := strconv.ParseUint(parts[3], 0, 32)
			if err != nil {
				return 0, 0, fmt.Errorf("Error parsing uid from /etc/passwd")
			}
			return uint32(uid), uint32(gid), nil
		}
	}
	return 0, 0, fmt.Errorf("User %s not found in /etc/passwd", username)
}
