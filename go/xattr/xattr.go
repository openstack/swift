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

package xattr

import "errors"

// Setxattr sets xattrs for a file, given a file descriptor, attribute name, and value buffer.
func Setxattr(fileNameOrFd interface{}, attr string, value []byte) (int, error) {
	switch v := fileNameOrFd.(type) {
	case string:
		return setxattr(v, attr, value)
	case uintptr:
		return fsetxattr(v, attr, value)
	case int:
		return fsetxattr(uintptr(v), attr, value)
	default:
		return 0, errors.New("Invalid fileNameOrFd")
	}
}

// Getxattr gets xattrs from a file, given a filename(string) or file descriptor(uintptr), an attribute name, and value buffer to store it to.
func Getxattr(fileNameOrFd interface{}, attr string, value []byte) (int, error) {
	switch v := fileNameOrFd.(type) {
	case string:
		return getxattr(v, attr, value)
	case uintptr:
		return fgetxattr(v, attr, value)
	case int:
		return fgetxattr(uintptr(v), attr, value)
	default:
		return 0, errors.New("Invalid fileNameOrFd")
	}
}
