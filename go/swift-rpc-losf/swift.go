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
	"errors"
	"fmt"
	"strconv"
)

// getPartitionFromOhash returns the partition, given an object hash and the partition bit count
func getPartitionFromOhash(ohash []byte, partitionBits int) (partition uint64, err error) {
	if len(ohash) < 16 {
		err = errors.New("ohash must be at least 16 bits long")
		return
	}
	highHash, err := strconv.ParseUint(string(ohash[0:16]), 16, 64)
	if err != nil {
		return
	}

	// shift to get the partition
	partition = highHash >> uint64(64-partitionBits)
	return
}

// getLastPartition returns the last possible partition given the partition bit count
func getLastPartition(partitionBits int) (partition uint64, err error) {
	for i := 0; i < partitionBits; i++ {
		partition |= 1 << uint64(i)
	}
	return
}

// Returns the first possible object prefix the KV for the given partition and bit count
// Example: 876, 18 bits -> 00db000000000000
func getObjPrefixFromPartition(partition uint64, partitionBits int) (prefix []byte, err error) {
	firstnum := partition << uint64(64-partitionBits)
	prefix = []byte(fmt.Sprintf("%016x0000000000000000", firstnum))
	return
}

// Returns the first possible object prefix the KV for the given partition and bit count,
// in its encoded form
func getEncodedObjPrefixFromPartition(partition uint64, partitionBits int) (prefix []byte, err error) {
	key, err := getObjPrefixFromPartition(partition, partitionBits)
	if err != nil {
		return
	}

	prefix, err = EncodeObjectKey(key)
	return
}
