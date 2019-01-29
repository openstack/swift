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
	"bytes"
	"testing"
)

type getPartitionTest struct {
	ohash             string
	bitCount          int
	expectedPartition uint64
}

func TestGetPartitionFromOhash(t *testing.T) {
	var getPartitionTests = []getPartitionTest{
		{"b80362143ac3221d15a75f4bd1af3fac", 18, 188429},
		{"00db344e979c8c8fa4376dc60ba8102e", 18, 876},
		{"01342cbbf02d9b27396ac937b0f049e1", 18, 1232},
		{"ffffc63ac2fa908fc137e7e0f1c4df97", 18, 262143},
	}

	for _, tt := range getPartitionTests {
		partition, err := getPartitionFromOhash([]byte(tt.ohash), tt.bitCount)
		if err != nil {
			t.Error(err)
		}
		if partition != tt.expectedPartition {
			t.Errorf("For ohash: %s, got partition %d, expected %d\n", tt.ohash, partition, tt.expectedPartition)
		}
	}

	// Test invalid data, too short
	invalidHash := []byte("abcd")
	bitCount := 18
	_, err := getPartitionFromOhash(invalidHash, bitCount)
	if err == nil {
		t.Fatalf("Should fail to getPartitionFromOhash for: %x, %d", invalidHash, bitCount)
	}

	// invalid md5
	invalidHash = []byte("zzzz2cbbf02d9b27396ac937b0f049e1")
	_, err = getPartitionFromOhash(invalidHash, bitCount)
	if err == nil {
		t.Fatalf("Should fail to getPartitionFromOhash for: %x, %d", invalidHash, bitCount)
	}
}

type getLastPartitionTest struct {
	bitCount          int
	expectedPartition uint64
}

func TestGetLastPartition(t *testing.T) {
	var getLastPartitionTests = []getLastPartitionTest{
		{18, 262143},
		{17, 131071},
		{16, 65535},
	}

	for _, tt := range getLastPartitionTests {
		partition, err := getLastPartition(tt.bitCount)
		if err != nil {
			t.Error(err)
		}
		if partition != tt.expectedPartition {
			t.Errorf("For bitcount: %d, got last partition: %d, expected %d\n", tt.bitCount, partition, tt.expectedPartition)
		}
	}
}

type getObjTest struct {
	partition      uint64
	bitCount       int
	expectedPrefix []byte
}

func TestGetObjPrefixFromPartition(t *testing.T) {
	var getObjTests = []getObjTest{
		{876, 18, []byte("00db0000000000000000000000000000")},
		{209827, 18, []byte("cce8c000000000000000000000000000")},
		{260177, 18, []byte("fe144000000000000000000000000000")},
		{260179, 18, []byte("fe14c000000000000000000000000000")},
		{260180, 18, []byte("fe150000000000000000000000000000")},
	}

	for _, tt := range getObjTests {
		prefix, err := getObjPrefixFromPartition(tt.partition, tt.bitCount)
		if err != nil {
			t.Error(err)
		}
		if bytes.Compare(prefix, tt.expectedPrefix) != 0 {
			t.Errorf("For partition: %d, bitCount: %d, got prefix: %s, expected %s\n", tt.partition, tt.bitCount, prefix, tt.expectedPrefix)
		}
	}
}

func TestGetEncodedObjPrefixFromPartition(t *testing.T) {
	var getObjTests = []getObjTest{
		{876, 18, []byte("\x00\xdb\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
		{209827, 18, []byte("\xcc\xe8\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
		{260177, 18, []byte("\xfe\x14\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
		{260179, 18, []byte("\xfe\x14\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
		{260180, 18, []byte("\xfe\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
	}

	for _, tt := range getObjTests {
		prefix, err := getEncodedObjPrefixFromPartition(tt.partition, tt.bitCount)
		if err != nil {
			t.Error(err)
		}
		if bytes.Compare(prefix, tt.expectedPrefix) != 0 {
			t.Errorf("For partition: %d, bitCount: %d, got prefix: %x, expected %x\n", tt.partition, tt.bitCount, prefix, tt.expectedPrefix)
		}
	}
}
