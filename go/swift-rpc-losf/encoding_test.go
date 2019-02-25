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

type dfKeyTest struct {
	index uint32
	value []byte
}

func TestVolumeKey(t *testing.T) {
	var dFKeyTests = []dfKeyTest{
		{0, []byte("\x00")},
		{1, []byte("\x01")},
		{123, []byte("\x7b")},
		{863523, []byte("\xa3\xda\x34")},
		{1<<32 - 1, []byte("\xff\xff\xff\xff\x0f")},
	}

	for _, tt := range dFKeyTests {
		// Test encoding
		val := EncodeVolumeKey(tt.index)
		if !bytes.Equal(val, tt.value) {
			t.Errorf("For index: %d, got %x, expected %x", tt.index, val, tt.value)
		}

		// Test decoding
		index, err := DecodeVolumeKey(val)
		if err != nil {
			t.Fatal(err)
		}
		if index != tt.index {
			t.Errorf("For value: %x, got %d, expected %d", val, index, tt.index)
		}

	}

	// Test overflow
	m1 := []byte{0x80, 0x80, 0x80, 0x80}
	_, err := DecodeVolumeKey(m1)
	if err == nil {
		t.Errorf("We should fail to decode %x", m1)
	}
}

type dfValueTest struct {
	partition    int64
	volumeType int32
	nextOffset   int64
	usedSpace    int64
	state        int64
	value        []byte
}

func TestVolumeValue(t *testing.T) {
	var dfValueTests = []dfValueTest{
		{0, 0, 0, 0, 0, []byte("\x00\x00\x00\x00\x00")},
		{1343, 12, 3345314, 9821637, 2, []byte("\xbf\x0a\x0c\xa2\x97\xcc\x01\xc5\xbb\xd7\x04\x02")},
		{^int64(0), ^int32(0), ^int64(0), ^int64(0), ^int64(0), bytes.Repeat([]byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01"), 5)},
		// any negative value does not make sense, and should be caught by the RPC.
		// test anyway, they get cast to uint64.
		{-3572, 12, -1977878, 66666, -999999,
			[]byte("\x8c\xe4\xff\xff\xff\xff\xff\xff\xff\x01\x0c\xea\xa3\x87\xff\xff\xff" +
				"\xff\xff\xff\x01\xea\x88\x04\xc1\xfb\xc2\xff\xff\xff\xff\xff\xff\x01")},
	}

	for _, tt := range dfValueTests {
		// Test encoding
		val := EncodeVolumeValue(tt.partition, tt.volumeType, tt.nextOffset, tt.usedSpace, tt.state)
		if !bytes.Equal(val, tt.value) {
			t.Errorf("For partition: %d, volumeType: %d, nextOffset: %d, usedSpace: %d, state: %d "+
				"got: %x, expected: %x",
				tt.partition, tt.volumeType, tt.nextOffset, tt.usedSpace, tt.state, val, tt.value)
		}

		// Test decoding
		partition, volumeType, nextOffset, usedSpace, state, err := DecodeVolumeValue(tt.value)
		if err != nil {
			t.Error(err)
		}
		if partition != tt.partition {
			t.Errorf("Decoding value: %x, expected: %d, got: %d", tt.value, tt.partition, partition)
		}
		if volumeType != tt.volumeType {
			t.Errorf("Decoding value: %x, expected: %d, got: %d", tt.value, tt.volumeType, volumeType)
		}
		if nextOffset != tt.nextOffset {
			t.Errorf("Decoding value: %x, expected: %d, got: %d", tt.value, tt.nextOffset, nextOffset)
		}
		if usedSpace != tt.usedSpace {
			t.Errorf("Decoding value: %x, expected: %d, got: %d", tt.value, tt.usedSpace, usedSpace)
		}
		if state != tt.state {
			t.Errorf("Decoding value: %x, expected: %d, got: %d", tt.value, tt.state, state)
		}
	}
	// Test overflow
	m1 := []byte{0x80, 0x80, 0x80, 0x80}
	_, _, _, _, _, err := DecodeVolumeValue(m1)
	if err == nil {
		t.Errorf("We should fail to decode %x", m1)
	}
}

type objectKeyTest struct {
	key   []byte
	value []byte
}

func TestObjectKey(t *testing.T) {
	var objectKeyTests = []objectKeyTest{
		{[]byte("b80362143ac3221d15a75f4bd1af3fac1484213329.64315.data"),
			[]byte("\xb8\x03\x62\x14\x3a\xc3\x22\x1d\x15\xa7\x5f\x4b\xd1\xaf" +
				"\x3f\xac\x31\x34\x38\x34\x32\x31\x33\x33\x32\x39\x2e\x36\x34" +
				"\x33\x31\x35\x2e\x64\x61\x74\x61")},
		{[]byte("a2b98cd26a070c2e5200be1f950813d51494323929.64315.ts"),
			[]byte("\xa2\xb9\x8c\xd2\x6a\x07\x0c\x2e\x52\x00\xbe\x1f\x95\x08" +
				"\x13\xd5\x31\x34\x39\x34\x33\x32\x33\x39\x32\x39\x2e\x36\x34" +
				"\x33\x31\x35\x2e\x74\x73")},
	}

	for _, tt := range objectKeyTests {
		// Test encoding
		val, err := EncodeObjectKey(tt.key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(val, tt.value) {
			t.Errorf("For key: %x, got %x, expected %x", tt.key, val, tt.value)
		}

		// Test decoding
		key := make([]byte, 32+len(val[16:]))
		err = DecodeObjectKey(val, key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(key, tt.key) {
			t.Errorf("For value: %x, got %d, expected %d", val, key, tt.key)
		}

	}

	// Test encoding invalid object name, too short
	invalidName := []byte("tooshort")
	_, err := EncodeObjectKey(invalidName)
	if err == nil {
		t.Fatalf("should fail to encode object key: %x", invalidName)
	}

	// Test encoding invalid object name, bad char
	invalidName = []byte("badchar\xff6a070c2e5200be1f950813d51494323929.64315.ts")
	_, err = EncodeObjectKey(invalidName)
	if err == nil {
		t.Fatalf("should fail to encode: %x as object key", invalidName)
	}

	// Test decoding invalid data
	invalidData := []byte("tooshort")
	key := make([]byte, 12)
	err = DecodeObjectKey(invalidData, key)
	if err == nil {
		t.Fatalf("should fail to decode: %x as object key", invalidData)
	}

}

type objectValueTest struct {
	volumeIndex uint32
	offset        uint64
	value         []byte
}

func TestObjectValue(t *testing.T) {
	var objectValueTests = []objectValueTest{
		{0, 0, []byte("\x00\x00")},
		{1, 16384, []byte("\x01\x80\x80\x01")},
		{823762, 61 * 1024 * 1024 * 1024, []byte("\xd2\xa3\x32\x80\x80\x80\x80\xf4\x01")},
		{1<<32 - 1, 1<<64 - 1, []byte("\xff\xff\xff\xff\x0f\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01")},
	}

	for _, tt := range objectValueTests {
		// Test encoding
		val := EncodeObjectValue(tt.volumeIndex, tt.offset)
		if !bytes.Equal(val, tt.value) {
			t.Errorf("For volumeType: %d, offset: %d, got %x, expected %x", tt.volumeIndex, tt.offset, val, tt.value)
		}

		// Test decoding
		volumeIndex, offset, err := DecodeObjectValue(val)
		if err != nil {
			t.Fatal(err)
		}
		if volumeIndex != tt.volumeIndex {
			t.Errorf("Decoding value: %x, expected: %d, got: %d", tt.value, tt.volumeIndex, volumeIndex)
		}
		if offset != tt.offset {
			t.Errorf("Decoding value: %x, expected: %d, got: %d", tt.value, tt.offset, offset)
		}
	}

	// Test decoding invalid data
	invalidData := []byte("\xff")
	_, _, err := DecodeObjectValue(invalidData)
	if err == nil {
		t.Fatalf("should fail to decode: %x as object value", invalidData)
	}
}
