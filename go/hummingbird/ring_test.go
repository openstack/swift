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
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeARing writes a basic ring with the given attributes
func writeARing(w io.Writer, deviceCount int, replicaCount int, partShift uint) error {
	gzw := gzip.NewWriter(w)
	devs := []Device{}
	for i := 0; i < deviceCount; i++ {
		ip := fmt.Sprintf("127.0.0.%d", i)
		devs = append(devs, Device{Id: i, Device: "sda", Ip: ip, Meta: "", Port: 1234, Region: 0, ReplicationIp: ip, ReplicationPort: 1234, Weight: 1, Zone: 0})
	}
	ringData := map[string]interface{}{
		"devs":          devs,
		"replica_count": replicaCount,
		"part_shift":    partShift,
	}
	data, err := json.Marshal(ringData)
	if err != nil {
		return err
	}
	gzw.Write([]byte{'R', '1', 'N', 'G'})
	binary.Write(gzw, binary.BigEndian, uint16(1))
	binary.Write(gzw, binary.BigEndian, uint32(len(data)))
	gzw.Write(data)

	partitionCount := 1 << (32 - partShift)

	for i := 0; i < replicaCount; i++ {
		part2dev := make([]uint16, partitionCount)
		for j := 0; j < partitionCount; j++ {
			part2dev[j] = uint16((j + i) % len(devs))
		}
		binary.Write(gzw, binary.LittleEndian, part2dev)
	}
	gzw.Flush()

	return nil
}

func TestLoadRing(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer fp.Close()
	defer os.RemoveAll(fp.Name())
	require.Nil(t, writeARing(fp, 4, 2, 29))
	r, err := LoadRing(fp.Name(), "prefix", "suffix")
	require.Nil(t, err)
	ring, ok := r.(*hashRing)
	require.True(t, ok)
	require.NotNil(t, ring)
	require.Equal(t, 4, len(ring.getData().Devs))
	require.Equal(t, 2, ring.getData().ReplicaCount)
	require.Equal(t, uint64(29), ring.getData().PartShift)
}

func TestGetNodes(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer fp.Close()
	defer os.RemoveAll(fp.Name())
	require.Nil(t, writeARing(fp, 4, 2, 29))
	r, err := LoadRing(fp.Name(), "prefix", "suffix")
	require.Nil(t, err)
	ring, ok := r.(*hashRing)
	require.True(t, ok)
	require.NotNil(t, ring)
	nodes := ring.GetNodes(0)
	require.Equal(t, 2, len(nodes))
	// some of these values may not be obvious, but they shouldn't change
	require.Equal(t, 0, nodes[0].Id)
	require.Equal(t, 1, nodes[1].Id)
}

func TestGetJobNodes(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer fp.Close()
	defer os.RemoveAll(fp.Name())
	require.Nil(t, writeARing(fp, 4, 2, 29))
	r, err := LoadRing(fp.Name(), "prefix", "suffix")
	require.Nil(t, err)
	ring, ok := r.(*hashRing)
	require.True(t, ok)
	require.NotNil(t, ring)
	nodes, handoff := ring.GetJobNodes(0, 0)
	require.Equal(t, 1, len(nodes))
	require.Equal(t, 1, nodes[0].Id)
	require.False(t, handoff)
	nodes, handoff = ring.GetJobNodes(0, 2)
	require.Equal(t, 2, len(nodes))
	require.True(t, handoff)
}

func TestRingReload(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	defer fp.Close()
	defer os.RemoveAll(fp.Name())
	require.Nil(t, writeARing(fp, 4, 2, 29))
	r, err := LoadRing(fp.Name(), "prefix", "suffix")
	require.Nil(t, err)
	ring, ok := r.(*hashRing)
	require.True(t, ok)
	require.NotNil(t, ring)
	fp.Seek(0, os.SEEK_SET)
	fp.Truncate(0)
	require.Nil(t, writeARing(fp, 5, 3, 30))
	// make sure the mtime has changed
	os.Chtimes(fp.Name(), time.Now(), time.Now().Add(time.Second))
	ring.reload()
	require.Equal(t, 5, len(ring.getData().Devs))
	require.Equal(t, 3, ring.getData().ReplicaCount)
	require.Equal(t, uint64(30), ring.getData().PartShift)
}
