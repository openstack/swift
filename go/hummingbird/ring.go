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
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

type Ring interface {
	GetNodes(partition uint64) (response []*Device)
	GetJobNodes(partition uint64, localDevice int) (response []*Device, handoff bool)
	GetPartition(account string, container string, object string) uint64
	LocalDevices(localPort int) (devs []*Device, err error)
	GetMoreNodes(partition uint64) MoreNodes
}

type MoreNodes interface {
	Next() *Device
}

type Device struct {
	Id              int     `json:"id"`
	Device          string  `json:"device"`
	Ip              string  `json:"ip"`
	Meta            string  `json:"meta"`
	Port            int     `json:"port"`
	Region          int     `json:"region"`
	ReplicationIp   string  `json:"replication_ip"`
	ReplicationPort int     `json:"replication_port"`
	Weight          float64 `json:"weight"`
	Zone            int     `json:"zone"`
}

type hashRing struct {
	Devs                                []Device `json:"devs"`
	ReplicaCount                        int      `json:"replica_count"`
	PartShift                           uint64   `json:"part_shift"`
	replica2part2devId                  [][]uint16
	prefix                              string
	suffix                              string
	regionCount, zoneCount, ipPortCount int
}

type regionZone struct {
	region, zone int
}

type ipPort struct {
	region, zone, port int
	ip                 string
}

type hashMoreNodes struct {
	r                 *hashRing
	used, sameRegions map[int]bool
	sameZones         map[regionZone]bool
	sameIpPorts       map[ipPort]bool
	parts, start, inc int
	partition         uint64
}

func (d *Device) String() string {
	return fmt.Sprintf("Device{Id: %d, Device: %s, Ip: %s, Port: %d}", d.Id, d.Device, d.Ip, d.Port)
}

func (r *hashRing) GetNodes(partition uint64) (response []*Device) {
	if partition >= uint64(len(r.replica2part2devId[0])) {
		return nil
	}
	for i := 0; i < r.ReplicaCount; i++ {
		response = append(response, &r.Devs[r.replica2part2devId[i][partition]])
	}
	return response
}

func (r *hashRing) GetJobNodes(partition uint64, localDevice int) (response []*Device, handoff bool) {
	handoff = true
	if partition >= uint64(len(r.replica2part2devId[0])) {
		return nil, false
	}
	for i := 0; i < r.ReplicaCount; i++ {
		dev := &r.Devs[r.replica2part2devId[i][partition]]
		if dev.Id == localDevice {
			handoff = false
		} else {
			response = append(response, dev)
		}
	}
	return response, handoff
}

func (r *hashRing) GetPartition(account string, container string, object string) uint64 {
	hash := md5.New()
	hash.Write([]byte(r.prefix + "/" + account + "/"))
	if container != "" {
		hash.Write([]byte(container + "/"))
		if object != "" {
			hash.Write([]byte(object + "/"))
		}
	}
	// treat as big endian unsigned int
	hash.Write([]byte(r.suffix))
	digest := hash.Sum(nil)
	val := uint64(digest[0])<<24 | uint64(digest[1])<<16 | uint64(digest[2])<<8 | uint64(digest[3])
	return val >> r.PartShift
}

func (r *hashRing) LocalDevices(localPort int) (devs []*Device, err error) {
	var localIPs = make(map[string]bool)

	localAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range localAddrs {
		localIPs[strings.Split(addr.String(), "/")[0]] = true
	}

	for i, dev := range r.Devs {
		if localIPs[dev.ReplicationIp] && dev.ReplicationPort == localPort {
			devs = append(devs, &r.Devs[i])
		}
	}
	return devs, nil
}

func (r *hashRing) GetMoreNodes(partition uint64) MoreNodes {
	return &hashMoreNodes{r: r, partition: partition, used: nil}
}

func (m *hashMoreNodes) addDevice(d *Device) {
	m.used[d.Id] = true
	m.sameRegions[d.Region] = true
	m.sameZones[regionZone{d.Region, d.Zone}] = true
	m.sameIpPorts[ipPort{d.Region, d.Zone, d.Port, d.Ip}] = true
}

func (m *hashMoreNodes) initialize() {
	m.parts = len(m.r.replica2part2devId[0])
	m.used = make(map[int]bool)
	m.sameRegions = make(map[int]bool)
	m.sameZones = make(map[regionZone]bool)
	m.sameIpPorts = make(map[ipPort]bool)
	for _, mp := range m.r.replica2part2devId {
		m.addDevice(&m.r.Devs[mp[m.partition]])
	}
	hash := md5.New()
	hash.Write([]byte(strconv.FormatUint(m.partition, 10)))
	digest := hash.Sum(nil)
	m.start = int((uint64(digest[0])<<24 | uint64(digest[1])<<16 | uint64(digest[2])<<8 | uint64(digest[3])) >> m.r.PartShift)
	m.inc = m.parts / 65536
	if m.inc == 0 {
		m.inc = 1
	}
}

func (m *hashMoreNodes) Next() *Device {
	if m.used == nil {
		m.initialize()
	}
	var check func(d *Device) bool
	if len(m.sameRegions) < m.r.regionCount {
		check = func(d *Device) bool { return !m.sameRegions[d.Region] }
	} else if len(m.sameZones) < m.r.zoneCount {
		check = func(d *Device) bool { return !m.sameZones[regionZone{d.Region, d.Zone}] }
	} else if len(m.sameIpPorts) < m.r.ipPortCount {
		check = func(d *Device) bool { return !m.sameIpPorts[ipPort{d.Region, d.Zone, d.Port, d.Ip}] }
	} else {
		check = func(d *Device) bool { return !m.used[d.Id] }
	}
	for i := 0; i < m.parts; i += m.inc {
		handoffPart := (i + m.start) % m.parts
		for _, part2devId := range m.r.replica2part2devId {
			if handoffPart < len(part2devId) {
				if check(&m.r.Devs[part2devId[handoffPart]]) {
					m.addDevice(&m.r.Devs[part2devId[handoffPart]])
					return &m.r.Devs[part2devId[handoffPart]]
				}
			}
		}
	}
	return nil
}

func LoadRing(path string, prefix string, suffix string) (Ring, error) {
	fp, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			errMsg := fmt.Sprintf("File at %s doesn't exists", path)
			return nil, errors.New(errMsg)
		} else {
			return nil, err
		}
	}
	gz, err := gzip.NewReader(fp)
	if err != nil {
		return nil, err
	}
	magicBuf := make([]byte, 4)
	io.ReadFull(gz, magicBuf)
	if string(magicBuf) != "R1NG" {
		return nil, errors.New("Bad magic string")
	}
	var ringVersion uint16
	binary.Read(gz, binary.BigEndian, &ringVersion)
	if ringVersion != 1 {
		return nil, errors.New(fmt.Sprintf("Unknown ring version %d", ringVersion))
	}
	// TODO: assert ringVersion == 1
	var json_len uint32
	binary.Read(gz, binary.BigEndian, &json_len)
	jsonBuf := make([]byte, json_len)
	io.ReadFull(gz, jsonBuf)
	var ring hashRing
	json.Unmarshal(jsonBuf, &ring)
	ring.prefix = prefix
	ring.suffix = suffix
	partitionCount := 1 << (32 - ring.PartShift)
	for i := 0; i < ring.ReplicaCount; i++ {
		part2dev := make([]uint16, partitionCount)
		binary.Read(gz, binary.LittleEndian, &part2dev)
		ring.replica2part2devId = append(ring.replica2part2devId, part2dev)
	}
	regionCount := make(map[int]bool)
	zoneCount := make(map[regionZone]bool)
	ipPortCount := make(map[ipPort]bool)
	for _, d := range ring.Devs {
		regionCount[d.Region] = true
		zoneCount[regionZone{d.Region, d.Zone}] = true
		ipPortCount[ipPort{d.Region, d.Zone, d.Port, d.Ip}] = true
	}
	ring.regionCount = len(regionCount)
	ring.zoneCount = len(zoneCount)
	ring.ipPortCount = len(ipPortCount)
	return &ring, nil
}

// GetRing returns the current ring given the ring_type ("account", "container", "object"),
// hash path prefix, and hash path suffix. An error is raised if the requested ring does
// not exist.
func GetRing(ring_type, prefix, suffix string) (Ring, error) {
	var ring Ring
	var err error
	if ring, err = LoadRing(fmt.Sprintf("/etc/hummingbird/%s.ring.gz", ring_type), prefix, suffix); err != nil {
		if ring, err = LoadRing(fmt.Sprintf("/etc/swift/%s.ring.gz", ring_type), prefix, suffix); err != nil {
			return nil, errors.New(fmt.Sprintf("Error loading %s ring", ring_type))
		}
	}
	return ring, nil
}
