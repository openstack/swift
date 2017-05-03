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
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const reloadTime = 15 * time.Second

const (
	FirstPartitionID PartitionID = 0
)

type (
	PartitionID uint64
	DeviceID int

	AccountName string
	ContainerName string
	ObjectName string
)

type Ring interface {
	GetNodes(partition PartitionID) (response []*Device)
	GetNodesInOrder(partition PartitionID) (response []*Device)
	GetJobNodes(partition PartitionID, localDevice DeviceID) (response []*Device, handoff bool)
	GetPartition(account AccountName, container ContainerName, object ObjectName) PartitionID
	LocalDevices(localPort int) (devs []*Device, err error)
	AllDevices() (devs []Device)
	GetMoreNodes(partition PartitionID) MoreNodes
}

type MoreNodes interface {
	Next() *Device
}

type Device struct {
	Id              DeviceID     `json:"id"`
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

type ringData struct {
	Devs                                []Device `json:"devs"`
	ReplicaCount                        int      `json:"replica_count"`
	PartShift                           uint64   `json:"part_shift"`
	replica2part2devId                  [][]uint16
	regionCount, zoneCount, ipPortCount int
}

type hashRing struct {
	data   atomic.Value
	path   string
	prefix string
	suffix string
	mtime  time.Time
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
	used              map[DeviceID]bool
	sameRegions       map[int]bool
	sameZones         map[regionZone]bool
	sameIpPorts       map[ipPort]bool
	parts, start, inc int
	partition         PartitionID
}


func (p PartitionID) String() string {
	return strconv.FormatUint(uint64(p), 10)
}

func PartitionIDFromString(p string) (PartitionID, error) {
	partitioni, err := strconv.ParseUint(p, 10, 64)
	if err != nil {
		return PartitionID(0), err
	}
	return PartitionID(partitioni), nil
}

func (d *Device) String() string {
	return fmt.Sprintf("Device{Id: %d, Device: %s, Ip: %s, Port: %d}", d.Id, d.Device, d.Ip, d.Port)
}

func (r *hashRing) getData() *ringData {
	return r.data.Load().(*ringData)
}

func (r *hashRing) GetNodes(partition PartitionID) (response []*Device) {
	response = r.GetNodesInOrder(partition)
	for i := range response {
		j := rand.Intn(i + 1)
		response[i], response[j] = response[j], response[i]
	}
	return response
}

func (r *hashRing) GetNodesInOrder(partition PartitionID) (response []*Device) {
	d := r.getData()
	if uint64(partition) >= uint64(len(d.replica2part2devId[0])) {
		return nil
	}
	for i := 0; i < d.ReplicaCount; i++ {
		response = append(response, &d.Devs[d.replica2part2devId[i][partition]])
	}
	return response
}

func (r *hashRing) GetJobNodes(partition PartitionID, localDevice DeviceID) (response []*Device, handoff bool) {
	d := r.getData()
	handoff = true
	if uint64(partition) >= uint64(len(d.replica2part2devId[0])) {
		return nil, false
	}
	for i := 0; i < d.ReplicaCount; i++ {
		dev := &d.Devs[d.replica2part2devId[i][partition]]
		if dev.Id == localDevice {
			handoff = false
		} else {
			response = append(response, dev)
		}
	}
	return response, handoff
}

func (r *hashRing) GetPartition(account AccountName, container ContainerName, object ObjectName) PartitionID {
	d := r.getData()
	hash := md5.New()
	hash.Write([]byte(r.prefix + "/" + string(account)))
	if container != "" {
		hash.Write([]byte("/" + string(container)))
		if object != "" {
			hash.Write([]byte("/" + string(object)))
		}
	}
	hash.Write([]byte(r.suffix))
	digest := hash.Sum(nil)
	// treat as big endian unsigned int
	val := uint64(digest[0]) << 24 | uint64(digest[1]) << 16 | uint64(digest[2]) << 8 | uint64(digest[3])
	return PartitionID(val >> d.PartShift)
}

func (r *hashRing) LocalDevices(localPort int) (devs []*Device, err error) {
	d := r.getData()
	var localIPs = make(map[string]bool)

	localAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range localAddrs {
		localIPs[strings.Split(addr.String(), "/")[0]] = true
	}

	for i, dev := range d.Devs {
		if localIPs[dev.ReplicationIp] && dev.ReplicationPort == localPort {
			devs = append(devs, &d.Devs[i])
		}
	}
	return devs, nil
}

func (r *hashRing) AllDevices() (devs []Device) {
	d := r.getData()
	return d.Devs
}

func (r *hashRing) GetMoreNodes(partition PartitionID) MoreNodes {
	return &hashMoreNodes{r: r, partition: partition, used: nil}
}

func (r *hashRing) reload() error {
	fi, err := os.Stat(r.path)
	if err != nil {
		return err
	}
	if fi.ModTime() == r.mtime {
		return nil
	}
	fp, err := os.Open(r.path)
	if err != nil {
		return err
	}
	gz, err := gzip.NewReader(fp)
	if err != nil {
		return err
	}
	magicBuf := make([]byte, 4)
	io.ReadFull(gz, magicBuf)
	if string(magicBuf) != "R1NG" {
		return errors.New("Bad magic string")
	}
	var ringVersion uint16
	binary.Read(gz, binary.BigEndian, &ringVersion)
	if ringVersion != 1 {
		return fmt.Errorf("Unknown ring version %d", ringVersion)
	}
	var json_len uint32
	binary.Read(gz, binary.BigEndian, &json_len)
	jsonBuf := make([]byte, json_len)
	io.ReadFull(gz, jsonBuf)
	data := &ringData{}
	if err := json.Unmarshal(jsonBuf, data); err != nil {
		return err
	}
	partitionCount := 1 << (32 - data.PartShift)
	for i := 0; i < data.ReplicaCount; i++ {
		part2dev := make([]uint16, partitionCount)
		binary.Read(gz, binary.LittleEndian, &part2dev)
		data.replica2part2devId = append(data.replica2part2devId, part2dev)
	}
	regionCount := make(map[int]bool)
	zoneCount := make(map[regionZone]bool)
	ipPortCount := make(map[ipPort]bool)
	for _, d := range data.Devs {
		regionCount[d.Region] = true
		zoneCount[regionZone{d.Region, d.Zone}] = true
		ipPortCount[ipPort{d.Region, d.Zone, d.Port, d.Ip}] = true
	}
	data.regionCount = len(regionCount)
	data.zoneCount = len(zoneCount)
	data.ipPortCount = len(ipPortCount)
	r.mtime = fi.ModTime()
	r.data.Store(data)
	return nil
}

func (r *hashRing) reloader() error {
	for {
		time.Sleep(reloadTime)
		r.reload()
	}
}

func (m *hashMoreNodes) addDevice(d *Device) {
	m.used[d.Id] = true
	m.sameRegions[d.Region] = true
	m.sameZones[regionZone{d.Region, d.Zone}] = true
	m.sameIpPorts[ipPort{d.Region, d.Zone, d.Port, d.Ip}] = true
}

func (m *hashMoreNodes) initialize() {
	d := m.r.getData()
	m.parts = len(d.replica2part2devId[0])
	m.used = make(map[DeviceID]bool)
	m.sameRegions = make(map[int]bool)
	m.sameZones = make(map[regionZone]bool)
	m.sameIpPorts = make(map[ipPort]bool)
	for _, mp := range d.replica2part2devId {
		m.addDevice(&d.Devs[mp[m.partition]])
	}
	hash := md5.New()
	hash.Write([]byte(string(m.partition)))
	digest := hash.Sum(nil)
	m.start = int((uint64(digest[0]) << 24 | uint64(digest[1]) << 16 | uint64(digest[2]) << 8 | uint64(digest[3])) >> d.PartShift)
	m.inc = m.parts / 65536
	if m.inc == 0 {
		m.inc = 1
	}
}

func (m *hashMoreNodes) Next() *Device {
	d := m.r.getData()
	if m.used == nil {
		m.initialize()
	}
	var check func(d *Device) bool
	if len(m.sameRegions) < d.regionCount {
		check = func(d *Device) bool {
			return !m.sameRegions[d.Region]
		}
	} else if len(m.sameZones) < d.zoneCount {
		check = func(d *Device) bool {
			return !m.sameZones[regionZone{d.Region, d.Zone}]
		}
	} else if len(m.sameIpPorts) < d.ipPortCount {
		check = func(d *Device) bool {
			return !m.sameIpPorts[ipPort{d.Region, d.Zone, d.Port, d.Ip}]
		}
	} else {
		check = func(d *Device) bool {
			return !m.used[d.Id]
		}
	}
	for i := 0; i < m.parts; i += m.inc {
		handoffPart := (i + m.start) % m.parts
		for _, part2devId := range d.replica2part2devId {
			if handoffPart < len(part2devId) {
				if check(&d.Devs[part2devId[handoffPart]]) {
					m.addDevice(&d.Devs[part2devId[handoffPart]])
					return &d.Devs[part2devId[handoffPart]]
				}
			}
		}
	}
	return nil
}

func LoadRing(path string, prefix string, suffix string) (Ring, error) {
	ring := &hashRing{prefix: prefix, suffix: suffix, path: path, mtime: time.Unix(0, 0)}
	if err := ring.reload(); err == nil {
		go ring.reloader()
		return ring, nil
	} else {
		return nil, err
	}
}

// GetRing returns the current ring given the ring_type ("account", "container", "object"),
// hash path prefix, and hash path suffix. An error is raised if the requested ring does
// not exist.
func GetRing(ringType, prefix, suffix string, policy int) (Ring, error) {
	var ring Ring
	var err error
	ringFile := fmt.Sprintf("%s.ring.gz", ringType)
	if policy != 0 {
		ringFile = fmt.Sprintf("%s-%d.ring.gz", ringType, policy)
	}
	if ring, err = LoadRing(fmt.Sprintf("/etc/hummingbird/%s", ringFile), prefix, suffix); err != nil {
		if ring, err = LoadRing(fmt.Sprintf("/etc/swift/%s", ringFile), prefix, suffix); err != nil {
			return nil, fmt.Errorf("Error loading %s:%d ring: %s", ringType, policy, err)
		}
	}
	return ring, nil
}
