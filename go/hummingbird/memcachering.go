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
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	jsonFlag    = 2
	opGet       = byte(0x00)
	opSet       = byte(0x01)
	opDelete    = byte(0x04)
	opIncrement = byte(0x05)
	opDecrement = byte(0x06)
)

type MemcacheRing interface {
	Decr(key string, delta int, timeout int) (int64, error)
	Delete(key string) error
	Get(key string) (interface{}, error)
	GetStructured(key string, val interface{}) error
	GetMulti(serverKey string, keys []string) (map[string]interface{}, error)
	Incr(key string, delta int, timeout int) (int64, error)
	Set(key string, value interface{}, timeout int) error
	SetMulti(serverKey string, values map[string]interface{}, timeout int) error
}

type memcacheRing struct {
	ring                        map[string]string
	serverKeys                  []string
	servers                     map[string]*server
	connTimeout                 int64
	responseTimeout             int64
	maxFreeConnectionsPerServer int64
	tries                       int64
	nodeWeight                  int64
}

func NewMemcacheRing(conf string) (*memcacheRing, error) {
	iniFile, err := LoadIniFile(conf)
	if err != nil {
		return nil, fmt.Errorf("Unable to load conf file: %s", conf)
	}
	return NewMemcacheRingFromIniFile(iniFile)
}

func NewMemcacheRingFromIniFile(iniFile IniFile) (*memcacheRing, error) {
	ring := &memcacheRing{}
	ring.ring = make(map[string]string)
	ring.serverKeys = make([]string, 0)
	ring.servers = make(map[string]*server)

	ring.maxFreeConnectionsPerServer = iniFile.GetInt("memcache", "max_free_connections_per_server", 100)
	ring.connTimeout = iniFile.GetInt("memcache", "conn_timeout", 100)
	ring.responseTimeout = iniFile.GetInt("memcache", "response_timeout", 100)
	ring.nodeWeight = iniFile.GetInt("memcache", "node_weight", 50)
	ring.tries = iniFile.GetInt("memcache", "tries", 5)
	for _, s := range strings.Split(iniFile.GetDefault("memcache", "memcache_servers", ""), ",") {
		err := ring.addServer(s)
		if err != nil {
			return nil, err
		}
	}
	if len(ring.servers) == 0 {
		ring.addServer("127.0.0.1:11211")
	}
	ring.sortServerKeys()
	if int64(len(ring.servers)) < ring.tries {
		ring.tries = int64(len(ring.servers))
	}
	return ring, nil
}

func hashKey(s string) string {
	h := md5.New()
	io.WriteString(h, s)
	return hex.EncodeToString(h.Sum(nil))
}

func hashKeyToBytes(s string) []byte {
	h := md5.New()
	io.WriteString(h, s)
	buf := make([]byte, 32)
	hex.Encode(buf, h.Sum(nil))
	return buf
}

func (ring *memcacheRing) addServer(serverString string) error {
	server, err := newServer(serverString, ring.connTimeout, ring.responseTimeout, ring.maxFreeConnectionsPerServer)
	if err != nil {
		return err
	}
	ring.servers[serverString] = server
	for i := 0; int64(i) < ring.nodeWeight; i++ {
		ring.ring[hashKey(fmt.Sprintf("%s-%d", serverString, i))] = serverString
	}
	return nil
}

func (ring *memcacheRing) sortServerKeys() {
	ring.serverKeys = make([]string, 0)
	for k := range ring.ring {
		ring.serverKeys = append(ring.serverKeys, k)
	}
	sort.Strings(ring.serverKeys)
}

func (ring *memcacheRing) Decr(key string, delta int, timeout int) (int64, error) {
	return ring.Incr(key, -delta, timeout)
}

func (ring *memcacheRing) Delete(key string) error {
	fn := func(conn *connection) error {
		_, _, err := conn.roundTripPacket(opDelete, hashKeyToBytes(key), nil, nil)
		if err != nil && err != CacheMiss {
			return err
		}
		return nil
	}
	return ring.loop(key, fn)
}

func (ring *memcacheRing) GetStructured(key string, val interface{}) error {
	fn := func(conn *connection) error {
		value, extras, err := conn.roundTripPacket(opGet, hashKeyToBytes(key), nil, nil)
		if err != nil {
			return err
		}
		flags := binary.BigEndian.Uint32(extras[0:4])
		if flags&jsonFlag == 0 {
			return errors.New("Not json data")
		}
		if err := json.Unmarshal(value, val); err != nil {
			return err
		}
		return nil
	}
	return ring.loop(key, fn)
}

func (ring *memcacheRing) Get(key string) (interface{}, error) {
	type Return struct {
		value interface{}
	}
	ret := &Return{}
	fn := func(conn *connection) error {
		value, extras, err := conn.roundTripPacket(opGet, hashKeyToBytes(key), nil, nil)
		if err != nil {
			return err
		}
		flags := binary.BigEndian.Uint32(extras[0:4])
		if flags&jsonFlag != 0 {
			if err := json.Unmarshal(value, &ret.value); err != nil {
				return err
			}
		} else {
			ret.value = value
		}
		return nil
	}
	return ret.value, ring.loop(key, fn)
}

func (ring *memcacheRing) GetMulti(serverKey string, keys []string) (map[string]interface{}, error) {
	type Return struct {
		value map[string]interface{}
	}
	ret := &Return{}
	fn := func(conn *connection) error {
		ret.value = make(map[string]interface{})
		for _, key := range keys {
			if err := conn.sendPacket(opGet, hashKeyToBytes(key), nil, nil); err != nil {
				return err
			}
		}
		for _, key := range keys {
			value, extras, err := conn.receivePacket()
			if err != nil {
				if err == CacheMiss {
					continue
				}
				return err
			}
			flags := binary.BigEndian.Uint32(extras[0:4])
			if flags&jsonFlag != 0 {
				var v interface{}
				if err := json.Unmarshal(value, &v); err != nil {
					return err
				}
				ret.value[key] = v
			} else {
				ret.value[key] = value
			}
		}
		return nil
	}
	return ret.value, ring.loop(serverKey, fn)
}

func (ring *memcacheRing) Incr(key string, delta int, timeout int) (int64, error) {
	type Return struct {
		value int64
	}
	ret := &Return{}
	op := opIncrement
	dfl := delta
	if delta < 0 {
		op = opDecrement
		delta = 0 - delta
		dfl = 0
	}
	extras := make([]byte, 20)
	binary.BigEndian.PutUint64(extras[0:8], uint64(delta))
	binary.BigEndian.PutUint64(extras[8:16], uint64(dfl))
	binary.BigEndian.PutUint32(extras[16:20], uint32(timeout))
	fn := func(conn *connection) error {
		value, _, err := conn.roundTripPacket(op, hashKeyToBytes(key), nil, extras)
		if err != nil {
			return err
		}
		ret.value = int64(binary.BigEndian.Uint64(value[0:8]))
		return nil
	}
	return ret.value, ring.loop(key, fn)
}

func (ring *memcacheRing) Set(key string, value interface{}, timeout int) error {
	serl, err := json.Marshal(value)
	if err != nil {
		return err
	}
	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras[0:4], uint32(jsonFlag))
	binary.BigEndian.PutUint32(extras[4:8], uint32(timeout))
	fn := func(conn *connection) error {
		_, _, err = conn.roundTripPacket(opSet, hashKeyToBytes(key), serl, extras)
		return err
	}
	return ring.loop(key, fn)
}

func (ring *memcacheRing) SetMulti(serverKey string, values map[string]interface{}, timeout int) error {
	fn := func(conn *connection) error {
		for key, value := range values {
			serl, err := json.Marshal(value)
			if err != nil {
				return err
			}
			extras := make([]byte, 8)
			binary.BigEndian.PutUint32(extras[0:4], uint32(jsonFlag))
			binary.BigEndian.PutUint32(extras[4:8], uint32(timeout))
			if err = conn.sendPacket(opSet, hashKeyToBytes(key), serl, extras); err != nil {
				return err
			}
		}
		for _ = range values {
			if _, _, err := conn.receivePacket(); err != nil {
				return err
			}
		}
		return nil
	}
	return ring.loop(serverKey, fn)
}

type serverIterator struct {
	ring    *memcacheRing
	key     string
	current int
	servers []string
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (ring *memcacheRing) newServerIterator(key string) *serverIterator {
	return &serverIterator{ring, hashKey(key), -1, make([]string, 0)}
}

func (it *serverIterator) next() bool {
	return int64(len(it.servers)) < it.ring.tries
}

func (it *serverIterator) value() *server {
	if int64(len(it.servers)) > it.ring.tries {
		panic("serverIterator.Value() called when there are no more tries left")
	}
	if it.current == -1 {
		it.current = sort.SearchStrings(it.ring.serverKeys, it.key) % len(it.ring.serverKeys)
	} else {
		for stringInSlice(it.ring.ring[it.ring.serverKeys[it.current]], it.servers) {
			it.current = (it.current + 1) % len(it.ring.serverKeys)
		}
	}
	serverString := it.ring.ring[it.ring.serverKeys[it.current]]
	it.servers = append(it.servers, serverString)
	return it.ring.servers[serverString]
}

func (ring *memcacheRing) loop(key string, fn func(*connection) error) error {
	it := ring.newServerIterator(key)
	for it.next() {
		server := it.value()
		conn, err := server.getConnection()
		if err != nil {
			continue
		}
		err = fn(conn)
		server.releaseConnection(conn, err)
		if err == nil {
			return nil
		} else if err == CacheMiss {
			return err
		}
	}
	return fmt.Errorf("Operation failed")
}

type server struct {
	serverString       string
	addr               net.Addr
	lock               sync.Mutex
	connTimeout        time.Duration
	requestTimeout     time.Duration
	maxFreeConnections int64
	connections        []*connection
}

func newServer(serverString string, connTimeout int64, requestTimeout int64, maxFreeConnections int64) (*server, error) {
	var addr net.Addr
	var err error
	s := server{serverString: serverString}
	if strings.Contains(serverString, "/") {
		addr, err = net.ResolveUnixAddr("unix", serverString)
		if err != nil {
			return nil, err
		}
	} else {
		if !strings.Contains(serverString, ":") {
			serverString = fmt.Sprintf("%s:11211", serverString)
		}
		addr, err = net.ResolveTCPAddr("tcp", serverString)
		if err != nil {
			return nil, err
		}
	}
	s.addr = addr
	s.connTimeout = time.Duration(connTimeout) * time.Millisecond
	s.requestTimeout = time.Duration(requestTimeout) * time.Millisecond
	s.maxFreeConnections = maxFreeConnections
	s.connections = make([]*connection, 0)
	return &s, nil
}

func (s *server) connectionCount() uint64 {
	return uint64(len(s.connections))
}

func (s *server) getExistingConnection() *connection {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.connections) == 0 {
		return nil
	}
	conn := s.connections[len(s.connections)-1]
	s.connections = s.connections[:len(s.connections)-1]
	return conn
}

func (s *server) getConnection() (*connection, error) {
	conn := s.getExistingConnection()
	if conn != nil {
		return conn, nil
	}
	return newConnection(s.serverString, s.connTimeout, s.requestTimeout)
}

func (s *server) releaseConnection(conn *connection, err error) {
	if err == nil || err != Disconnected {
		s.lock.Lock()
		defer s.lock.Unlock()
		if int64(len(s.connections)) < s.maxFreeConnections {
			s.connections = append(s.connections, conn)
			return
		}
	}
	conn.close()
}

var (
	Disconnected = fmt.Errorf("Disconnected from memcache server")
	CacheMiss    = fmt.Errorf("Server cache miss")
)

type connection struct {
	conn       net.Conn
	rw         *bufio.ReadWriter
	reqTimeout time.Duration
	packetBuf  []byte
}

func newConnection(address string, connTimeout time.Duration, requestTimeout time.Duration) (*connection, error) {
	domain := "tcp"
	if strings.Contains(address, "/") {
		domain = "unix"
	} else if !strings.Contains(address, ":") {
		address = fmt.Sprintf("%s:%d", address, 11211)
	}
	conn, err := net.DialTimeout(domain, address, connTimeout)
	if err != nil {
		return nil, Disconnected
	}
	if c, ok := conn.(*net.TCPConn); ok {
		c.SetNoDelay(true)
	}
	return &connection{
		conn:       conn,
		rw:         bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		reqTimeout: requestTimeout,
		packetBuf:  make([]byte, 256),
	}, nil
}

func (c *connection) close() error {
	return c.conn.Close()
}

func (c *connection) receivePacket() ([]byte, []byte, error) {
	packet := c.packetBuf[0:24]
	if bytes, err := c.rw.Read(packet); err != nil || bytes != 24 {
		c.close()
		return nil, nil, Disconnected
	}
	keyLen := binary.BigEndian.Uint16(packet[2:4])
	extrasLen := packet[4]
	status := binary.BigEndian.Uint16(packet[6:8])
	bodyLen := int(binary.BigEndian.Uint32(packet[8:12]))
	for cap(c.packetBuf) < bodyLen {
		c.packetBuf = append(c.packetBuf, ' ')
	}
	body := c.packetBuf[0:bodyLen]
	if bytes, err := c.rw.Read(body); err != nil || bytes != bodyLen {
		c.close()
		return nil, nil, Disconnected
	}
	switch status {
	case 0:
		return body[int(keyLen)+int(extrasLen) : int(bodyLen)], body[int(keyLen):int(extrasLen)], nil
	case 1:
		return nil, nil, CacheMiss
	default:
		return nil, nil, fmt.Errorf("Error response from memcache: %d", status)
	}
}

func (c *connection) roundTripPacket(opcode byte, key []byte, value []byte, extras []byte) ([]byte, []byte, error) {
	if err := c.sendPacket(opcode, key, value, extras); err != nil {
		return nil, nil, err
	}
	return c.receivePacket()
}

func (c *connection) sendPacket(opcode byte, key []byte, value []byte, extras []byte) error {
	c.conn.SetDeadline(time.Now().Add(c.reqTimeout))
	packet := c.packetBuf[0:24]
	packet[0], packet[1], packet[4], packet[5] = 0x80, opcode, byte(len(extras)), 0
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(key)))
	packet[6], packet[7] = 0, 0
	binary.BigEndian.PutUint32(packet[8:12], uint32(len(key)+len(value)+len(extras)))
	packet[12], packet[13], packet[14], packet[15] = 0, 0, 0, 0
	packet[16], packet[17], packet[18], packet[19], packet[20], packet[21], packet[22], packet[23] = 0, 0, 0, 0, 0, 0, 0, 0
	packet = append(append(append(packet, extras...), key...), value...)
	if _, err := c.rw.Write(packet); err != nil || c.rw.Flush() != nil {
		c.close()
		return Disconnected
	}
	return nil
}
