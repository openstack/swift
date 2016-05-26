package hummingbird

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	ini "github.com/vaughan0/go-ini"
)

func TestLocalHost(t *testing.T) {
	// construct ini and check for running memcache servers
	var buffer bytes.Buffer
	iniFile := Config{File: make(ini.File)}
	for i := 0; i < 4; i++ {
		server := fmt.Sprintf("localhost:%d", 10000+i)
		if i > 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(server)
		if i < 3 {
			c, err := net.Dial("tcp", server)
			if err != nil {
				t.Skipf("skipping test; no server running at %s", server)
				return
			}
			c.Write([]byte("flush_all\r\n"))
			c.Close()
		}
	}
	section := iniFile.Section("memcache")
	section["dial_timeout"] = "1000"
	section["max_free_connections_per_server"] = "3"
	section["memcache_servers"] = buffer.String()
	ring, err := NewMemcacheRingFromConfig(iniFile)
	if err != nil {
		t.Fatal("Unable to get memcache ring")
	}
	testWithRing(t, ring)
}

func TestUnixSocket(t *testing.T) {
	// construct ini and start memcache servers
	var buffer bytes.Buffer
	iniFile := Config{File: make(ini.File)}
	for i := 0; i < 4; i++ {
		sock := fmt.Sprintf("/tmp/test-memecachering-%d", i)
		if err := os.Remove(sock); err != nil {
			if !os.IsNotExist(err) {
				t.Fatalf("")
			}
		}
		if i > 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(sock)
		// don't start a memcache server for last sock
		if i < 3 {
			// start memcache server
			cmd := exec.Command("memcached", "-u", "memcache", "-s", sock)
			if err := cmd.Start(); err != nil {
				t.Fatal("Unable to run memcached")
				return
			}
			defer cmd.Wait()
			defer cmd.Process.Kill()
			// Wait a bit for the socket to appear.
			for i := 1; i < 10; i++ {
				if _, err := os.Stat(sock); err == nil {
					break
				}
				time.Sleep(time.Duration(25*i) * time.Millisecond)
			}
		}
	}
	time.Sleep(time.Duration(25) * time.Millisecond)
	section := iniFile.Section("memcache")
	section["dial_timeout"] = "1000"
	section["max_free_connections_per_server"] = "3"
	section["memcache_servers"] = buffer.String()
	ring, err := NewMemcacheRingFromConfig(iniFile)
	if err != nil {
		t.Fatal("Unable to get memcache ring")
	}
	testWithRing(t, ring)
}

func testWithRing(t *testing.T, ring *memcacheRing) {
	testSetGetDelete(t, ring)
	testGetCacheMiss(t, ring)
	testIncr(t, ring)
	testDecr(t, ring)
	testSetGetMulti(t, ring)
	testManySetGets(t, ring)
	testConnectionLimits(t, ring)
	testExpiration(t, ring)
}

func testSetGetDelete(t *testing.T, ring *memcacheRing) {
	key := "testJsonSetGet"
	setValue := "some_value"
	if err := ring.Set(key, setValue, 0); err != nil {
		t.Error(err)
		return
	}
	if getValue, err := ring.Get(key); err != nil {
		t.Error(err)
		return
	} else {
		assert.EqualValues(t, setValue, getValue)
	}
	if err := ring.Delete(key); err != nil {
		t.Error(err)
		return
	}
	if _, err := ring.Get(key); err != nil {
		assert.EqualValues(t, CacheMiss, err)
	} else {
		t.Errorf("Expected a cache miss")
	}
}

func testGetCacheMiss(t *testing.T, ring *memcacheRing) {
	// make a call to an unset cache value and check for cache miss return
	key := "testGetCacheMiss"
	if _, err := ring.Get(key); err != nil {
		assert.EqualValues(t, CacheMiss, err)
	} else {
		t.Errorf("Expected a cache miss")
	}
}

func testIncr(t *testing.T, ring *memcacheRing) {
	// increment multiple times and check running total
	key := "testIncr"
	var running_total int64 = 0
	for i := 1; i <= 10; i++ {
		running_total += int64(i)
		if j, err := ring.Incr(key, i, 2); err != nil {
			t.Error(err)
			return
		} else {
			assert.EqualValues(t, running_total, j)
			if value, err := ring.Incr(key, 0, 2); err != nil {
				t.Error(err)
				return
			} else {
				assert.EqualValues(t, running_total, value)
			}
		}
	}
}

func testDecr(t *testing.T, ring *memcacheRing) {
	key := "testDecr"
	// test decrement on unset value sets value to 0
	if value, err := ring.Decr(key, 1, 2); err != nil {
		t.Error(err)
		return
	} else {
		assert.EqualValues(t, int64(0), value)
	}
	// test running total goes to and stays at zero
	var running_total int = 30
	if value, err := ring.Incr(key, running_total, 2); err != nil {
		t.Error(err)
		return
	} else {
		assert.EqualValues(t, running_total, value)
	}
	for i := 1; i <= 10; i++ {
		if i < running_total {
			running_total -= i
		} else {
			running_total = 0
		}
		if j, err := ring.Decr(key, i, 2); err != nil {
			t.Error(err)
			return
		} else {
			assert.EqualValues(t, running_total, j)
			if value, err := ring.Incr(key, 0, 2); err != nil {
				t.Error(err)
				return
			} else {
				assert.EqualValues(t, running_total, value)
			}
		}
	}
}

func testSetGetMulti(t *testing.T, ring *memcacheRing) {
	key := "testSetGetMulti"
	// set three values at once
	setValues := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	if err := ring.SetMulti(key, setValues, 0); err != nil {
		t.Error(err)
		return
	}
	// get the three values
	getValues, err := ring.GetMulti(key, []string{"key1", "key2", "key3"})
	if err != nil {
		t.Error(err)
		return
	}
	// verify the values match what was set
	found := []string{}
	for getKey, getValue := range getValues {
		for setKey, setValue := range setValues {
			if getKey == setKey {
				assert.EqualValues(t, setValue, getValue)
				found = append(found, getKey)
			}
		}
	}
	assert.EqualValues(t, len(setValues), len(found))
}

func testManySetGets(t *testing.T, ring *memcacheRing) {
	key := "testManySetGets"
	opCount := 100
	var wg sync.WaitGroup
	setIt := func(i int) {
		defer wg.Done()
		setKey := fmt.Sprintf("%s%d", key, i)
		setValue := fmt.Sprintf("value%d", i)
		if err := ring.Set(setKey, setValue, 0); err != nil {
			t.Error(err)
		}
	}
	// set a bunch of keys
	wg.Add(opCount)
	for i := 1; i <= opCount; i++ {
		go setIt(i)
	}
	wg.Wait()
	getIt := func(i int) {
		defer wg.Done()
		getKey := fmt.Sprintf("%s%d", key, i)
		setValue := fmt.Sprintf("value%d", i)
		if getValue, err := ring.Get(getKey); err != nil {
			t.Error(err)
		} else {
			assert.EqualValues(t, setValue, getValue)
		}
	}
	// get a bunch of keys
	wg.Add(opCount)
	for i := 1; i <= opCount; i++ {
		go getIt(i)
	}
	wg.Wait()
}

func testConnectionLimits(t *testing.T, ring *memcacheRing) {
	key := "testConnectionLimits"
	opCount := 1000
	var wg sync.WaitGroup
	setIt := func(i int) {
		defer wg.Done()
		setKey := fmt.Sprintf("%s%d", key, i)
		if err := ring.Set(setKey, "", 0); err != nil {
			t.Error(err)
		}
	}
	// set a bunck of keys
	wg.Add(opCount)
	for i := 0; i < opCount; i++ {
		go setIt(i)
	}
	wg.Wait()
	// verify connections per server are less than the limit
	for _, server := range ring.servers {
		assert.True(t, server.connectionCount() <= 3)
	}
}

func testExpiration(t *testing.T, ring *memcacheRing) {
	if testing.Short() {
		t.Log("Skipping testExpiration()")
		return
	}
	key := "testExpiration"
	opCount := 100
	var wg sync.WaitGroup
	setIt := func(i int, expiration int) {
		defer wg.Done()
		setKey := fmt.Sprintf("%s%d", key, i)
		if err := ring.Set(setKey, "", expiration); err != nil {
			t.Error(err)
		}
	}
	// test both ways of setting expiration
	for i := 0; i < 2; i++ {
		var expiration int
		if i == 0 {
			expiration = 1
		} else {
			now := time.Now()
			expiration = int(now.Unix()) + 1
		}
		wg.Add(opCount)
		// set a bunch of keys with expiration
		for i := 0; i < opCount; i++ {
			go setIt(i, expiration)
		}
		wg.Wait()
		// sleep so the items will expire
		time.Sleep(2 * time.Second)
		// get a bunch of keys and verify cache miss
		for i := 0; i < opCount; i++ {
			getKey := fmt.Sprintf("%s%d", key, i)
			if _, err := ring.Get(getKey); err != nil {
				assert.EqualValues(t, CacheMiss, err)
			} else {
				t.Errorf("Expected a cache miss")
			}
		}
	}
}
