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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log/syslog"
	"math/rand"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	ini "github.com/vaughan0/go-ini"
)

var (
	PathNotDirErrorCode = 1
	OsErrorCode         = 2
	NotMountedErrorCode = 3
	LockPathError       = 4
	UnhandledError      = 5
	ONE_WEEK            = 604800
)

type BackendError struct {
	Err  error
	Code int
}

func (e BackendError) Error() string {
	return fmt.Sprintf("%s ( %d )", e.Err, e.Code)
}

type httpRange struct {
	Start, End int64
}

var GMT *time.Location

type IniFile struct{ ini.File }

func (f IniFile) Get(section string, key string) (string, bool) {
	if value, ok := f.File.Get(section, key); ok {
		return value, true
	} else if value, ok := f.File.Get("DEFAULT", key); ok {
		return value, true
	} else if value, ok := f.File.Get(section, "set "+key); ok {
		return value, true
	} else if value, ok := f.File.Get("DEFAULT", "set "+key); ok {
		return value, true
	}
	return "", false
}

func (f IniFile) GetDefault(section string, key string, dfl string) string {
	if value, ok := f.Get(section, key); ok {
		return value
	}
	return dfl
}

func (f IniFile) GetBool(section string, key string, dfl bool) bool {
	if value, ok := f.Get(section, key); ok {
		return LooksTrue(value)
	}
	return dfl
}

func (f IniFile) GetInt(section string, key string, dfl int64) int64 {
	if value, ok := f.Get(section, key); ok {
		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			return val
		}
		panic(fmt.Sprintf("Error parsing integer %s/%s from config.", section, key))
	}
	return dfl
}

func (f IniFile) GetLimit(section string, key string, dfla int64, dflb int64) (int64, int64) {
	if value, ok := f.Get(section, key); ok {
		fmt.Sscanf(value, "%d/%d", &dfla, &dflb)
	}
	return dfla, dflb
}

func (f IniFile) HasSection(section string) bool {
	return f.File[section] != nil
}

func LoadIniFile(filename string) (IniFile, error) {
	file := IniFile{make(ini.File)}
	return file, file.LoadFile(filename)
}

func UidFromConf(serverConf string) (uint32, uint32, error) {
	if ini, err := LoadIniFile(serverConf); err == nil {
		username := ini.GetDefault("DEFAULT", "user", "swift")
		usr, err := user.Lookup(username)
		if err != nil {
			return 0, 0, err
		}
		uid, err := strconv.ParseUint(usr.Uid, 10, 32)
		if err != nil {
			return 0, 0, err
		}
		gid, err := strconv.ParseUint(usr.Gid, 10, 32)
		if err != nil {
			return 0, 0, err
		}
		return uint32(uid), uint32(gid), nil
	} else {
		if matches, err := filepath.Glob(serverConf + "/*.conf"); err == nil && len(matches) > 0 {
			return UidFromConf(matches[0])
		}
	}
	return 0, 0, fmt.Errorf("Unable to find config file")
}

func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	partDir := filepath.Dir(filename)
	tmpFile, err := ioutil.TempFile(partDir, ".tmp-o-file")
	if err != nil {
		return err
	}
	defer tmpFile.Close()
	defer os.RemoveAll(tmpFile.Name())
	if err = tmpFile.Chmod(perm); err != nil {
		return err
	}
	if _, err = tmpFile.Write(data); err != nil {
		return err
	}
	if err = tmpFile.Sync(); err != nil {
		return err
	}
	if err = syscall.Rename(tmpFile.Name(), filename); err != nil {
		return err
	}
	return nil
}

func LockPath(directory string, timeout int) (*os.File, error) {
	sleepTime := 5
	lockfile := filepath.Join(directory, ".lock")
	file, err := os.OpenFile(lockfile, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		if os.IsNotExist(err) && os.MkdirAll(directory, 0755) == nil {
			file, err = os.OpenFile(lockfile, os.O_RDWR|os.O_CREATE, 0660)
		}
		if file == nil {
			return nil, errors.New(fmt.Sprintf("Unable to open file ccc. ( %s )", err.Error()))
		}
	}
	for stop := time.Now().Add(time.Duration(timeout) * time.Second); time.Now().Before(stop); {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return file, nil
		}
		time.Sleep(time.Millisecond * time.Duration(sleepTime))
		sleepTime += 5
	}
	file.Close()
	return nil, errors.New("Timed out")
}

func LockParent(file string, timeout int) (*os.File, error) {
	return LockPath(filepath.Dir(file), timeout)
}

func IsMount(dir string) (bool, error) {
	dir = filepath.Clean(dir)
	if fileinfo, err := os.Stat(dir); err == nil {
		if parentinfo, err := os.Stat(filepath.Dir(dir)); err == nil {
			return fileinfo.Sys().(*syscall.Stat_t).Dev != parentinfo.Sys().(*syscall.Stat_t).Dev, nil
		} else {
			return false, errors.New("Unable to stat parent")
		}
	} else {
		return false, errors.New("Unable to stat directory")
	}
}

var urlSafeMap = [256]bool{'A': true, 'B': true, 'C': true, 'D': true, 'E': true, 'F': true,
	'G': true, 'H': true, 'I': true, 'J': true, 'K': true, 'L': true, 'M': true, 'N': true,
	'O': true, 'P': true, 'Q': true, 'R': true, 'S': true, 'T': true, 'U': true, 'V': true,
	'W': true, 'X': true, 'Y': true, 'Z': true, 'a': true, 'b': true, 'c': true, 'd': true,
	'e': true, 'f': true, 'g': true, 'h': true, 'i': true, 'j': true, 'k': true, 'l': true,
	'm': true, 'n': true, 'o': true, 'p': true, 'q': true, 'r': true, 's': true, 't': true,
	'u': true, 'v': true, 'w': true, 'x': true, 'y': true, 'z': true, '0': true, '1': true,
	'2': true, '3': true, '4': true, '5': true, '6': true, '7': true, '8': true, '9': true,
	'_': true, '.': true, '-': true, '/': true,
}

func Urlencode(str string) string {
	// output matches python's urllib.quote()

	finalSize := len(str)
	for i := 0; i < len(str); i++ {
		if !urlSafeMap[str[i]] {
			finalSize += 2
		}
	}
	if finalSize == len(str) {
		return str
	}
	buf := make([]byte, finalSize)
	j := 0
	for i := 0; i < len(str); i++ {
		if urlSafeMap[str[i]] {
			buf[j] = str[i]
			j++
		} else {
			buf[j] = '%'
			buf[j+1] = "0123456789ABCDEF"[str[i]>>4]
			buf[j+2] = "0123456789ABCDEF"[str[i]&15]
			j += 3
		}
	}
	return string(buf)
}

func ParseDate(date string) (time.Time, error) {
	if GMT == nil {
		var err error
		GMT, err = time.LoadLocation("GMT")
		if err != nil {
			return time.Now(), err
		}
	}
	if ius, err := time.ParseInLocation(time.RFC1123, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.RFC1123Z, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.ANSIC, date, GMT); err == nil {
		return ius, nil
	}
	if ius, err := time.ParseInLocation(time.RFC850, date, GMT); err == nil {
		return ius, nil
	}
	if strings.Contains(date, "_") {
		all_date_parts := strings.Split(date, "_")
		date = all_date_parts[0]
	}
	if timestamp, err := strconv.ParseFloat(date, 64); err == nil {
		nans := int64((timestamp - float64(int64(timestamp))) * 1.0e9)
		return time.Unix(int64(timestamp), nans).In(GMT), nil
	}
	return time.Now(), errors.New("invalid time")
}

func FormatTimestamp(timestamp string) (string, error) {
	parsed, err := ParseDate(timestamp)
	if err != nil {
		return "", err
	}
	return parsed.Format("2006-01-02T15:04:05.999999"), nil
}

func CanonicalTimestamp(t float64) string {
	ret := strconv.FormatFloat(t, 'f', 5, 64)
	for len(ret) < 16 {
		ret = "0" + ret
	}
	return ret
}

func LooksTrue(check string) bool {
	check = strings.TrimSpace(strings.ToLower(check))
	return check == "true" || check == "yes" || check == "1" || check == "on" || check == "t" || check == "y"
}

func SetupLogger(facility string, prefix string, host string) *syslog.Writer {
	if host == "" {
		host = "127.0.0.1:514"
	}
	facility_mapping := map[string]syslog.Priority{"LOG_USER": syslog.LOG_USER,
		"LOG_MAIL": syslog.LOG_MAIL, "LOG_DAEMON": syslog.LOG_DAEMON,
		"LOG_AUTH": syslog.LOG_AUTH, "LOG_SYSLOG": syslog.LOG_SYSLOG,
		"LOG_LPR": syslog.LOG_LPR, "LOG_NEWS": syslog.LOG_NEWS,
		"LOG_UUCP": syslog.LOG_UUCP, "LOG_CRON": syslog.LOG_CRON,
		"LOG_AUTHPRIV": syslog.LOG_AUTHPRIV, "LOG_FTP": syslog.LOG_FTP,
		"LOG_LOCAL0": syslog.LOG_LOCAL0, "LOG_LOCAL1": syslog.LOG_LOCAL1,
		"LOG_LOCAL2": syslog.LOG_LOCAL2, "LOG_LOCAL3": syslog.LOG_LOCAL3,
		"LOG_LOCAL4": syslog.LOG_LOCAL4, "LOG_LOCAL5": syslog.LOG_LOCAL5,
		"LOG_LOCAL6": syslog.LOG_LOCAL6, "LOG_LOCAL7": syslog.LOG_LOCAL7}
	logger, err := syslog.Dial("udp", host, facility_mapping[facility], prefix)
	if err != nil || logger == nil {
		panic(fmt.Sprintf("Unable to dial logger: %s", err))
	}
	return logger
}

func UUID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", rand.Int63n(0xffffffff), rand.Int63n(0xffff), rand.Int63n(0xffff), rand.Int63n(0xffff), rand.Int63n(0xffffffffffff))
}

func GetTimestamp() string {
	return CanonicalTimestamp(float64(time.Now().UnixNano()) / 1000000000.0)
}

func GetTransactionId() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func HeaderGetDefault(h http.Header, key string, dfl string) string {
	val := h.Get(key)
	if val == "" {
		return dfl
	}
	return val
}

func ParseRange(rangeHeader string, fileSize int64) (reqRanges []httpRange, err error) {
	rangeHeader = strings.Replace(strings.ToLower(rangeHeader), " ", "", -1)
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil, nil
	}
	rangeHeader = rangeHeader[6:]
	rangeStrings := strings.Split(rangeHeader, ",")
	if len(rangeStrings) > 100 {
		return nil, errors.New("Too many ranges")
	}
	if len(rangeStrings) == 0 {
		return nil, nil
	}
	for _, rng := range rangeStrings {
		var start, end int64
		var err error
		startend := strings.Split(rng, "-")
		if len(startend) != 2 || (startend[0] == "" && startend[1] == "") {
			return nil, nil
		}
		if start, err = strconv.ParseInt(startend[0], 0, 64); err != nil && startend[0] != "" {
			return nil, nil
		}
		if end, err = strconv.ParseInt(startend[1], 0, 64); err != nil && startend[1] != "" {
			return nil, nil
		} else if startend[1] != "" && end < start {
			return nil, nil
		}

		if startend[0] == "" {
			if end == 0 {
				continue
			} else if end > fileSize {
				reqRanges = append(reqRanges, httpRange{0, fileSize})
			} else {
				reqRanges = append(reqRanges, httpRange{fileSize - end, fileSize})
			}
		} else if startend[1] == "" {
			if start < fileSize {
				reqRanges = append(reqRanges, httpRange{start, fileSize})
			} else {
				continue
			}
		} else if start < fileSize {
			if end+1 < fileSize {
				reqRanges = append(reqRanges, httpRange{start, end + 1})
			} else {
				reqRanges = append(reqRanges, httpRange{start, fileSize})
			}
		}
	}
	if len(reqRanges) == 0 {
		return nil, errors.New("Unsatisfiable range")
	}
	return reqRanges, nil
}

func UseMaxProcs() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func SetRlimits() {
	syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Max: 65536, Cur: 65536})
}

func GetEpochFromTimestamp(timestamp string) (string, error) {
	split_timestamp := strings.Split(timestamp, "_")
	floatTimestamp, err := strconv.ParseFloat(split_timestamp[0], 64)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", split_timestamp[0]))
	}
	return CanonicalTimestamp(floatTimestamp), nil
}

func StandardizeTimestamp(timestamp string) (string, error) {
	offset := strings.Contains(timestamp, "_")
	if offset {
		split_timestamp := strings.Split(timestamp, "_")
		floatTimestamp, err := strconv.ParseFloat(split_timestamp[0], 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", split_timestamp[0]))
		}
		intOffset, err := strconv.ParseInt(split_timestamp[1], 16, 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse int from '%s'.", split_timestamp[1]))
		}

		split_timestamp[0] = CanonicalTimestamp(floatTimestamp)
		split_timestamp[1] = fmt.Sprintf("%016x", intOffset)
		timestamp = strings.Join(split_timestamp, "_")
	} else {
		floatTimestamp, err := strconv.ParseFloat(timestamp, 64)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Could not parse float from '%s'.", timestamp))
		}
		timestamp = CanonicalTimestamp(floatTimestamp)
	}
	return timestamp, nil
}

func IsNotDir(err error) bool {
	if se, ok := err.(*os.SyscallError); ok {
		return se.Err == syscall.ENOTDIR
	}
	if se, ok := err.(*os.PathError); ok {
		return os.IsNotExist(se)
	}
	return false
}

var buf64kpool = NewFreePool(128)

func Copy(src io.Reader, dsts ...io.Writer) (written int64, err error) {
	var buf []byte
	var ok bool
	if buf, ok = buf64kpool.Get().([]byte); !ok {
		buf = make([]byte, 64*1024)
	}
	written, err = io.CopyBuffer(io.MultiWriter(dsts...), src, buf)
	buf64kpool.Put(buf)
	return
}

func CopyN(src io.Reader, n int64, dsts ...io.Writer) (written int64, err error) {
	written, err = Copy(io.LimitReader(src, n), dsts...)
	if written == n {
		return n, nil
	}
	if written < n && err == nil {
		err = io.EOF
	}
	return
}

func GetDefault(h http.Header, key string, dfl string) string {
	val := h.Get(key)
	if val == "" {
		return dfl
	}
	return val
}

func ReadDirNames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	if len(list) > 1 {
		sort.Strings(list)
	}
	return list, nil
}

// More like a map of semaphores.  I don't know what to call it.
type KeyedLimit struct {
	limitPerKey int64
	totalLimit  int64
	lock        sync.Mutex
	locked      map[string]bool
	inUse       map[string]int64
	totalUse    int64
}

func (k *KeyedLimit) Acquire(key string, force bool) int64 {
	// returns 0 if Acquire is successful, otherwise the number of requests inUse by disk or -1 if disk is locked
	k.lock.Lock()
	if k.locked[key] {
		k.lock.Unlock()
		return -1
	} else if v := k.inUse[key]; !force && (v >= k.limitPerKey || k.totalUse > k.totalLimit) {
		k.lock.Unlock()
		return v
	} else {
		k.inUse[key] += 1
		k.totalUse += 1
		k.lock.Unlock()
		return 0
	}
}

func (k *KeyedLimit) Release(key string) {
	k.lock.Lock()
	k.inUse[key] -= 1
	k.totalUse -= 1
	k.lock.Unlock()
}

func (k *KeyedLimit) Lock(key string) {
	k.lock.Lock()
	k.locked[key] = true
	k.lock.Unlock()
}

func (k *KeyedLimit) Unlock(key string) {
	k.lock.Lock()
	k.locked[key] = false
	k.lock.Unlock()
}

func (k *KeyedLimit) Keys() []string {
	k.lock.Lock()
	keys := make([]string, len(k.inUse))
	i := 0
	for key := range k.inUse {
		keys[i] = key
		i += 1
	}
	k.lock.Unlock()
	return keys
}

func (k *KeyedLimit) MarshalJSON() ([]byte, error) {
	k.lock.Lock()
	data, err := json.Marshal(k.inUse)
	k.lock.Unlock()
	return data, err
}

func NewKeyedLimit(limitPerKey int64, totalLimit int64) *KeyedLimit {
	return &KeyedLimit{limitPerKey: limitPerKey, totalLimit: totalLimit, locked: make(map[string]bool), inUse: make(map[string]int64)}
}

// GetHashPrefixAndSuffix retrieves the hash path prefix and suffix from
// the correct configs based on the environments setup. The suffix cannot
// be nil
func GetHashPrefixAndSuffix() (prefix string, suffix string, err error) {
	config_locations := []string{"/etc/hummingbird/hummingbird.conf", "/etc/swift/swift.conf"}

	for _, loc := range config_locations {
		if conf, e := LoadIniFile(loc); e == nil {
			var ok bool
			prefix, _ = conf.Get("swift-hash", "swift_hash_path_prefix")
			if suffix, ok = conf.Get("swift-hash", "swift_hash_path_suffix"); !ok {
				err = errors.New("Hash path suffix not defined")
				return
			}
			break
		}
	}
	return
}

func Exists(file string) bool {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		return false
	}
	return true
}

func CollectRuntimeMetrics(statsdHost string, statsdPort, statsdPause int64, prefix string) {
	address := fmt.Sprintf("%s:%d", statsdHost, statsdPort)
	client, err := statsd.NewClient(address, prefix)
	if err != nil {
		panic(fmt.Sprintf("Unable to connect to Statsd"))
	}

	defer client.Close()

	for {

		err = client.Gauge("cpu.goroutines", int64(runtime.NumGoroutine()), 1.0)
		if err != nil {
			panic(fmt.Sprintf("unable to send data"))
		}
		// CGo calls
		client.Gauge("cpu.cgo_calls", int64(runtime.NumCgoCall()), 1.0)

		m := &runtime.MemStats{}
		runtime.ReadMemStats(m)

		client.Gauge("mem.alloc", int64(m.Alloc), 1.0)
		client.Gauge("mem.total", int64(m.TotalAlloc), 1.0)
		client.Gauge("mem.sys", int64(m.Sys), 1.0)
		client.Gauge("mem.lookups", int64(m.Lookups), 1.0)
		client.Gauge("mem.malloc", int64(m.Mallocs), 1.0)
		client.Gauge("mem.frees", int64(m.Frees), 1.0)
		client.Gauge("mem.stack.inuse", int64(m.StackInuse), 1.0)
		client.Gauge("mem.stack.sys", int64(m.StackSys), 1.0)
		client.Gauge("mem.stack.mspan_inuse", int64(m.MSpanInuse), 1.0)
		client.Gauge("mem.stack.mspan_sys", int64(m.MSpanSys), 1.0)
		client.Gauge("mem.stack.mcache_inuse", int64(m.MCacheInuse), 1.0)
		client.Gauge("mem.stack.mcache_sys", int64(m.MCacheSys), 1.0)
		client.Gauge("mem.heap.alloc", int64(m.HeapAlloc), 1.0)
		client.Gauge("mem.heap.sys", int64(m.HeapSys), 1.0)
		client.Gauge("mem.heap.idle", int64(m.HeapIdle), 1.0)
		client.Gauge("mem.heap.inuse", int64(m.HeapInuse), 1.0)
		client.Gauge("mem.heap.released", int64(m.HeapReleased), 1.0)
		client.Gauge("mem.heap.objects", int64(m.HeapObjects), 1.0)
		client.Gauge("mem.othersys", int64(m.OtherSys), 1.0)
		client.Gauge("mem.gc.sys", int64(m.GCSys), 1.0)
		client.Gauge("mem.gc.next", int64(m.NextGC), 1.0)
		client.Gauge("mem.gc.last", int64(m.LastGC), 1.0)
		client.Gauge("mem.gc.pause_total", int64(m.PauseTotalNs), 1.0)
		client.Gauge("mem.gc.pause", int64(m.PauseNs[(m.NumGC+255)%256]), 1.0)
		client.Gauge("mem.gc.count", int64(m.NumGC), 1.0)

		time.Sleep(time.Duration(statsdPause) * time.Second)
	}

}

func DumpGoroutinesStackTrace(pid int) {
	filename := filepath.Join("/tmp", strconv.Itoa(pid)+".dump")
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	ioutil.WriteFile(filename, buf, 0644)
}
