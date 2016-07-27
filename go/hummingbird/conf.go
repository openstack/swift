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
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/vaughan0/go-ini"
)

// Config represents an ini file.
type Config struct{ ini.File }

// Get fetches a value from the Config, looking in the DEFAULT section if not found in the specific section.  Also ignores "set " key prefixes, like paste.
func (f Config) Get(section string, key string) (string, bool) {
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

// GetDefault returns a value from the config, or returns the default setting if the entry doesn't exist.
func (f Config) GetDefault(section string, key string, dfl string) string {
	if value, ok := f.Get(section, key); ok {
		return value
	}
	return dfl
}

// GetBool loads a true/false value from the config, with support for things like "yes", "true", "1", "t", etc.
func (f Config) GetBool(section string, key string, dfl bool) bool {
	if value, ok := f.Get(section, key); ok {
		return LooksTrue(value)
	}
	return dfl
}

// GetInt loads an entry from the config, parsed as an integer value.
func (f Config) GetInt(section string, key string, dfl int64) int64 {
	if value, ok := f.Get(section, key); ok {
		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			return val
		}
		panic(fmt.Sprintf("Error parsing integer %s/%s from config.", section, key))
	}
	return dfl
}

// GetFloat loads an entry from the config, parsed as a floating point value.
func (f Config) GetFloat(section string, key string, dfl float64) float64 {
	if value, ok := f.Get(section, key); ok {
		if val, err := strconv.ParseFloat(value, 64); err == nil {
			return val
		}
		panic(fmt.Sprintf("Error parsing float %s/%s from config.", section, key))
	}
	return dfl
}

// GetLimit loads an entry from the config in the format of %d/%d.
func (f Config) GetLimit(section string, key string, dfla int64, dflb int64) (int64, int64) {
	if value, ok := f.Get(section, key); ok {
		fmt.Sscanf(value, "%d/%d", &dfla, &dflb)
	}
	return dfla, dflb
}

// HasSection determines whether or not the section exists in the ini file.
func (f Config) HasSection(section string) bool {
	return f.File[section] != nil
}

// LoadConfig loads an ini from a path.  The path should be a *.conf file or a *.conf.d directory.
func LoadConfig(path string) (Config, error) {
	file := Config{make(ini.File)}
	if fi, err := os.Stat(path); err != nil {
		return file, err
	} else if fi.IsDir() {
		files, err := filepath.Glob(filepath.Join(path, "*.conf"))
		if err != nil {
			return file, err
		}
		sort.Strings(files)
		for _, subfile := range files {
			sf, err := LoadConfig(subfile)
			if err != nil {
				return file, err
			}
			for sec, val := range sf.File {
				file.File[sec] = val
			}
		}
		return file, nil
	}
	return file, file.LoadFile(path)
}

// LoadConfigs finds and loads any configs that exist for the given path.  Multiple configs are supported for things like SAIO setups.
func LoadConfigs(path string) ([]Config, error) {
	configPaths := []string{}
	configs := []Config{}
	if fi, err := os.Stat(path); err == nil && fi.IsDir() && !strings.HasSuffix(path, ".conf.d") {
		if multiConfigs, err := filepath.Glob(filepath.Join(path, "*.conf")); err == nil {
			configPaths = append(configPaths, multiConfigs...)
		}
		if multiConfigs, err := filepath.Glob(filepath.Join(path, "*.conf.d")); err == nil {
			configPaths = append(configPaths, multiConfigs...)
		}
	} else {
		configPaths = append(configPaths, path)
	}
	for _, p := range configPaths {
		if config, err := LoadConfig(p); err == nil {
			configs = append(configs, config)
		}
	}
	if len(configs) == 0 {
		return nil, errors.New("Unable to find any configs")
	}
	return configs, nil
}

// StringConfig returns an Config from a string, for use in tests.
func StringConfig(data string) (Config, error) {
	file := Config{make(ini.File)}
	return file, file.Load(bytes.NewBufferString(data))
}

// UidFromConf returns the uid and gid for the user set in the first config found.
func UidFromConf(path string) (uint32, uint32, error) {
	configs, err := LoadConfigs(path)
	if err != nil {
		return 0, 0, err
	}
	for _, config := range configs {
		username := config.GetDefault("DEFAULT", "user", "swift")
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
	}
	return 0, 0, fmt.Errorf("Unable to find config")
}
