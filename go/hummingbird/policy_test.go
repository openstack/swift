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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadPolicy(t *testing.T) {
	tempFile, _ := ioutil.TempFile("", "INI")
	tempFile.Write([]byte("[swift-hash]\nswift_hash_path_prefix = changeme\nswift_hash_path_suffix = changeme\n" +
		"[storage-policy:0]\nname = gold\naliases = yellow, orange\npolicy_type = replication\ndefault = yes\n" +
		"[storage-policy:1]\nname = silver\npolicy_type = replication\ndeprecated = yes\n"))
	oldConfigs := configLocations
	defer func() {
		configLocations = oldConfigs
		defer tempFile.Close()
		defer os.Remove(tempFile.Name())
	}()
	configLocations = []string{tempFile.Name()}
	policyList := LoadPolicies()
	require.Equal(t, policyList[0].Name, "gold")
	require.Equal(t, policyList[0].Default, true)
	require.Equal(t, policyList[0].Deprecated, false)
	require.Equal(t, policyList[0].Aliases, []string{"yellow", "orange"})
	require.Equal(t, policyList[1].Name, "silver")
	require.Equal(t, policyList[1].Deprecated, true)
	require.Equal(t, policyList[1].Default, false)
	require.Equal(t, policyList[1].Aliases, []string{})
}

func TestNoPolicies(t *testing.T) {
	tempFile, _ := ioutil.TempFile("", "INI")
	tempFile.Write([]byte("[swift-hash]\nswift_hash_path_prefix = changeme\nswift_hash_path_suffix = changeme\n"))
	oldConfigs := configLocations
	defer func() {
		configLocations = oldConfigs
		defer tempFile.Close()
		defer os.Remove(tempFile.Name())
	}()
	configLocations = []string{tempFile.Name()}
	policyList := LoadPolicies()
	require.Equal(t, policyList[0].Name, "Policy-0")
	require.Equal(t, policyList[0].Default, true)
	require.Equal(t, policyList[0].Deprecated, false)
}
