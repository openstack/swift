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
	"fmt"
	"strings"
)

type Policy struct {
	Index      int
	Type       string
	Name       string
	Aliases    []string
	Default    bool
	Deprecated bool
	Config     map[string]string
}

type PolicyList map[int]*Policy

// LoadPolicies loads policies, probably from /etc/swift/swift.conf
func normalLoadPolicies() PolicyList {
	policies := map[int]*Policy{0: &Policy{
		Index:      0,
		Type:       "replication",
		Name:       "Policy-0",
		Aliases:    nil,
		Default:    false,
		Deprecated: false,
	}}
	for _, loc := range configLocations {
		if conf, e := LoadConfig(loc); e == nil {
			for key := range conf.File {
				var policyIndex int
				if c, err := fmt.Sscanf(key, "storage-policy:%d", &policyIndex); err == nil && c == 1 {
					aliases := []string{}
					aliasList := conf.GetDefault(key, "aliases", "")
					for _, alias := range strings.Split(aliasList, ",") {
						alias = strings.Trim(alias, " ")
						if alias != "" {
							aliases = append(aliases, alias)
						}
					}
					policies[policyIndex] = &Policy{
						Index:      policyIndex,
						Type:       conf.GetDefault(key, "policy_type", "replication"),
						Name:       conf.GetDefault(key, "name", fmt.Sprintf("Policy-%d", policyIndex)),
						Aliases:    aliases,
						Deprecated: conf.GetBool(key, "deprecated", false),
						Default:    conf.GetBool(key, "default", false),
						Config:     map[string]string(conf.File[key]),
					}
				}
			}
			break
		}
	}
	defaultFound := false
	for _, policy := range policies {
		if policy.Default {
			defaultFound = true
		}
	}
	if !defaultFound {
		policies[0].Default = true
	}
	return PolicyList(policies)
}

type loadPoliciesFunc func() PolicyList

var LoadPolicies loadPoliciesFunc = normalLoadPolicies
