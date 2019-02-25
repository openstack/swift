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

// Returns the number of entries within the namespace
func countItems(kv KV, namespace byte) (itemCount uint64) {
	it := kv.NewIterator(namespace)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		itemCount++
	}

	return
}

// Returns KV stats in a map
// It will walk the whole KV to do so, used mostly for debugging, don't use for monitoring.
func CollectStats(s *server) (entriesCount map[string]uint64) {
	entriesCount = make(map[string]uint64)

	entriesCount["volume_count"] = countItems(s.kv, volumePrefix)
	entriesCount["object_count"] = countItems(s.kv, objectPrefix)
	entriesCount["deletequeue_count"] = countItems(s.kv, deleteQueuePrefix)
	entriesCount["quarantine_count"] = countItems(s.kv, quarantinePrefix)

	return
}
