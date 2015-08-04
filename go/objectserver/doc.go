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

/*
Package objectserver provides a Object Server implementation.

Hummingbird Replication

The hummingbird object server is backwards-compatible with the python-swift
replicator, but hummingbird-to-hummingbird replication uses its own
simple protocol that it sends over a hijacked HTTP connection.

Messages are 32-bit length prefixed JSON serialized structures.  The basic
flow looks like:

	replicator sends a BeginReplicationRequest{Device string, Partition string, NeedHashes bool}
	server responds with a BeginReplicationResponse{Hashes map[string]string}
	for each object file in the partition where suffix hash doesn't match {
		replicator sends a SyncFileRequest{Path string, Xattrs string, Size int}
		server responds with a SyncFileResponse{Exists bool, NewerExists bool, GoAhead bool}
		if response.GoAhead is true {
			replicator sends raw file body
			server responds with a FileUploadResponse{Success bool}
		}
	}

The replicator limits concurrency per-device and overall.  When the server
gets a BeginReplicationRequest, it'll wait up to 60 seconds for a slot to open
up before rejecting it.

Unlike python-swift, the replicator will only read each filesystem once per
pass.
*/
package objectserver
