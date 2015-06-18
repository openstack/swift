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
replicator, but the hummingbird replicator has a few improvements that only
allow it to work with the hummingbird object server.

A replication ID is sent with all requests to associate the calls for a single
partition replication operation.  This helps eliminate a common problem where we
serve an expensive REPLICATE call, only to reject the subsequent rsync due to
concurrency limits.  The server can inexpensively reject the initial REPLICATE
call if it's already at the incoming replication concurrency limit.

The SYNC method replaces rsync for transmitting data.  It functions like a
simple PUT operation to the real destination file path relative to the root,
and can accept .data, .meta, or .ts files.  It looks something like:

	SYNC /sda/objects/123/fff/ffffffffffffffffffffffffffffffff/12345.6789.data HTTP/1.1
	X-Attrs: [hex-encoded xattr data]
	X-Replication-Id: [unique ID per partition replication]
	Content-Length: [size]
	Expect: 100-Continue

SYNC request responses:
	201: file was successfully uploaded
	400: there was a problem with the request
	409: this file or a newer one already exists
	500: mostly caused by filesystem errors
	507: disk unmounted or full

The X-Attrs header contains the hex-encoded raw pickled metadata from the
file's xattrs.

If the replicator receives a 409 response with the "Newer-File-Exists: true"
header, the file is stale and can be removed.  It's hoped that this will reduce
the incidence of orphaned object data that can occur with long-lived handoffs.

Since the SYNC method invalidates hashes, the followup REPLICATE call to force
the server to recalculate suffixes is no longer required.  However, an "end"
request is sent to inform the server that the replication pass has finished,
and it can remove that replication ID from its concurrency accounting.
*/
package objectserver
