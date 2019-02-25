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

// RPC functions
//
// TODO: The naming of things is not consistent with the python code.

package main

import (
	"bytes"
	"fmt"
	pb "github.com/openstack/swift-rpc-losf/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/alecuyer/statsd"
	"net"
	"os"
	"path"
	"sync"
	"time"
)

type server struct {
	kv         KV
	grpcServer *grpc.Server

	// DB state (is it in sync with the volumes state)
	isClean bool

	diskPath   string // full path to mountpoint
	diskName   string // without the path
	socketPath string // full path to the socket

	// statsd used as is done in swift
	statsd_c *statsd.Client

	// channel to signal server should stop
	stopChan chan os.Signal
}

// The following consts are used as a key prefix for different types in the KV

// prefix for "volumes" (large file to which we write objects)
const volumePrefix = 'd'

// prefix for "objects" ("vfile" in the python code, would be a POSIX file on a regular backend)
const objectPrefix = 'o'

// This is meant to be used when a new file is created with the same name as an existing file.
// Deprecate this. As discussed in https://review.openstack.org/#/c/162243, overwriting an existing file
// never seemed like a good idea, and was done to mimic the existing renamer() behavior.
// We have no way to know if the new file is "better" than the existing one.
const deleteQueuePrefix = 'q'

// Quarantined objects
const quarantinePrefix = 'r'

// stats stored in the KV
const statsPrefix = 's'

// max key length in ascii format.
const maxObjKeyLen = 96

// RegisterVolume registers a new volume (volume) to the KV, given its index number and starting offset.
// Will return an error if the volume index already exists.
func (s *server) RegisterVolume(ctx context.Context, in *pb.NewVolumeInfo) (*pb.NewVolumeReply, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "RegisterVolume", "Partition": in.Partition, "Type": in.Type,
		"VolumeIndex": in.VolumeIndex, "Offset": in.Offset, "State": in.State})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeVolumeKey(in.VolumeIndex)

	// Does the volume already exist ?
	value, err := s.kv.Get(volumePrefix, key)
	if err != nil {
		reqlog.Error("unable to check for existing volume key")
		s.statsd_c.Increment("register_volume.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to check for existing volume key")
	}

	if value != nil {
		reqlog.Info("volume index already exists in db")
		s.statsd_c.Increment("register_volume.ok")
		return nil, status.Errorf(codes.AlreadyExists, "volume index already exists in db")
	}

	// Register the volume
	usedSpace := int64(0)
	value = EncodeVolumeValue(int64(in.Partition), int32(in.Type), int64(in.Offset), usedSpace, int64(in.State))

	err = s.kv.Put(volumePrefix, key, value)
	if err != nil {
		reqlog.Error("failed to Put new volume entry")
		s.statsd_c.Increment("register_volume.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to register new volume")
	}
	s.statsd_c.Increment("register_volume.ok")

	return &pb.NewVolumeReply{}, nil
}

// UnregisterVolume will delete a volume entry from the kv.
func (s *server) UnregisterVolume(ctx context.Context, in *pb.VolumeIndex) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "UnregisterVolume", "VolumeIndex": in.Index})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeVolumeKey(in.Index)

	// Check for key
	value, err := s.kv.Get(volumePrefix, key)
	if err != nil {
		reqlog.Error("unable to check for volume key")
		s.statsd_c.Increment("unregister_volume.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to check for volume key")
	}

	if value == nil {
		reqlog.Info("volume index does not exist in db")
		s.statsd_c.Increment("unregister_volume.ok")
		return nil, status.Errorf(codes.NotFound, "volume index does not exist in db")
	}

	// Key exists, delete it
	err = s.kv.Delete(volumePrefix, key)
	if err != nil {
		reqlog.Error("failed to Delete volume entry")
		s.statsd_c.Increment("unregister_volume.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to delete volume entry")
	}

	s.statsd_c.Increment("unregister_volume.ok")
	return &pb.Empty{}, nil
}

// UpdateVolumeState will modify an existing volume state
func (s *server) UpdateVolumeState(ctx context.Context, in *pb.NewVolumeState) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "UpdateVolumeState", "VolumeIndex": in.VolumeIndex, "State": in.State})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeVolumeKey(in.VolumeIndex)
	value, err := s.kv.Get(volumePrefix, key)
	if err != nil {
		reqlog.Error("unable to retrieve volume key")
		s.statsd_c.Increment("update_volume_state.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve volume key")
	}

	if value == nil {
		reqlog.Info("volume index does not exist in db")
		s.statsd_c.Increment("update_volume_state.ok")
		return nil, status.Errorf(codes.NotFound, "volume index does not exist in db")
	}

	partition, dfType, offset, usedSpace, state, err := DecodeVolumeValue(value)
	reqlog.WithFields(logrus.Fields{"current_state": state}).Info("updating state")
	if err != nil {
		reqlog.Error("failed to decode Volume value")
		s.statsd_c.Increment("update_volume_state.fail")
		return nil, status.Errorf(codes.Internal, "failed to decode Volume value")
	}

	value = EncodeVolumeValue(partition, dfType, offset, usedSpace, int64(in.State))
	err = s.kv.Put(volumePrefix, key, value)
	if err != nil {
		reqlog.Error("failed to Put updated volume entry")
		s.statsd_c.Increment("update_volume_state.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to update volume state")
	}
	s.statsd_c.Increment("update_volume_state.ok")

	return &pb.Empty{}, nil
}

// GetVolume will return a volume information
func (s *server) GetVolume(ctx context.Context, in *pb.VolumeIndex) (*pb.Volume, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "GetVolume", "Volume index": in.Index})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeVolumeKey(in.Index)
	value, err := s.kv.Get(volumePrefix, key)
	if err != nil {
		reqlog.Errorf("Failed to get volume key in KV: %s", err)
		s.statsd_c.Increment("get_volume.fail")
		return nil, status.Errorf(codes.Internal, "Failed to get volume key in KV")
	}

	if value == nil {
		reqlog.Info("No such Volume")
		s.statsd_c.Increment("get_volume.ok")
		return nil, status.Errorf(codes.NotFound, "No such Volume")
	}

	partition, dfType, nextOffset, _, state, err := DecodeVolumeValue(value)
	if err != nil {
		reqlog.Error("Failed to decode Volume value")
		s.statsd_c.Increment("get_volume.fail")
		return nil, status.Errorf(codes.Internal, "Failed to decode Volume value")
	}

	s.statsd_c.Increment("get_volume.ok")
	return &pb.Volume{VolumeIndex: in.Index, VolumeType: uint32(dfType), VolumeState: uint32(state),
		Partition: uint32(partition), NextOffset: uint64(nextOffset)}, nil
}

// ListVolumes will return all volumes of the given type, for the given partition.
// Currently this scans all volumes in the KV. Likely fast enough as long as the KV is cached.
// If it becomes a performance issue, we may want to add an in-memory cache indexed by partition.
func (s *server) ListVolumes(ctx context.Context, in *pb.ListVolumesInfo) (*pb.Volumes, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "ListVolumes", "Partition": in.Partition, "Type": in.Type})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	response := &pb.Volumes{}

	// Iterate over volumes and return the ones that match the request
	it := s.kv.NewIterator(volumePrefix)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		idx, err := DecodeVolumeKey(it.Key())
		if err != nil {
			reqlog.Error("failed to decode volume key")
			s.statsd_c.Increment("list_volumes.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode volume value")
		}

		partition, dfType, nextOffset, _, state, err := DecodeVolumeValue(it.Value())
		if err != nil {
			reqlog.Error("failed to decode volume value")
			s.statsd_c.Increment("list_volumes.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode volume value")
		}
		if uint32(partition) == in.Partition && pb.VolumeType(dfType) == in.Type {
			response.Volumes = append(response.Volumes, &pb.Volume{VolumeIndex: idx,
				VolumeType: uint32(in.Type), VolumeState: uint32(state),
				Partition: uint32(partition), NextOffset: uint64(nextOffset)})
		}
	}

	s.statsd_c.Increment("list_volumes.ok")
	return response, nil
}

// RegisterObject registers a new object to the kv.
func (s *server) RegisterObject(ctx context.Context, in *pb.NewObjectInfo) (*pb.NewObjectReply, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "RegisterObject",
		"Name":          fmt.Sprintf("%s", in.Name),
		"DiskPath":      s.diskPath,
		"VolumeIndex": in.VolumeIndex,
		"Offset":        in.Offset,
		"NextOffset":    in.NextOffset,
		"Length":        in.NextOffset - in.Offset, // debug
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// Check if volume exists
	volumeKey := EncodeVolumeKey(in.VolumeIndex)
	volumeValue, err := s.kv.Get(volumePrefix, volumeKey)
	if err != nil {
		reqlog.Error("unable to check for existing volume key")
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to check for existing volume key")
	}

	if volumeValue == nil {
		reqlog.Info("volume index does not exist in db")
		s.statsd_c.Increment("register_object.ok")
		return nil, status.Errorf(codes.FailedPrecondition, "volume index does not exist in db")
	}

	partition, volumeType, _, currentUsedSpace, state, err := DecodeVolumeValue(volumeValue)

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	objectValue := EncodeObjectValue(in.VolumeIndex, in.Offset)

	// If an object exists with the same name, we need to move it to the delete queue before overwriting the key.
	// On the regular file backend, this would happen automatically with the rename operation. In our case,
	// we would leak space. (The space will be reclaimed on compaction, but it shouldn't happen).

	var objMutex = &sync.Mutex{}
	objMutex.Lock()

	existingValue, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to check for existing object")
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if existingValue != nil {
		reqlog.Info("object already exists")
		s.statsd_c.Increment("register_object.ok")
		return nil, status.Errorf(codes.AlreadyExists, "object already exists")
	}

	// Update volume offset
	volumeNewValue := EncodeVolumeValue(int64(partition), volumeType, int64(in.NextOffset), int64(currentUsedSpace), state)
	wb := s.kv.NewWriteBatch()
	defer wb.Close()
	wb.Put(volumePrefix, volumeKey, volumeNewValue)
	wb.Put(objectPrefix, objectKey, objectValue)

	err = wb.Commit()
	if err != nil {
		reqlog.Error("failed to Put new volume value and new object entry")
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to update volume and register new object")
	}
	objMutex.Unlock()

	s.statsd_c.Increment("register_object.ok")
	return &pb.NewObjectReply{}, nil
}

// UnregisterObject removes an an object entry from the kv.
func (s *server) UnregisterObject(ctx context.Context, in *pb.ObjectName) (*pb.DelObjectReply, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "UnregisterObject",
		"Name":          fmt.Sprintf("%s", in.Name),
		"DiskPath":      s.diskPath,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("unregister_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("unregister_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("unregister_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	// Delete key
	err = s.kv.Delete(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("failed to Delete key")
		s.statsd_c.Increment("unregister_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to unregister object")
	}

	s.statsd_c.Increment("unregister_object.ok")
	return &pb.DelObjectReply{}, nil
}

// RenameObject changes an object key in the kv. (used for erasure code)
func (s *server) RenameObject(ctx context.Context, in *pb.RenameInfo) (*pb.RenameReply, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "RenameObject",
		"Name":     fmt.Sprintf("%s", in.Name),
		"NewName":  fmt.Sprintf("%s", in.NewName),
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	objectNewKey, err := EncodeObjectKey(in.NewName)
	if err != nil {
		reqlog.Error("unable to encode new object key")
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("rename_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	// Delete old entry and create a new one
	wb := s.kv.NewWriteBatch()
	defer wb.Close()
	wb.Delete(objectPrefix, objectKey)
	wb.Put(objectPrefix, objectNewKey, value)

	err = wb.Commit()
	if err != nil {
		reqlog.Error("failed to commit WriteBatch for rename")
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "failed to commit WriteBatch for rename")
	}

	s.statsd_c.Increment("rename_object.ok")
	return &pb.RenameReply{}, nil
}

// LoadObject returns an object information
func (s *server) LoadObject(ctx context.Context, in *pb.LoadObjectInfo) (*pb.Object, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "LoadObject",
		"Name":          fmt.Sprintf("%s", in.Name),
		"IsQuarantined": fmt.Sprintf("%t", in.IsQuarantined),
	})
	reqlog.Debug("RPC Call")

	var prefix byte

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("load_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	if in.IsQuarantined {
		prefix = quarantinePrefix
	} else {
		prefix = objectPrefix
	}
	reqlog.Debugf("is quarantined: %v", in.IsQuarantined)
	value, err := s.kv.Get(prefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("load_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("load_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	volumeIndex, offset, err := DecodeObjectValue(value)
	if err != nil {
		reqlog.Error("failed to decode object value")
		s.statsd_c.Increment("load_object.fail")
		return nil, status.Errorf(codes.Internal, "unable to read object")
	}

	s.statsd_c.Increment("load_object.ok")
	return &pb.Object{Name: in.Name, VolumeIndex: volumeIndex, Offset: offset}, nil
}

// QuarantineDir will change all keys below a given prefix to mark objects as quarantined. (the whole "directory")
// DEPRECATED. To remove once the matching python code is deployed everywhere
func (s *server) QuarantineDir(ctx context.Context, in *pb.ObjectPrefix) (*pb.DelObjectReply, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "QuarantineDir",
		"Prefix":   in.Prefix,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// prefix must be 32 characters for this to work (because we now encode the md5 hash, see
	// EncodeObjectKey in encoding.go
	if len(in.Prefix) != 32 {
		reqlog.Error("prefix len != 32")
		s.statsd_c.Increment("quarantine_dir.fail")
		return nil, status.Errorf(codes.Internal, "prefix len != 32")
	}

	prefix, err := EncodeObjectKey(in.Prefix)
	if err != nil {
		reqlog.Error("unable to encode object prefix")
		s.statsd_c.Increment("quarantine_dir.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	// TODO: Wrap in a WriteBatch. Still async, so we will still need to rely on the volume
	for it.Seek(prefix); it.Valid() && len(prefix) <= len(it.Key()) && bytes.Equal(prefix, it.Key()[:len(prefix)]); it.Next() {
		// Register quarantined object
		// TODO: may already be present with that name, in which case, append a unique suffix (1, 2, 3..), or we will
		// leak space in the volume file
		reqlog.WithFields(logrus.Fields{
			"Key": it.Key(),
		}).Debug("Quarantine")
		err := s.kv.Put(quarantinePrefix, it.Key(), it.Value())
		if err != nil {
			reqlog.Error("failed to Put new quarantined object entry")
			s.statsd_c.Increment("quarantine_dir.fail")
			return nil, status.Errorf(codes.Unavailable, "unable to register new quarantined object")
		}
		reqlog.Debug("registered new quarantined object")

		// Delete object key
		err = s.kv.Delete(objectPrefix, it.Key())
		if err != nil {
			reqlog.Error("failed to delete key")
			s.statsd_c.Increment("quarantine_dir.fail")
			return nil, status.Errorf(codes.Unavailable, "unable to unregister object")
		}
	}

	s.statsd_c.Increment("quarantine_dir.ok")
	return &pb.DelObjectReply{}, nil
}

// QuarantineObject
func (s *server) QuarantineObject(ctx context.Context, in *pb.ObjectName) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "QuarantineObject",
		"Name":     fmt.Sprintf("%s", in.Name),
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("quarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("quarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("quarantine_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	// Add quarantine key, delete obj key
	wb := s.kv.NewWriteBatch()
	defer wb.Close()
	// TODO: check here if an ohash already exists with the same name. Put files in the same dir, or make a new one ? (current swift code
	// appears to add an extension in that case. This will require a new format (encode/decode) in the KV)
	// Also check if full key already exists.
	wb.Put(quarantinePrefix, objectKey, value)
	wb.Delete(objectPrefix, objectKey)
	err = wb.Commit()
	if err != nil {
		reqlog.Error("failed to quarantine object")
		s.statsd_c.Increment("quarantine_object.fail")
		return nil, status.Error(codes.Unavailable, "unable to quarantine object")
	}

	s.statsd_c.Increment("quarantine_object.ok")
	return &pb.Empty{}, nil
}

// UnquarantineObject
func (s *server) UnquarantineObject(ctx context.Context, in *pb.ObjectName) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "UnquarantineObject",
		"Name":     fmt.Sprintf("%s", in.Name),
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Error("unable to encode object key")
		s.statsd_c.Increment("unquarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(quarantinePrefix, objectKey)
	if err != nil {
		reqlog.Error("unable to retrieve object")
		s.statsd_c.Increment("unquarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve object")
	}

	if value == nil {
		reqlog.Debug("object does not exist")
		s.statsd_c.Increment("unquarantine_object.ok")
		return nil, status.Errorf(codes.NotFound, "%s", in.Name)
	}

	// Add object key, delete quarantine key
	wb := s.kv.NewWriteBatch()
	defer wb.Close()
	wb.Put(objectPrefix, objectKey, value)
	wb.Delete(quarantinePrefix, objectKey)
	err = wb.Commit()
	if err != nil {
		reqlog.Error("failed to unquarantine object")
		s.statsd_c.Increment("unquarantine_object.fail")
		return nil, status.Error(codes.Unavailable, "unable to unquarantine object")
	}

	s.statsd_c.Increment("unquarantine_object.ok")
	return &pb.Empty{}, nil
}

// LoadObjectsByPrefix returns list of objects with the given prefix.
// In practice this is used to emulate the object hash directory that swift
// would create with the regular diskfile backend.
func (s *server) LoadObjectsByPrefix(ctx context.Context, in *pb.ObjectPrefix) (*pb.LoadObjectsResponse, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "LoadObjectsByPrefix",
		"Prefix":   fmt.Sprintf("%s", in.Prefix),
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// prefix must be 32 characters for this to work (because we now encode the md5 hash, see
	// EncodeObjectKey in encoding.go
	if len(in.Prefix) != 32 {
		reqlog.Error("prefix len != 32")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "prefix len != 32")
	}

	prefix, err := EncodeObjectKey(in.Prefix)
	if err != nil {
		reqlog.Error("unable to encode object prefix")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	response := &pb.LoadObjectsResponse{}

	// adds one byte because of prefix. Otherwise len(prefix) would be len(prefix)-1
	for it.Seek(prefix); it.Valid() && len(prefix) <= len(it.Key()) && bytes.Equal(prefix, it.Key()[:len(prefix)]); it.Next() {

		// Decode value
		volumeIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Error("failed to decode object value")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to read object")
		}

		key := make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		response.Objects = append(response.Objects, &pb.Object{Name: key, VolumeIndex: volumeIndex, Offset: offset})
	}

	s.statsd_c.Increment("load_objects_by_prefix.ok")
	return response, nil
}

// LoadObjectsByVolume returns a list of all objects within a volume.
// TODO: add an option to list quarantined objects
func (s *server) LoadObjectsByVolume(in *pb.VolumeIndex, stream pb.FileMgr_LoadObjectsByVolumeServer) error {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "LoadObjectsByVolume",
		"VolumeIndex": in.Index})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// Need an option to return quarantined files
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	// Objects are not indexed by volume. We have to scan the whole KV and examine each value.
	// It shouldn't matter as this is only used for compaction, and each object will have to be copied.
	// Disk activity dwarfs CPU usage. (for spinning rust anyway, but SSDs?)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		volumeIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Error("failed to decode object value")
			s.statsd_c.Increment("load_objects_by_volume.fail")
			return status.Errorf(codes.Internal, "unable to read object")
		}

		if volumeIndex == in.Index {
			key := make([]byte, 32+len(it.Key()[16:]))
			err = DecodeObjectKey(it.Key(), key)
			if err != nil {
				reqlog.Error("failed to decode object key")
				s.statsd_c.Increment("load_objects_by_prefix.fail")
				return status.Errorf(codes.Internal, "unable to decode object key")
			}
			object := &pb.Object{Name: key, VolumeIndex: volumeIndex, Offset: offset}
			err = stream.Send(object)
			if err != nil {
				reqlog.Error("failed to send streamed response")
				s.statsd_c.Increment("load_objects_by_volume.fail")
				return status.Errorf(codes.Internal, "failed to send streamed response")
			}
		}
	}
	s.statsd_c.Increment("load_objects_by_volume.ok")
	return nil
}

// ListPartitions returns a list of partitions for which we have objects.
// This is used to emulate a listdir() of partitions below the "objects" directory.
func (s *server) ListPartitions(ctx context.Context, in *pb.ListPartitionsInfo) (*pb.DirEntries, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "ListPartitions",
		"PartitionBits": in.PartitionBits,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	var currentPartition uint64
	var err error
	var ohash []byte

	// Partition bits
	pBits := int(in.PartitionBits)

	response := &pb.DirEntries{}

	// Seek to first object key
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()
	it.SeekToFirst()

	// No object in the KV.
	if !it.Valid() {
		s.statsd_c.Increment("list_partitions.ok")
		return response, nil
	}

	// Extract the md5 hash
	reqlog.WithFields(logrus.Fields{"key": it.Key()}).Debug("Raw first KV key")
	if len(it.Key()) < 16 {
		reqlog.WithFields(logrus.Fields{"key": it.Key()}).Error("object key < 16")
	} else {
		ohash = make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key()[:16], ohash)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(ohash, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return response, err
		}
	}

	response.Entry = append(response.Entry, fmt.Sprintf("%d", currentPartition))
	if err != nil {
		s.statsd_c.Increment("list_partitions.fail")
		return response, err
	}

	maxPartition, err := getLastPartition(pBits)

	for currentPartition < maxPartition {
		currentPartition++
		firstKey, err := getEncodedObjPrefixFromPartition(currentPartition, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return response, err
		}
		nextFirstKey, err := getEncodedObjPrefixFromPartition(currentPartition+1, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return response, err
		}

		// key logging is now wrong, as it's not the ascii form
		reqlog.WithFields(logrus.Fields{"currentPartition": currentPartition,
			"maxPartition": maxPartition,
			"firstKey":     firstKey,
			"ohash":        ohash,
			"nextFirstKey": nextFirstKey}).Debug("In loop")

		it.Seek(firstKey)
		if !it.Valid() {
			s.statsd_c.Increment("list_partitions.ok")
			return response, nil
		}

		if len(it.Key()) < 16 {
			reqlog.WithFields(logrus.Fields{"key": it.Key()}).Error("object key < 16")
		} else {
			ohash = make([]byte, 32+len(it.Key()[16:]))
			err = DecodeObjectKey(it.Key()[:16], ohash)
			if err != nil {
				reqlog.Error("failed to decode object key")
				s.statsd_c.Increment("load_objects_by_prefix.fail")
				return nil, status.Errorf(codes.Internal, "unable to decode object key")
			}
			// nextFirstKey is encoded, compare with encoded hash (16 first bits of the key)
			if bytes.Compare(it.Key()[:16], nextFirstKey) > 0 {
				// There was no key in currentPartition, find in which partition we are
				currentPartition, err = getPartitionFromOhash(ohash, pBits)
				if err != nil {
					s.statsd_c.Increment("list_partitions.fail")
					return response, err
				}
			}
			response.Entry = append(response.Entry, fmt.Sprintf("%d", currentPartition))
		}
	}

	reqlog.Debug("ListPartitions done")
	s.statsd_c.Increment("list_partitions.ok")
	return response, nil
}

// ListPartitionRecursive returns a list of files with structured path info (suffix, object hash) within a partition
// The response should really be streamed, but that makes eventlet hang on the python side...
// This is used to optimize REPLICATE on the object server.
func (s *server) ListPartitionRecursive(ctx context.Context, in *pb.ListPartitionInfo) (*pb.PartitionContent, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "ListPartitionRecursive",
		"Partition":     in.Partition,
		"PartitionBits": in.PartitionBits,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// Partition bits
	pBits := int(in.PartitionBits)
	partition := uint64(in.Partition)

	firstKey, err := getEncodedObjPrefixFromPartition(partition, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition_recursive.fail")
		return nil, status.Errorf(codes.Internal, "failed to calculate encoded object prefix from partition")
	}

	// Seek to first key in partition, if any
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	response := &pb.PartitionContent{}

	it.Seek(firstKey)
	// No object in the KV
	if !it.Valid() {
		s.statsd_c.Increment("list_partition_recursive.ok")
		return response, nil
	}

	key := make([]byte, 32+len(it.Key()[16:]))
	err = DecodeObjectKey(it.Key(), key)
	if err != nil {
		reqlog.Error("failed to decode object key")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "unable to decode object key")
	}
	currentPartition, err := getPartitionFromOhash(key, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition_recursive.fail")
		return nil, status.Errorf(codes.Internal, "unable to extract partition from object hash")
	}

	// Iterate over all files within the partition
	for currentPartition == partition {
		reqlog.Debug("Sending an entry")
		entry := &pb.FullPathEntry{Suffix: key[29:32], Ohash: key[:32], Filename: key[32:]}
		response.FileEntries = append(response.FileEntries, entry)

		it.Next()
		// Check if we're at the end of the KV
		if !it.Valid() {
			break
		}
		key = make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(key, pBits)
	}

	s.statsd_c.Increment("list_partition_recursive.ok")
	return response, nil
}

// ListPartition returns a list of suffixes within a partition
func (s *server) ListPartition(ctx context.Context, in *pb.ListPartitionInfo) (*pb.DirEntries, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "ListPartition",
		"Partition":     in.Partition,
		"PartitionBits": in.PartitionBits,
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	// Set to hold suffixes within partition
	suffixSet := make(map[[3]byte]bool)
	var suffix [3]byte

	// Partition bits
	pBits := int(in.PartitionBits)
	partition := uint64(in.Partition)

	response := &pb.DirEntries{}

	firstKey, err := getEncodedObjPrefixFromPartition(partition, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition.fail")
		return response, err
	}

	// Seek to first key in partition, if any
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	it.Seek(firstKey)
	// No object in the KV
	if !it.Valid() {
		s.statsd_c.Increment("list_partition.ok")
		return response, nil
	}

	key := make([]byte, 32+len(it.Key()[16:]))
	err = DecodeObjectKey(it.Key(), key)
	if err != nil {
		reqlog.Error("failed to decode object key")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "unable to decode object key")
	}
	currentPartition, err := getPartitionFromOhash(key, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition.fail")
		return response, err
	}

	// Get all suffixes in the partition
	for currentPartition == partition {
		// Suffix is the last three bytes of the object hash
		copy(suffix[:], key[29:32])
		suffixSet[suffix] = true
		it.Next()
		if !it.Valid() {
			break
		}
		key = make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(key, pBits)
	}

	// Build the response from the hashmap
	for suffix := range suffixSet {
		response.Entry = append(response.Entry, fmt.Sprintf("%s", suffix))
	}

	s.statsd_c.Increment("list_partition.ok")
	return response, nil
}

// ListSuffix returns a list of object hashes below the partition and suffix
func (s *server) ListSuffix(ctx context.Context, in *pb.ListSuffixInfo) (*pb.DirEntries, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function":      "ListSuffix",
		"Partition":     in.Partition,
		"Suffix":        fmt.Sprintf("%s", in.Suffix),
		"PartitionBits": in.PartitionBits,
	})

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	execTimeSerie := fmt.Sprintf("list_suffix.runtime.%s", s.diskName)
	defer s.statsd_c.NewTiming().Send(execTimeSerie)
	reqlog.Debug("RPC Call")

	lastOhash := make([]byte, 32)

	pBits := int(in.PartitionBits)
	partition := uint64(in.Partition)
	suffix := in.Suffix

	response := &pb.DirEntries{}

	failSerie := fmt.Sprintf("list_suffix.fail.%s", s.diskName)
	successSerie := fmt.Sprintf("list_suffix.ok.%s", s.diskName)
	firstKey, err := getEncodedObjPrefixFromPartition(partition, pBits)
	if err != nil {
		s.statsd_c.Increment(failSerie)
		return response, err
	}

	// Seek to first key in partition, if any
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	it.Seek(firstKey)
	// No object in the KV
	if !it.Valid() {
		s.statsd_c.Increment(successSerie)
		return response, nil
	}

	// Allocate the slice with a capacity matching the length of the longest possible key
	// We can then reuse it in the loop below. (avoid heap allocations, profiling showed it was an issue)
	curKey := make([]byte, 32+len(firstKey[16:]), maxObjKeyLen)
	err = DecodeObjectKey(firstKey, curKey)
	if err != nil {
		reqlog.Error("failed to decode object key")
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "unable to decode object key")
	}
	currentPartition, err := getPartitionFromOhash(curKey, pBits)
	if err != nil {
		s.statsd_c.Increment(failSerie)
		return response, err
	}

	for currentPartition == partition {
		// Suffix is the last three bytes of the object hash
		// key := make([]byte, 32+len(it.Key()[16:]))
		curKey = curKey[:32+len(it.Key()[16:])]
		err = DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		if bytes.Compare(curKey[29:32], suffix) == 0 {
			ohash := make([]byte, 32)
			ohash = curKey[:32]
			// Only add to the list if we have not already done so
			if !bytes.Equal(ohash, lastOhash) {
				response.Entry = append(response.Entry, (fmt.Sprintf("%s", ohash)))
				copy(lastOhash, ohash)
			}
		}
		it.Next()
		if !it.Valid() {
			break
		}
		curKey = curKey[:32+len(it.Key()[16:])]
		err = DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(curKey, pBits)
	}

	s.statsd_c.Increment(successSerie)
	return response, nil
}

// ListQuarantineOHashes returns the list of quarantined object hashes
// THe list may be large (over 500k entries seen in production). We use a stream
// as this is called with no eventlet.
func (s *server) ListQuarantinedOHashes(in *pb.Empty, stream pb.FileMgr_ListQuarantinedOHashesServer) error {
	reqlog := log.WithFields(logrus.Fields{
		"Function":  "ListQuarantineOHashes",
	})

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	reqlog.Debug("RPC Call")

	it := s.kv.NewIterator(quarantinePrefix)
	defer it.Close()

	curKey := make([]byte, maxObjKeyLen)
	lastOhash := make([]byte, 32)
	// Iterate over all quarantined files, extracting unique object hashes
	for it.SeekToFirst(); it.Valid(); it.Next() {
		curKey = curKey[:32+len(it.Key()[16:])]
		err := DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Error("failed to decode quarantined object key")
			s.statsd_c.Increment("list_quarantined_ohashes.fail")
			return status.Errorf(codes.Internal, "unable to decode quarantined object key")
		}
		if !bytes.Equal(curKey[:32], lastOhash) {
			objectName := &pb.QuarantinedObjectName{Name: curKey[:32]}
			err = stream.Send(objectName)
			if err != nil {
				reqlog.Error("failed to send streamed respone in ListQuarantinedOhashes")
				s.statsd_c.Increment("list_quarantined_ohashes.fail")
				return status.Errorf(codes.Internal, "failed to send streamed respone in ListQuarantinedOhashes")
			}
			copy(lastOhash, curKey[:32])
		}
	}

	s.statsd_c.Increment("list_quarantined_ohashes.ok")
	return nil
}

func (s *server) ListQuarantinedOHash(ctx context.Context, in *pb.ObjectPrefix) (*pb.LoadObjectsResponse, error) {
	reqlog := log.WithFields(logrus.Fields{
		"Function": "ListQuarantineOHash",
		"Prefix":   fmt.Sprintf("%s", in.Prefix),
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	if len(in.Prefix) != 32 {
		reqlog.Error("prefix len != 32")
		s.statsd_c.Increment("list_quarantined_ohash.fail")
		return nil, status.Errorf(codes.Internal, "prefix len != 32")
	}

	prefix, err := EncodeObjectKey(in.Prefix)
	if err != nil {
		reqlog.Error("unable to encode object prefix")
		s.statsd_c.Increment("list_quarantined_ohash.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(quarantinePrefix)
	defer it.Close()

	response := &pb.LoadObjectsResponse{}

	// adds one byte because of prefix. Otherwise len(prefix) would be len(prefix)-1
	for it.Seek(prefix); it.Valid() && len(prefix) <= len(it.Key()) && bytes.Equal(prefix, it.Key()[:len(prefix)]); it.Next() {

		// Decode value
		volumeIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Error("failed to decode object value")
			s.statsd_c.Increment("list_quarantined_ohash.fail")
			return nil, status.Errorf(codes.Internal, "unable to read object")
		}

		key := make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Error("failed to decode object key")
			s.statsd_c.Increment("list_quarantined_ohash.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		response.Objects = append(response.Objects, &pb.Object{Name: key, VolumeIndex: volumeIndex, Offset: offset})
	}

	s.statsd_c.Increment("list_quarantined_ohash.ok")
	return response, nil
}

func (s *server) GetNextOffset(ctx context.Context, in *pb.GetNextOffsetInfo) (*pb.VolumeNextOffset, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "GetNextOffset", "VolumeIndex": in.VolumeIndex})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeVolumeKey(in.VolumeIndex)

	value, err := s.kv.Get(volumePrefix, key)
	if err != nil {
		reqlog.Error("unable to retrieve volume key")
		s.statsd_c.Increment("get_next_offset.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to retrieve volume key")
	}

	if value == nil {
		reqlog.Info("volume index does not exist in db")
		s.statsd_c.Increment("get_next_offset.fail")
		return nil, status.Errorf(codes.FailedPrecondition, "volume index does not exist in db")
	}

	_, _, nextOffset, _, _, err := DecodeVolumeValue(value)
	if err != nil {
		reqlog.WithFields(logrus.Fields{"value": value}).Error("failed to decode volume value")
		s.statsd_c.Increment("get_next_offset.fail")
		return nil, status.Errorf(codes.Internal, "failed to decode volume value")
	}

	s.statsd_c.Increment("get_next_offset.ok")
	return &pb.VolumeNextOffset{Offset: uint64(nextOffset)}, nil
}

// GetStats returns stats for the KV. used for initial debugging, remove?
func (s *server) GetStats(ctx context.Context, in *pb.GetStatsInfo) (kvstats *pb.KVStats, err error) {
	kvstats = new(pb.KVStats)

	m := CollectStats(s)
	kvstats.Stats = m

	return
}

// Sets KV state (is in sync with volumes, or not)
func (s *server) SetKvState(ctx context.Context, in *pb.KvState) (*pb.Empty, error) {
	reqlog := log.WithFields(logrus.Fields{"Function": "SetClean", "IsClean": in.IsClean})
	reqlog.Info("RPC Call")

	s.isClean = in.IsClean
	return &pb.Empty{}, nil
}

// Gets KV state (is in sync with volumes, or not)
func (s *server) GetKvState(ctx context.Context, in *pb.Empty) (*pb.KvState, error) {
	state := new(pb.KvState)
	state.IsClean = s.isClean
	return state, nil
}

// Stops serving RPC requests and closes KV if we receive SIGTERM/SIGINT
func shutdownHandler(s *server, wg *sync.WaitGroup) {
	<-s.stopChan
	rlog := log.WithFields(logrus.Fields{"socket": s.socketPath})
	rlog.Info("Shutting down")

	// Stop serving RPC requests
	// If the graceful shutdown does not complete in 5s, call Stop(). (it is safe to do, will broadcast to the
	// running GracefulStop)
	rlog.Debug("Stopping RPC")
	t := time.AfterFunc(time.Second*5, func() { s.grpcServer.Stop() })
	s.grpcServer.GracefulStop()
	t.Stop()

	// Mark DB as clean
	if s.isClean == true {
		rlog.Debug("Mark DB as closed")
		err := MarkDbClosed(s.kv)
		if err != nil {
			rlog.Warn("Failed to mark db as clean when shutting down")
		}
	} else {
		rlog.Warn("State is not clean, not marking DB as closed (still out of sync with volumes)")
	}

	// Close KV
	rlog.Debug("Closing KV")
	s.kv.Close()
	wg.Done()
}

func runServer(kv KV, diskPath string, socketPath string, stopChan chan os.Signal, isClean bool) (err error) {
	var wg sync.WaitGroup

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return
	}
	os.Chmod(socketPath, 0660)

	grpcServer := grpc.NewServer()
	_, diskName := path.Split(path.Clean(diskPath))
	fs := &server{kv: kv, grpcServer: grpcServer, diskPath: diskPath, diskName: diskName, socketPath: socketPath,
		isClean: isClean, stopChan: stopChan}

	// Initialize statsd client
	statsdPrefix := "kv"
	fs.statsd_c, err = statsd.New(statsd.Prefix(statsdPrefix))
	if err != nil {
		return
	}

	// Start shutdown handler
	wg.Add(1)
	go shutdownHandler(fs, &wg)

	pb.RegisterFileMgrServer(grpcServer, fs)

	// Ignore the error returned by Serve on termination
	// (the doc : Serve always returns non-nil error.)
	grpcServer.Serve(lis)

	wg.Wait()

	return
}
