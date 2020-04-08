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
	"context"
	"fmt"
	"github.com/alecuyer/statsd/v2"
	"github.com/golang/protobuf/proto"
	"github.com/openstack/swift-rpc-losf/codes"
	pb "github.com/openstack/swift-rpc-losf/proto"
	"github.com/openstack/swift-rpc-losf/status"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type server struct {
	kv         KV
	httpServer *http.Server

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

type rpcFunc func(*server, context.Context, *[]byte) (*[]byte, error)

// RegisterVolume registers a new volume (volume) to the KV, given its index number and starting offset.
// Will return an error if the volume index already exists.
func RegisterVolume(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.RegisterVolumeRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("RegisterVolume failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		reqlog.Errorf("unable to check for existing volume key: %v", err)
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
		reqlog.Errorf("failed to Put new volume entry: %v", err)
		s.statsd_c.Increment("register_volume.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to register new volume")
	}
	s.statsd_c.Increment("register_volume.ok")

	out, err := proto.Marshal(&pb.RegisterVolumeReply{})
	if err != nil {
		reqlog.Errorf("failed to serialize reply for new volume entry: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply for new volume entry: %v", err)
	}
	return &out, nil
}

// UnregisterVolume will delete a volume entry from the kv.
func UnregisterVolume(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.UnregisterVolumeRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("UnregisterVolume failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		reqlog.Errorf("unable to check for volume key: %v", err)
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
		reqlog.Errorf("failed to Delete volume entry: %v", err)
		s.statsd_c.Increment("unregister_volume.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to delete volume entry")
	}

	s.statsd_c.Increment("unregister_volume.ok")
	return serializePb(&pb.UnregisterVolumeReply{})
}

// UpdateVolumeState will modify an existing volume state
func UpdateVolumeState(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.UpdateVolumeStateRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("UpdateVolumeState failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{"Function": "UpdateVolumeState", "VolumeIndex": in.VolumeIndex, "State": in.State})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeVolumeKey(in.VolumeIndex)
	value, err := s.kv.Get(volumePrefix, key)
	if err != nil {
		reqlog.Errorf("unable to retrieve volume key: %v", err)
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
		reqlog.Errorf("failed to decode Volume value: %v", err)
		s.statsd_c.Increment("update_volume_state.fail")
		return nil, status.Errorf(codes.Internal, "failed to decode Volume value")
	}

	value = EncodeVolumeValue(partition, dfType, offset, usedSpace, int64(in.State))
	err = s.kv.Put(volumePrefix, key, value)
	if err != nil {
		reqlog.Errorf("failed to Put updated volume entry: %v", err)
		s.statsd_c.Increment("update_volume_state.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to update volume state")
	}
	s.statsd_c.Increment("update_volume_state.ok")

	out, err := proto.Marshal(&pb.UpdateVolumeStateReply{})
	if err != nil {
		reqlog.Errorf("failed to serialize reply for update volume: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply for update volume: %v", err)
	}
	return &out, nil
}

// GetVolume will return a volume information
func GetVolume(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.GetVolumeRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("GetVolume failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{"Function": "GetVolume", "Volume index": in.Index})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeVolumeKey(in.Index)
	value, err := s.kv.Get(volumePrefix, key)
	if err != nil {
		reqlog.Errorf("Failed to get volume key %d in KV: %v", key, err)
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
		reqlog.Errorf("Failed to decode Volume value: %v", err)
		s.statsd_c.Increment("get_volume.fail")
		return nil, status.Errorf(codes.Internal, "Failed to decode Volume value")
	}

	s.statsd_c.Increment("get_volume.ok")

	pb_volume := pb.GetVolumeReply{VolumeIndex: in.Index, VolumeType: pb.VolumeType(dfType), VolumeState: uint32(state),
		Partition: uint32(partition), NextOffset: uint64(nextOffset)}
	out, err := proto.Marshal(&pb_volume)
	if err != nil {
		reqlog.Errorf("failed to serialize reply for get volume: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply for get volume: %v", err)
	}
	return &out, nil
}

// ListVolumes will return all volumes of the given type, for the given partition.
// If GetlAllVolumes is true, all volumes are listed (all types, all partitions)
// Currently this scans all volumes in the KV. Likely fast enough as long as the KV is cached.
// If it becomes a performance issue, we may want to add an in-memory cache indexed by partition.
func ListVolumes(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.ListVolumesRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("ListVolumes failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{"Function": "ListVolumes", "Partition": in.Partition, "Type": in.Type})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	response := &pb.ListVolumesReply{}

	// Iterate over volumes and return the ones that match the request
	it := s.kv.NewIterator(volumePrefix)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		idx, err := DecodeVolumeKey(it.Key())
		if err != nil {
			reqlog.Errorf("failed to decode volume key: %v", err)
			s.statsd_c.Increment("list_volumes.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode volume value")
		}

		partition, dfType, nextOffset, _, state, err := DecodeVolumeValue(it.Value())
		if err != nil {
			reqlog.Errorf("failed to decode volume value: %v", err)
			s.statsd_c.Increment("list_volumes.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode volume value")
		}
		if uint32(partition) == in.Partition && pb.VolumeType(dfType) == in.Type {
			response.Volumes = append(response.Volumes, &pb.Volume{VolumeIndex: idx,
				VolumeType: pb.VolumeType(in.Type), VolumeState: uint32(state),
				Partition: uint32(partition), NextOffset: uint64(nextOffset)})
		}
	}

	s.statsd_c.Increment("list_volumes.ok")
	out, err := proto.Marshal(response)
	if err != nil {
		reqlog.Errorf("failed to serialize reply for list volumes: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply for list volumes: %v", err)
	}
	return &out, nil
}

// RegisterObject registers a new object to the kv.
func RegisterObject(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.RegisterObjectRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("RegisterObject failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{
		"Function":    "RegisterObject",
		"Name":        fmt.Sprintf("%s", in.Name),
		"DiskPath":    s.diskPath,
		"VolumeIndex": in.VolumeIndex,
		"Offset":      in.Offset,
		"NextOffset":  in.NextOffset,
		"Length":      in.NextOffset - in.Offset, // debug
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
		reqlog.Errorf("unable to check for existing volume key: %v", err)
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
		reqlog.Errorf("unable to encode object key: %v", err)
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
		reqlog.Errorf("unable to check for existing object: %v", err)
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
		reqlog.Errorf("failed to Put new volume value and new object entry: %v", err)
		s.statsd_c.Increment("register_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to update volume and register new object")
	}
	objMutex.Unlock()

	s.statsd_c.Increment("register_object.ok")

	out, err := proto.Marshal(&pb.RegisterObjectReply{})
	if err != nil {
		reqlog.Errorf("failed to serialize reply: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply: %v", err)
	}
	return &out, nil
}

// UnregisterObject removes an an object entry from the kv.
func UnregisterObject(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.UnregisterObjectRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("UnregisterObject failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}
	reqlog := log.WithFields(logrus.Fields{
		"Function": "UnregisterObject",
		"Name":     fmt.Sprintf("%s", in.Name),
		"DiskPath": s.diskPath,
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	objectKey, err := EncodeObjectKey(in.Name)
	if err != nil {
		reqlog.Errorf("unable to encode object key: %v", err)
		s.statsd_c.Increment("unregister_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Errorf("unable to retrieve object: %v", err)
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
		reqlog.Errorf("failed to Delete key: %v", err)
		s.statsd_c.Increment("unregister_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to unregister object")
	}

	s.statsd_c.Increment("unregister_object.ok")
	out, err := proto.Marshal(&pb.UnregisterObjectReply{})
	if err != nil {
		reqlog.Errorf("failed to serialize reply for del object reply: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply for del object reply: %v", err)
	}
	return &out, nil
}

// RenameObject changes an object key in the kv. (used for erasure code)
func RenameObject(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.RenameObjectRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		reqlog.Errorf("unable to encode object key: %v", err)
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	objectNewKey, err := EncodeObjectKey(in.NewName)
	if err != nil {
		reqlog.Errorf("unable to encode new object key: %v", err)
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Errorf("unable to retrieve object: %v", err)
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
		reqlog.Errorf("failed to commit WriteBatch for rename: %v", err)
		s.statsd_c.Increment("rename_object.fail")
		return nil, status.Errorf(codes.Unavailable, "failed to commit WriteBatch for rename")
	}

	s.statsd_c.Increment("rename_object.ok")

	out, err := proto.Marshal(&pb.RenameObjectReply{})
	if err != nil {
		reqlog.Errorf("failed to serialize reply: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply: %v", err)
	}
	return &out, nil
}

// LoadObject returns an object information
func LoadObject(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.LoadObjectRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		reqlog.Errorf("unable to encode object key: %v", err)
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
		reqlog.Errorf("unable to retrieve object: %v", err)
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
		reqlog.Errorf("failed to decode object value: %v", err)
		s.statsd_c.Increment("load_object.fail")
		return nil, status.Errorf(codes.Internal, "unable to read object")
	}

	s.statsd_c.Increment("load_object.ok")

	out, err := proto.Marshal(&pb.LoadObjectReply{Name: in.Name, VolumeIndex: volumeIndex, Offset: offset})
	if err != nil {
		reqlog.Errorf("failed to serialize reply: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply: %v", err)
	}
	return &out, nil
}

// QuarantineObject
func QuarantineObject(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.QuarantineObjectRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		reqlog.Errorf("unable to encode object key: %v", err)
		s.statsd_c.Increment("quarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(objectPrefix, objectKey)
	if err != nil {
		reqlog.Errorf("unable to retrieve object: %v", err)
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
		reqlog.Errorf("failed to quarantine object: %v", err)
		s.statsd_c.Increment("quarantine_object.fail")
		return nil, status.Error(codes.Unavailable, "unable to quarantine object")
	}

	s.statsd_c.Increment("quarantine_object.ok")

	out, err := proto.Marshal(&pb.QuarantineObjectReply{})
	if err != nil {
		reqlog.Errorf("failed to serialize reply: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply: %v", err)
	}
	return &out, nil
}

// UnquarantineObject
func UnquarantineObject(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.UnquarantineObjectRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		reqlog.Errorf("unable to encode object key: %v", err)
		s.statsd_c.Increment("unquarantine_object.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object key")
	}

	value, err := s.kv.Get(quarantinePrefix, objectKey)
	if err != nil {
		reqlog.Errorf("unable to retrieve object: %v", err)
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
		reqlog.Errorf("failed to unquarantine object: %v", err)
		s.statsd_c.Increment("unquarantine_object.fail")
		return nil, status.Error(codes.Unavailable, "unable to unquarantine object")
	}

	s.statsd_c.Increment("unquarantine_object.ok")

	out, err := proto.Marshal(&pb.UnquarantineObjectReply{})
	if err != nil {
		reqlog.Errorf("failed to serialize reply: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply: %v", err)
	}
	return &out, nil
}

// LoadObjectsByPrefix returns list of objects with the given prefix.
// In practice this is used to emulate the object hash directory that swift
// would create with the regular diskfile backend.
func LoadObjectsByPrefix(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.LoadObjectsByPrefixRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		reqlog.Errorf("unable to encode object prefix: %v", err)
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	response := &pb.LoadObjectsByPrefixReply{}

	// adds one byte because of prefix. Otherwise len(prefix) would be len(prefix)-1
	for it.Seek(prefix); it.Valid() && len(prefix) <= len(it.Key()) && bytes.Equal(prefix, it.Key()[:len(prefix)]); it.Next() {

		// Decode value
		volumeIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Errorf("failed to decode object value: %v", err)
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to read object")
		}

		key := make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Errorf("failed to decode object key: %v", err)
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		response.Objects = append(response.Objects, &pb.Object{Name: key, VolumeIndex: volumeIndex, Offset: offset})
	}

	s.statsd_c.Increment("load_objects_by_prefix.ok")

	return serializePb(response)
}

// LoadObjectsByVolume returns a list of all objects within a volume, with pagination.
// Quarantined, if true, will return only quarantined objects, if false, non-quarantined objects.
// PageToken is the object name to start from, as returned from a previous call in the
// NextPageToken field. If empty, the iterator will start from the first objects in the volume.
// PageSize is the maximum count of items to return. If zero, the server will pick a reasonnable limit.
// func (s *server) LoadObjectsByVolume(in *pb.VolumeIndex, stream pb.FileMgr_LoadObjectsByVolumeServer) error {
func LoadObjectsByVolume(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.LoadObjectsByVolumeRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{
		"Function":    "LoadObjectsByVolume",
		"VolumeIndex": in.Index,
		"PageToken":   in.PageToken,
		"PageSize":    in.PageSize,
	})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	limit := in.PageSize
	if limit == 0 {
		reqlog.Debug("page_size was not specified, set it to 10000")
		limit = 10000
	}

	pageToken := make([]byte, len(in.PageToken))
	pageToken = in.PageToken
	if bytes.Equal(pageToken, []byte("")) {
		pageToken = []byte(strings.Repeat("0", 32))
	}

	prefix, err := EncodeObjectKey(pageToken)
	if err != nil {
		reqlog.Errorf("unable to encode object prefix: %v", err)
		s.statsd_c.Increment("load_objects_by_volume.fail")
		return nil, status.Errorf(codes.Internal, "unable to encode object prefix")
	}

	// Return either quarantined or "regular" objects
	var it Iterator
	if in.Quarantined {
		it = s.kv.NewIterator(quarantinePrefix)
	} else {
		it = s.kv.NewIterator(objectPrefix)
	}
	defer it.Close()

	response := &pb.LoadObjectsByVolumeReply{}

	// Objects are not indexed by volume. We have to scan the whole KV and examine each value.
	// It shouldn't matter as this is only used for compaction, and each object will have to be copied.
	// Disk activity dwarfs CPU usage. (for spinning rust anyway, but SSDs?)
	count := uint32(0)
	for it.Seek(prefix); it.Valid() && count < limit; it.Next() {
		volumeIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Errorf("failed to decode object value: %v", err)
			s.statsd_c.Increment("load_objects_by_volume.fail")
			return nil, status.Errorf(codes.Internal, "unable to read object")
		}

		if volumeIndex == in.Index {
			key := make([]byte, 32+len(it.Key()[16:]))
			err = DecodeObjectKey(it.Key(), key)
			if err != nil {
				reqlog.Errorf("failed to decode object key: %v", err)
				s.statsd_c.Increment("load_objects_by_prefix.fail")
				return nil, status.Errorf(codes.Internal, "unable to decode object key")
			}
			response.Objects = append(response.Objects, &pb.Object{Name: key, VolumeIndex: volumeIndex, Offset: offset})
			count++
		}
	}

	// Set NextPageToken if there is at least one ohash found in the same volume
	for ; it.Valid(); it.Next() {
		volumeIndex, _, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Errorf("failed to decode object value: %v", err)
			s.statsd_c.Increment("load_objects_by_volume.fail")
			return nil, status.Errorf(codes.Internal, "unable to read object")
		}

		if volumeIndex == in.Index {
			key := make([]byte, 32+len(it.Key()[16:]))
			err = DecodeObjectKey(it.Key(), key)
			if err != nil {
				reqlog.Errorf("failed to decode object key: %v", err)
				s.statsd_c.Increment("load_objects_by_prefix.fail")
				return nil, status.Errorf(codes.Internal, "unable to decode object key")
			}
			nextPageToken := make([]byte, len(key))
			copy(nextPageToken, key)
			response.NextPageToken = key
			break
		}

	}
	s.statsd_c.Increment("load_objects_by_volume.ok")
	return serializePb(response)
}

// ListPartitions returns a list of partitions for which we have objects.
// This is used to emulate a listdir() of partitions below the "objects" directory.
func ListPartitions(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.ListPartitionsRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		return serializePb(response)
	}

	// Extract the md5 hash
	if len(it.Key()) < 16 {
		reqlog.WithFields(logrus.Fields{"key": it.Key()}).Error("object key < 16")
	} else {
		ohash = make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key()[:16], ohash)
		if err != nil {
			reqlog.Errorf("failed to decode object key: %v", err)
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(ohash, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return nil, err
		}
	}

	response.Entry = append(response.Entry, fmt.Sprintf("%d", currentPartition))
	if err != nil {
		s.statsd_c.Increment("list_partitions.fail")
		return nil, err
	}

	maxPartition, err := getLastPartition(pBits)

	for currentPartition < maxPartition {
		currentPartition++
		firstKey, err := getEncodedObjPrefixFromPartition(currentPartition, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return nil, err
		}
		nextFirstKey, err := getEncodedObjPrefixFromPartition(currentPartition+1, pBits)
		if err != nil {
			s.statsd_c.Increment("list_partitions.fail")
			return nil, err
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
			return serializePb(response)
		}

		if len(it.Key()) < 16 {
			reqlog.WithFields(logrus.Fields{"key": it.Key()}).Error("object key < 16")
		} else {
			ohash = make([]byte, 32+len(it.Key()[16:]))
			err = DecodeObjectKey(it.Key()[:16], ohash)
			if err != nil {
				reqlog.Errorf("failed to decode object key: %v", err)
				s.statsd_c.Increment("load_objects_by_prefix.fail")
				return nil, status.Errorf(codes.Internal, "unable to decode object key")
			}
			// nextFirstKey is encoded, compare with encoded hash (16 first bits of the key)
			if bytes.Compare(it.Key()[:16], nextFirstKey) > 0 {
				// There was no key in currentPartition, find in which partition we are
				currentPartition, err = getPartitionFromOhash(ohash, pBits)
				if err != nil {
					s.statsd_c.Increment("list_partitions.fail")
					return nil, err
				}
			}
			response.Entry = append(response.Entry, fmt.Sprintf("%d", currentPartition))
		}
	}

	s.statsd_c.Increment("list_partitions.ok")
	return serializePb(response)
}

// ListPartition returns a list of suffixes within a partition
func ListPartition(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.ListPartitionRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		return nil, err
	}

	// Seek to first key in partition, if any
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	it.Seek(firstKey)
	// No object in the KV
	if !it.Valid() {
		s.statsd_c.Increment("list_partition.ok")
		return serializePb(response)
	}

	key := make([]byte, 32+len(it.Key()[16:]))
	err = DecodeObjectKey(it.Key(), key)
	if err != nil {
		reqlog.Errorf("failed to decode object key: %v", err)
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "unable to decode object key")
	}
	currentPartition, err := getPartitionFromOhash(key, pBits)
	if err != nil {
		s.statsd_c.Increment("list_partition.fail")
		return nil, err
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
			reqlog.Errorf("failed to decode object key: %v", err)
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
	return serializePb(response)
}

// ListSuffix returns a list of object hashes below the partition and suffix
func ListSuffix(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.ListSuffixRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		return nil, err
	}

	// Seek to first key in partition, if any
	it := s.kv.NewIterator(objectPrefix)
	defer it.Close()

	it.Seek(firstKey)
	// No object in the KV
	if !it.Valid() {
		s.statsd_c.Increment(successSerie)
		return serializePb(response)
	}

	// Allocate the slice with a capacity matching the length of the longest possible key
	// We can then reuse it in the loop below. (avoid heap allocations, profiling showed it was an issue)
	curKey := make([]byte, 32+len(firstKey[16:]), maxObjKeyLen)
	err = DecodeObjectKey(firstKey, curKey)
	if err != nil {
		reqlog.Errorf("failed to decode object key: %v", err)
		s.statsd_c.Increment("load_objects_by_prefix.fail")
		return nil, status.Errorf(codes.Internal, "unable to decode object key")
	}
	currentPartition, err := getPartitionFromOhash(curKey, pBits)
	if err != nil {
		s.statsd_c.Increment(failSerie)
		return nil, err
	}

	for currentPartition == partition {
		// Suffix is the last three bytes of the object hash
		// key := make([]byte, 32+len(it.Key()[16:]))
		curKey = curKey[:32+len(it.Key()[16:])]
		err = DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Errorf("failed to decode object key: %v", err)
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
			reqlog.Errorf("failed to decode object key: %v", err)
			s.statsd_c.Increment("load_objects_by_prefix.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		currentPartition, err = getPartitionFromOhash(curKey, pBits)
	}

	s.statsd_c.Increment(successSerie)
	return serializePb(response)
}

// Returns a list of quarantiened object hashes, with pagination.
// PageToken is the ohash to start from, as returned from a previous call in the
// NextPageToken field. If empty, the iterator will start from the first quarantined
// object hash. PageSize is the maximum count of items to return. If zero,
// the server will pick a reasonnable limit.
func ListQuarantinedOHashes(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.ListQuarantinedOHashesRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{
		"Function":  "ListQuarantinedOhashes",
		"PageToken": fmt.Sprintf("%s", in.PageToken),
		"PageSize":  fmt.Sprintf("%d", in.PageSize),
	})
	reqlog.Debug("RPC Call")

	if !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	limit := in.PageSize
	if limit == 0 {
		reqlog.Debug("page_size was not specified, set it to 10000")
		limit = 10000
	}

	pageToken := make([]byte, 32)
	pageToken = in.PageToken
	if bytes.Equal(pageToken, []byte("")) {
		pageToken = []byte(strings.Repeat("0", 32))
	}
	if len(pageToken) != 32 {
		reqlog.Error("prefix len != 32")
		s.statsd_c.Increment("list_quarantined_ohashes.fail")
		return nil, status.Errorf(codes.InvalidArgument, "page token length != 32")
	}

	prefix, err := EncodeObjectKey(pageToken)
	if err != nil {
		reqlog.Errorf("unable to encode object prefix: %v", err)
		s.statsd_c.Increment("list_quarantined_ohashes.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(quarantinePrefix)
	defer it.Close()

	response := &pb.ListQuarantinedOHashesReply{}
	curKey := make([]byte, maxObjKeyLen)
	lastOhash := make([]byte, 32)

	count := uint32(0)
	for it.Seek(prefix); it.Valid() && count < limit; it.Next() {
		curKey = curKey[:32+len(it.Key()[16:])]
		err = DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Errorf("failed to decode quarantined object key: %v", err)
			s.statsd_c.Increment("list_quarantined_ohashes.fail")
			return nil, status.Errorf(codes.Internal, "unable decode quarantined object key")
		}
		if !bytes.Equal(curKey[:32], lastOhash) {
			ohash := make([]byte, 32)
			copy(ohash, curKey[:32])
			response.Objects = append(response.Objects, &pb.QuarantinedObjectName{Name: ohash})
			copy(lastOhash, curKey[:32])
			count++
		}
	}

	// Set NextPageToken if there is at least one ohash beyond what we have returned
	for ; it.Valid(); it.Next() {
		curKey = curKey[:32+len(it.Key()[16:])]
		err = DecodeObjectKey(it.Key(), curKey)
		if err != nil {
			reqlog.Errorf("failed to decode quarantined object key: %v", err)
			s.statsd_c.Increment("list_quarantined_ohashes.fail")
			return nil, status.Errorf(codes.Internal, "unable decode quarantined object key")
		}
		if !bytes.Equal(curKey[:32], lastOhash) {
			nextPageToken := make([]byte, 32)
			copy(nextPageToken, curKey[:32])
			response.NextPageToken = nextPageToken
			break
		}
	}

	s.statsd_c.Increment("list_quarantined_ohashes.ok")
	return serializePb(response)
}

func ListQuarantinedOHash(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.ListQuarantinedOHashRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

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
		reqlog.Errorf("unable to encode object prefix: %v", err)
		s.statsd_c.Increment("list_quarantined_ohash.fail")
		return nil, status.Errorf(codes.Unavailable, "unable to encode object prefix")
	}

	it := s.kv.NewIterator(quarantinePrefix)
	defer it.Close()

	response := &pb.ListQuarantinedOHashReply{}

	// adds one byte because of prefix. Otherwise len(prefix) would be len(prefix)-1
	for it.Seek(prefix); it.Valid() && len(prefix) <= len(it.Key()) && bytes.Equal(prefix, it.Key()[:len(prefix)]); it.Next() {

		// Decode value
		volumeIndex, offset, err := DecodeObjectValue(it.Value())
		if err != nil {
			reqlog.Errorf("failed to decode object value: %v", err)
			s.statsd_c.Increment("list_quarantined_ohash.fail")
			return nil, status.Errorf(codes.Internal, "unable to read object")
		}

		key := make([]byte, 32+len(it.Key()[16:]))
		err = DecodeObjectKey(it.Key(), key)
		if err != nil {
			reqlog.Errorf("failed to decode object key: %v", err)
			s.statsd_c.Increment("list_quarantined_ohash.fail")
			return nil, status.Errorf(codes.Internal, "unable to decode object key")
		}
		response.Objects = append(response.Objects, &pb.Object{Name: key, VolumeIndex: volumeIndex, Offset: offset})
	}

	s.statsd_c.Increment("list_quarantined_ohash.ok")
	return serializePb(response)
}

func GetNextOffset(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.GetNextOffsetRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{"Function": "GetNextOffset", "VolumeIndex": in.VolumeIndex})
	reqlog.Debug("RPC Call")

	if !in.RepairTool && !s.isClean {
		reqlog.Debug("KV out of sync with volumes")
		return nil, status.Errorf(codes.FailedPrecondition, "KV out of sync with volumes")
	}

	key := EncodeVolumeKey(in.VolumeIndex)

	value, err := s.kv.Get(volumePrefix, key)
	if err != nil {
		reqlog.Errorf("unable to retrieve volume key: %v", err)
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
		reqlog.WithFields(logrus.Fields{"value": value}).Errorf("failed to decode volume value: %v", err)
		s.statsd_c.Increment("get_next_offset.fail")
		return nil, status.Errorf(codes.Internal, "failed to decode volume value")
	}

	s.statsd_c.Increment("get_next_offset.ok")
	return serializePb(&pb.GetNextOffsetReply{Offset: uint64(nextOffset)})
}

// GetStats returns stats for the KV. used for initial debugging, remove?
func GetStats(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.GetStatsRequest{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	response := new(pb.GetStatsReply)

	m := CollectStats(s)
	response.Stats = m

	return serializePb(response)
}

// Sets KV state (is in sync with volumes, or not)
func SetKvState(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.KvState{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{"Function": "SetClean", "IsClean": in.IsClean})
	reqlog.Debug("RPC Call")

	s.isClean = in.IsClean
	return serializePb(&pb.SetKvStateReply{})
}

// Gets KV state (is in sync with volumes, or not)
func GetKvState(s *server, ctx context.Context, pbIn *[]byte) (*[]byte, error) {
	in := &pb.KvState{}
	if err := proto.Unmarshal(*pbIn, in); err != nil {
		logrus.Errorf("failed to unmarshal input: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "unable to deserialize protobuf")
	}

	reqlog := log.WithFields(logrus.Fields{"Function": "GetKvState"})
	reqlog.Debug("RPC Call")
	state := new(pb.KvState)
	state.IsClean = s.isClean
	return serializePb(state)
}

// Stops serving RPC requests and closes KV if we receive SIGTERM/SIGINT
func shutdownHandler(s *server, wg *sync.WaitGroup) {
	<-s.stopChan
	rlog := log.WithFields(logrus.Fields{"socket": s.socketPath})
	rlog.Info("Shutting down")

	// Stop serving RPC requests
	// Give it a 5s delay to finish serving active requests, then force close
	rlog.Debug("Stopping RPC")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		// Error or timeout
		rlog.Infof("HTTP server Shutdown: %v", err)
		if err = s.httpServer.Close(); err != nil {
			rlog.Infof("HTTP server Close: %v", err)
		}
	}

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

	if err != nil {
		return
	}
	os.Chmod(socketPath, 0660)

	_, diskName := path.Split(path.Clean(diskPath))
	fs := &server{kv: kv, diskPath: diskPath, diskName: diskName, socketPath: socketPath,
		isClean: isClean, stopChan: stopChan}

	go func() {
		unixListener, err := net.Listen("unix", fs.socketPath)
		if err != nil {
			log.Printf("Cannot serve")
		}
		server := http.Server{Handler: fs}
		fs.httpServer = &server
		server.Serve(unixListener)
	}()

	// Initialize statsd client
	statsdPrefix := "kv"
	fs.statsd_c, err = statsd.New(statsd.Prefix(statsdPrefix))
	if err != nil {
		return
	}

	// Start shutdown handler
	wg.Add(1)
	go shutdownHandler(fs, &wg)
	wg.Wait()

	return
}

var strToFunc = map[string]rpcFunc{
	"/register_volume":          RegisterVolume,
	"/unregister_volume":        UnregisterVolume,
	"/update_volume_state":      UpdateVolumeState,
	"/get_volume":               GetVolume,
	"/list_volumes":             ListVolumes,
	"/register_object":          RegisterObject,
	"/unregister_object":        UnregisterObject,
	"/rename_object":            RenameObject,
	"/load_object":              LoadObject,
	"/quarantine_object":        QuarantineObject,
	"/unquarantine_object":      UnquarantineObject,
	"/load_objects_by_prefix":   LoadObjectsByPrefix,
	"/load_objects_by_volume":   LoadObjectsByVolume,
	"/list_partitions":          ListPartitions,
	"/list_partition":           ListPartition,
	"/list_suffix":              ListSuffix,
	"/list_quarantined_ohashes": ListQuarantinedOHashes,
	"/list_quarantined_ohash":   ListQuarantinedOHash,
	"/get_next_offset":          GetNextOffset,
	"/get_stats":                GetStats,
	"/set_kv_state":             SetKvState,
	"/get_kv_state":             GetKvState,
}

func serializePb(msg proto.Message) (*[]byte, error) {
	out, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("failed to serialize reply: %v", err)
		return nil, status.Errorf(codes.Unavailable, "unable to serialize reply: %v", err)
	}
	return &out, nil
}

func sendError(w http.ResponseWriter, rpcErr error) (err error) {
	w.Header().Set("Content-Type", "Content-Type: text/plain; charset=utf-8")
	w.WriteHeader(int(rpcErr.(*status.RpcError).Code()))
	errorMsg := []byte(rpcErr.Error())
	_, err = w.Write(errorMsg)
	return
}

func sendReply(w http.ResponseWriter, serializedPb []byte) (err error) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(200)
	_, err = w.Write(serializedPb)
	return
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error
	log.Debugf(r.URL.Path)

	// Match URL to RPC function
	fn, ok := strToFunc[r.URL.Path]
	if !ok {
		log.Printf("No match for URL Path %s", r.URL.Path)
		if err = sendError(w, status.Errorf(codes.Unimplemented, "Unimplemented RPC function")); err != nil {
			log.Printf("Error sending reply: %v", err)
		}
		return
	}

	// Read request (body should be serialized protobuf)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		if err = sendError(w, status.Errorf(codes.Internal, "Failed to read request body")); err != nil {
			log.Printf("Error sending reply: %v", err)
		}
		return
	}

	// Call RPC function and send reply
	resp, err := fn(s, r.Context(), &body)
	if err != nil {
		log.Println(err)
		if err = sendError(w, err); err != nil {
			log.Printf("Error sending reply: %v", err)
		}
		return
	}

	if err = sendReply(w, *resp); err != nil {
		log.Printf("Error sending reply: %v", err)
	}
}
