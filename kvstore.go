// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"go.etcd.io/raft/v3/raftpb"
	"log"
)

var _ MemoryStateMachine = (*kvstore)(nil)

// a key-value store backed by raft
type kvstore struct {
	mockKvStore map[string]string // current committed key-value pairs
}

type kv struct {
	Key string
	Val string
}

func newKVStore() *kvstore {
	return &kvstore{mockKvStore: make(map[string]string)}
}

func (s *kvstore) Update(data string) error {
	var dataKv kv
	dec := gob.NewDecoder(bytes.NewBufferString(data))
	if err := dec.Decode(&dataKv); err != nil {
		log.Fatalf("raftexample: could not decode message (%v)", err)
	}
	s.mockKvStore[dataKv.Key] = dataKv.Val

	return nil
}

func (s *kvstore) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		log.Panic("err query type")
		return nil, nil
	}
	v, ok := s.mockKvStore[key]
	return v, nil
}

func (s *kvstore) GetSnapshot() ([]byte, error) {
	return json.Marshal(s.mockKvStore)
}

func (s *kvstore) RecoverFromSnapshot(snapshot raftpb.Snapshot) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot.Data, &store); err != nil {
		return err
	}
	s.mockKvStore = store
	return nil
}
