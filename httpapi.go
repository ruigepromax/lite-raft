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
	"context"
	"encoding/gob"
	"go.etcd.io/raft/v3/raftpb"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	node *raftNode
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()
	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(kv{key, string(v)}); err != nil {
			log.Fatal(err)
		}
		if err = h.node.Propose(context.Background(), buf.String()); err != nil {
			log.Fatal(err)
		}
		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if v, err := h.node.SyncRead(context.Background(), key); err == nil {
			s := v.(string)
			w.Write([]byte(s))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "POST":
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: url,
		}
		if err = h.node.ConfChange(context.Background(), cc); err != nil {
			log.Fatal(err)
		}

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		if err = h.node.ConfChange(context.Background(), cc); err != nil {
			log.Fatal(err)
		}

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(node *raftNode, port int) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			node: node,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	if err, ok := <-node.errorC; ok {
		log.Fatal(err)
	}
}
