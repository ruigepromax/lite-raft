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
	"errors"
	"fmt"
	"github.com/google/uuid"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/zap"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var snapshotCatchUpEntries uint64 = 1000

var requestTimeout = 5 * time.Second

type commit struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot // 应用快照
}

type proposeRequest struct {
	Id   uint64
	Data string
}
type notifier struct {
	c   chan struct{}
	err error
}

func newNotifier() *notifier {
	return &notifier{
		c: make(chan struct{}),
	}
}

func (nc *notifier) notify(err error) {
	nc.err = err
	close(nc.c)
}

// A key-value stream backed by raft
type raftNode struct {
	proposeC    chan string  // proposed messages (k,v)
	dataCommitC chan *commit // entries committed to log (k,v)
	errorC      chan error   // errors from raft session
	readStateC  chan raft.ReadState

	id    int      // client ID for raft session
	peers []string // raft peer URLs
	join  bool     // node is joining an existing cluster

	confState              raftpb.ConfState
	snapshotIndex          uint64
	appliedIndex           uint64
	snapshotCatchUpEntries uint64
	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *PebbleStorage
	sm          *StateMachine

	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
	stopOnce  sync.Once

	readWaitc    chan struct{}
	readNotifier *notifier
	readMu       sync.RWMutex
	applyWait    wait.WaitTime
	w            wait.Wait

	logger *zap.Logger
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(id int, peers []string, join bool, msm MemoryStateMachine) *raftNode {
	errorC := make(chan error)
	pebbleDir := fmt.Sprintf("raftexample-%d.pebble", id)
	storage, err := NewPebbleStorage(pebbleDir)
	if err != nil {
		panic(err)
	}

	rc := &raftNode{
		proposeC:               make(chan string),
		dataCommitC:            make(chan *commit),
		readStateC:             make(chan raft.ReadState, 1),
		errorC:                 errorC,
		id:                     id,
		peers:                  peers,
		join:                   join,
		raftStorage:            storage,
		stopc:                  make(chan struct{}),
		readWaitc:              make(chan struct{}),
		readNotifier:           newNotifier(),
		applyWait:              wait.NewTimeList(),
		w:                      wait.New(),
		httpstopc:              make(chan struct{}),
		httpdonec:              make(chan struct{}),
		snapshotCatchUpEntries: snapshotCatchUpEntries,
		sm:                     NewStateMachine(msm),

		logger: zap.NewExample(),
	}
	rc.startRaft()

	return rc
}

func (rc *raftNode) Propose(ctx context.Context, s string) error {
	id := uint64(uuid.New().ID())
	var data bytes.Buffer

	if err := gob.NewEncoder(&data).Encode(proposeRequest{Id: id, Data: s}); err != nil {
		log.Fatal(err)
	}

	cctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	ch := rc.w.Register(id)
	err := rc.node.Propose(cctx, data.Bytes())
	if err != nil {
		rc.w.Trigger(id, nil) // GC wait
		return err
	}

	select {
	case <-ch:
		return nil
	case <-cctx.Done():
		rc.w.Trigger(id, nil) // GC wait
		return ctx.Err()
	case <-rc.stopc:
		return ErrStopped
	}
}

func (rc *raftNode) ConfChange(ctx context.Context, conf raftpb.ConfChange) error {
	conf.ID = uint64(uuid.New().ID())
	ch := rc.w.Register(conf.ID)
	if err := rc.node.ProposeConfChange(ctx, conf); err != nil {
		rc.w.Trigger(conf.ID, nil)
		return err
	}

	select {
	case <-ch:
		rc.logger.Info("applied a configuration change through raft")
		return nil
	case <-ctx.Done():
		rc.w.Trigger(conf.ID, nil)
		return ctx.Err()
	case <-rc.stopc:
		return ErrStopped
	}
}

func (rc *raftNode) SyncRead(ctx context.Context, query interface{}) (interface{}, error) {
	return rc.linearizableRead(ctx, func() (interface{}, error) {
		return rc.sm.LookUp(query)
	})
}

func (rc *raftNode) linearizableRead(ctx context.Context, f func() (interface{}, error)) (interface{}, error) {
	err := rc.linearizableReadNotify(ctx)
	if err != nil {
		return nil, err
	}

	//TODO: Add timeout control
	return f()
}

func (rc *raftNode) linearizableReadNotify(ctx context.Context) error {
	rc.readMu.RLock()
	nc := rc.readNotifier
	rc.readMu.RUnlock()

	select {
	case rc.readWaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.c:
		return nc.err
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.stopc:
		return ErrStopped
	}
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) (applyIndex uint64, shouldStop bool) {
	if len(ents) == 0 {
		return
	}

	data := make([]string, 0, len(ents))
	w := make([]uint64, 0, len(ents))
	for i := range ents {
		e := ents[i]
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			var rq proposeRequest
			dec := gob.NewDecoder(bytes.NewBuffer(e.Data))
			if err := dec.Decode(&rq); err != nil {
				log.Fatalf("could not decode proposeRequest (%v)", err)
			}
			data = append(data, rq.Data)
			w = append(w, rq.Id)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					shouldStop = true
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
			rc.w.Trigger(cc.ID, struct{}{})
		}
	}

	applyIndex = ents[len(ents)-1].Index
	if len(data) > 0 {
		rc.applyEntryToMsm(data, applyIndex)
		for _, id := range w {
			rc.w.Trigger(id, struct{}{})
		}
	}

	return
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.stopc)
	rc.errorC <- err

	rc.node.Stop()
}

func (rc *raftNode) applyCommit() {
	snapshot, err := rc.raftStorage.Snapshot()
	if err != nil {
		log.Fatalf("failed to get snapshot from storage: %v", err)
	}
	//recover from snapshot
	rc.applySnapshot(snapshot)

	for {
		select {
		case dataCommit := <-rc.dataCommitC:
			rc.applyAll(dataCommit)
		case err, ok := <-rc.errorC:
			if ok {
				log.Fatal(err)
			}
		case <-rc.stopc:
			return
		}
	}
}

func (rc *raftNode) applyAll(dataCommit *commit) {
	rc.applySnapshot(dataCommit.snapshot)

	rc.applyEntries(dataCommit.entries)

	rc.applyWait.Trigger(rc.appliedIndex)
	// Trigger snapshot after a certain number of writes
	rc.maybeTriggerSnapshot()

}

func (rc *raftNode) applyEntries(entries []raftpb.Entry) {
	nents := rc.entriesToApply(entries)
	if len(nents) == 0 {
		return
	}

	var shouldStop bool
	if rc.appliedIndex, shouldStop = rc.publishEntries(nents); shouldStop {
		go rc.stop()
	}

}

func (rc *raftNode) applyEntryToMsm(entries []string, index uint64) {
	if len(entries) == 0 && index == rc.appliedIndex {
		// This may be an empty commit (e.g., during a Raft leader change), no action required.
		return
	}

	log.Printf("Applying %d entries up to index %d", len(entries), index)

	for i, data := range entries {
		if err := rc.sm.Update(data); err != nil {
			log.Fatalf("State machine failed to apply entry at index %d: %v", rc.appliedIndex+uint64(i+1), err)
		}
	}
}

func (rc *raftNode) applySnapshot(snapshot raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshot) {
		return
	}

	if snapshot.Metadata.Index <= rc.appliedIndex {
		log.Printf("Skipping snapshot at index %d (already applied). Current appliedIndex: %d", snapshot.Metadata.Index, rc.appliedIndex)
		return
	}

	log.Printf("Applying snapshot at index %d...", snapshot.Metadata.Index)

	err := rc.sm.RecoverFromSnapshot(snapshot)
	if err != nil {
		log.Fatalf("apply snapshot err from commit snapshot")
	}

	rc.confState = snapshot.Metadata.ConfState
	rc.appliedIndex = snapshot.Metadata.Index
	rc.snapshotIndex = snapshot.Metadata.Index

	log.Printf("Finished applying snapshot. Current appliedIndex: %d", rc.appliedIndex)

}

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapshotCatchUpEntries {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.sm.GetSnapshot()
	if err != nil {
		log.Panic(err)
	}
	_, err = rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}

	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) startRaft() {
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	hs, _, err := rc.raftStorage.InitialState()
	if err != nil {
		log.Fatalf("failed to get initial state from storage: %v", err)
	}

	if !raft.IsEmptyHardState(hs) || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	go rc.serveRaft()
	go rc.serveChannels()
	go rc.applyCommit()
	go rc.linearizableReadLoop()

}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopOnce.Do(func() {
		rc.stopHTTP()
		close(rc.stopc)
		rc.node.Stop()
		rc.raftStorage.Close()
	})
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) serveChannels() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	internalTimeout := time.Second

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if len(rd.ReadStates) != 0 {
				select {
				case rc.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-time.After(internalTimeout):
					log.Fatalln("timed out sending read state", zap.Duration("timeout", internalTimeout))
				case <-rc.stopc:
					return
				}
			}

			err := rc.raftStorage.SaveReadyState(rd)
			if err != nil {
				panic("append fail")
				return
			}
			rc.dataCommitC <- &commit{
				snapshot: rd.Snapshot,
				entries:  rd.CommittedEntries,
			}
			rc.transport.Send(rd.Messages)

			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			return
		}
	}
}

func (rc *raftNode) linearizableReadLoop() {
	for {
		requestId := uuid.New().String()
		select {
		case <-rc.readWaitc:
		case <-rc.stopc:
			return
		}

		nextnr := &notifier{
			c: make(chan struct{}),
		}
		rc.readMu.Lock()
		nr := rc.readNotifier
		rc.readNotifier = nextnr
		rc.readMu.Unlock()

		confirmedIndex, err := rc.requestCurrentIndex(requestId)
		if errors.Is(err, ErrStopped) {
			return
		}

		if err != nil {
			nr.notify(err)
			continue
		}

		appliedIndex := rc.getAppliedIndex()
		if appliedIndex < confirmedIndex {
			select {
			case <-rc.applyWait.Wait(confirmedIndex):
			case <-rc.stopc:
				return
			}
		}

		// unblock all l-reads requested at indices before confirmedIndex
		nr.notify(nil)
	}
}

func (rc *raftNode) requestCurrentIndex(requestId string) (uint64, error) {
	// 发送read index请求
	cctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	err := rc.node.ReadIndex(cctx, []byte(requestId))
	cancel()
	if err != nil {
		return 0, err
	}

	// 等待读请求处理完成并返回结果
	select {
	case state, ok := <-rc.readStateC:
		if !ok {
			return 0, ErrAborted
		}
		return state.Index, nil
	case <-rc.stopc:
		return 0, ErrStopped
	}
	//TODO case: time out

}

func (rc *raftNode) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&rc.appliedIndex)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
