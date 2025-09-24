package main

import (
	"go.etcd.io/raft/v3/raftpb"
	"sync"
)

type MemoryStateMachine interface {
	Lookup(interface{}) (interface{}, error)
	Update(string) error
	GetSnapshot() ([]byte, error)
	RecoverFromSnapshot(raftpb.Snapshot) error
}

type StateMachine struct {
	msm     MemoryStateMachine
	aborted bool
	mu      sync.RWMutex
}

func NewStateMachine(msm MemoryStateMachine) *StateMachine {
	return &StateMachine{msm: msm}
}

func (sm *StateMachine) LookUp(query interface{}) (interface{}, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.aborted {
		return nil, ErrAborted
	}

	return sm.msm.Lookup(query)
}

func (sm *StateMachine) Update(data string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.aborted {
		return ErrAborted
	}

	return sm.msm.Update(data)
}

func (sm *StateMachine) GetSnapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.aborted {
		return nil, ErrAborted
	}

	return sm.msm.GetSnapshot()
}

func (sm *StateMachine) RecoverFromSnapshot(snapshot raftpb.Snapshot) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.aborted {
		return ErrAborted
	}

	return sm.msm.RecoverFromSnapshot(snapshot)
}
