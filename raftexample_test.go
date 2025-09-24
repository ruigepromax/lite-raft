package main

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/raft/v3/raftpb"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

type mockStateMachine struct {
	i int
}

func newMockStateMachine() *mockStateMachine {
	return &mockStateMachine{}
}

func (s *mockStateMachine) Update(data string) error {
	i := s.i
	v, err := strconv.Atoi(data)
	if err != nil {
		return err
	}
	if v != i+1 {
		return errors.New("update data is not equals i + 1")
	}

	s.i = v
	return nil
}

func (s *mockStateMachine) Lookup(key interface{}) (interface{}, error) {
	return s.i, nil
}

func (s *mockStateMachine) GetSnapshot() ([]byte, error) {
	str := strconv.Itoa(s.i)
	b := []byte(str)
	return b, nil
}

func (s *mockStateMachine) RecoverFromSnapshot(snapshot raftpb.Snapshot) error {
	data := snapshot.Data
	str := string(data)
	num, err := strconv.Atoi(str)
	if err != nil {
		return err
	}
	s.i = num
	return nil
}

type cluster struct {
	t     *testing.T
	peers []string
	nodes []*raftNode
}

// newCluster creates a cluster containing n nodes.
func newCluster(t *testing.T, n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 20000+i)
	}

	clus := &cluster{
		t:     t,
		peers: peers,
		nodes: make([]*raftNode, n),
	}

	for i := 0; i < n; i++ {
		msm := newMockStateMachine()

		clus.nodes[i] = newRaftNode(i+1, peers, false, msm)
	}

	// Wait for the cluster to elect a leader. In a healthy cluster, this typically takes a few heartbeat cycles.
	if n > 1 {
		<-time.After(time.Second * 1)
	}

	return clus
}

// Close shuts down all cluster nodes and cleans up resources.
func (clus *cluster) Close() {
	for i, n := range clus.nodes {
		if n != nil {
			n.stop()
		}
		pebbleDir := fmt.Sprintf("raftexample-%d.pebble", i+1)
		if err := os.RemoveAll(pebbleDir); err != nil {
			clus.t.Logf("Failed to remove pebble directory %s: %v", pebbleDir, err)
		}
	}
}

func (clus *cluster) closeNoClean() {
	for _, n := range clus.nodes {
		if n != nil {
			n.stop()
		}
	}
}

// closeNoErrors is a helper function used to close the cluster and handle errors at the end of testing.
func (clus *cluster) closeNoErrors(t *testing.T) {
	t.Log("closing cluster...")
	clus.Close()
	t.Log("closing cluster [done]")
}

// findLeader finds the index of the leader node in the current cluster.
func (clus *cluster) findLeader() int {
	for i := 0; i < 10; i++ {
		for j, n := range clus.nodes {
			if n != nil && n.node != nil && n.node.Status().Lead == uint64(n.id) {
				return j
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return -1 // no leader
}

func (clus *cluster) allSyncReadData(t *testing.T, ch chan interface{}) {
	var wg sync.WaitGroup
	for i := 0; i < len(clus.nodes); i++ {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			node := clus.nodes[nodeIdx]
			data, err := node.SyncRead(context.Background(), "")
			if err != nil {
				t.Errorf("Failed to sync read: %s", err)
			}
			ch <- data
		}(i)
	}
	wg.Wait()
	close(ch)
}

// TestProposeOnCommit starts three nodes and calls the propose method to propose log entries.
func TestProposeOnCommit(t *testing.T) {
	num := 100
	nodeNum := 3

	ch := make(chan string, num)
	clus := newCluster(t, nodeNum)
	defer clus.closeNoErrors(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for s := range ch {
			node := clus.nodes[i]
			err := node.Propose(context.Background(), s)
			if err != nil {
				t.Errorf("Failed to propose: %s", err)
			}
			i = (i + 1) % nodeNum
		}
	}()

	leaderIdx := clus.findLeader()
	if leaderIdx == -1 {
		t.Fatal("Failed to find a leader")
	}
	for i := 1; i <= num; i++ {
		ch <- strconv.Itoa(i)
	}
	close(ch)
	wg.Wait()
	for i := 0; i < nodeNum; i++ {
		data, err := clus.nodes[i].SyncRead(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		gotValue := data.(int)
		if gotValue != num {
			t.Fatalf("expect %v, got %v", num, gotValue)
		}
	}

}

func TestProposeAndSyncRead(t *testing.T) {
	num := 100
	nodeNum := 3

	ch := make(chan string, num)
	clus := newCluster(t, nodeNum)
	defer clus.closeNoErrors(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for s := range ch {
			node := clus.nodes[i]
			expect, _ := strconv.Atoi(s)

			err := node.Propose(context.Background(), s)
			if err != nil {
				t.Errorf("Failed to propose: %s", err)
			}
			readCh := make(chan interface{}, nodeNum)
			clus.allSyncReadData(t, readCh)
			for data := range readCh {
				gotValue := data.(int)
				if gotValue != expect {
					t.Errorf("expect %v, got %v", expect, gotValue)
				}
			}

			i = (i + 1) % nodeNum
		}
	}()

	leaderIdx := clus.findLeader()
	if leaderIdx == -1 {
		t.Fatal("Failed to find a leader")
	}
	for i := 1; i <= num; i++ {
		ch <- strconv.Itoa(i)
	}
	close(ch)
	wg.Wait()

}

func TestSnapshot(t *testing.T) {
	clus := newCluster(t, 1)
	node := clus.nodes[0]
	num := 100

	for i := 1; i <= num; i++ {
		v := strconv.Itoa(i)
		err := node.Propose(context.Background(), v)
		if err != nil {
			t.Errorf("Failed to propose: %s", err)
		}
	}
	// triggerSnapshot
	node.snapshotCatchUpEntries = uint64(num)
	node.maybeTriggerSnapshot()

	clus.closeNoClean()

	//node.stop()
	clus = newCluster(t, 1)
	defer clus.closeNoErrors(t)
	<-time.After(time.Second * 3)

	node = clus.nodes[0]

	data, err := node.SyncRead(context.Background(), "")
	if err != nil {
		t.Errorf("Failed to read: %s", err)
	}
	gotValue := data.(int)
	if expect := num; gotValue != expect {
		t.Errorf("expect %v, got %v", expect, gotValue)
	}

}

func TestAddNewNode(t *testing.T) {
	clus := newCluster(t, 3)
	defer clus.closeNoErrors(t)
	defer func() {
		pebbleDir := fmt.Sprintf("raftexample-%d.pebble", 4)
		if err := os.RemoveAll(pebbleDir); err != nil {
			clus.t.Logf("Failed to remove pebble directory %s: %v", pebbleDir, err)
		}
	}()

	num := 100
	node := clus.nodes[0]
	for i := 1; i <= num; i++ {
		v := strconv.Itoa(i)
		err := node.Propose(context.Background(), v)
		if err != nil {
			t.Errorf("Failed to propose: %s", err)
		}
	}

	newNodeURL := "http://127.0.0.1:10004"
	confChange := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  4,
		Context: []byte(newNodeURL),
	}
	err := node.ConfChange(context.Background(), confChange)
	if err != nil {
		t.Fatalf("Failed to confChange: %v", err)
	}

	msm := newMockStateMachine()
	node = newRaftNode(4, append(clus.peers, newNodeURL), true, msm)
	<-time.After(time.Second * 3)

	data, err := node.SyncRead(context.Background(), "")
	if err != nil {
		t.Errorf("Failed to read: %s", err)
	}
	gotValue := data.(int)
	if expect := num; gotValue != expect {
		t.Errorf("expect %v, got %v", expect, gotValue)
	}
	err = node.Propose(context.Background(), strconv.Itoa(num+1))
	if err != nil {
		t.Errorf("Failed to propose: %s", err)
	}
}
