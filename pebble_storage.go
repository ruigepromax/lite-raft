package main

import (
	"encoding/binary"
	"errors"
	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"log"
	"sync"
)

var (
	entryPrefix   = []byte{0x01}
	hardStateKey  = []byte{0x02}
	snapshotKey   = []byte{0x03}
	firstIndexKey = []byte{0x04}
	lastIndexKey  = []byte{0x05}
)

// PebbleStorage implements the raft.Storage interface using Pebble.
type PebbleStorage struct {
	db     *pebble.DB
	logger *zap.Logger
	mu     sync.RWMutex // Protects snapshot and indices

	// Caching for performance
	snapshot   raftpb.Snapshot
	firstIndex uint64
	lastIndex  uint64
}

// NewPebbleStorage creates a new PebbleStorage.
func NewPebbleStorage(dbDir string) (*PebbleStorage, error) {
	// 2. 配置并打开 Pebble 数据库
	db, err := pebble.Open(dbDir, &pebble.Options{})
	if err != nil {
		log.Fatalf("raftexample: could not open pebble db (%v)", err)
	}
	s := &PebbleStorage{
		db:     db,
		logger: zap.L(),
	}

	// Load initial state from Pebble
	if err := s.loadSnapshot(); err != nil {
		return nil, err
	}
	if err := s.loadFirstIndex(); err != nil {
		return nil, err
	}
	if err := s.loadLastIndex(); err != nil {
		return nil, err
	}

	return s, nil
}

// load helpers
func (s *PebbleStorage) loadSnapshot() error {
	val, closer, err := s.db.Get(snapshotKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil // No snapshot yet
	}
	if err != nil {
		return err
	}
	defer closer.Close()

	return s.snapshot.Unmarshal(val)
}

func (s *PebbleStorage) loadFirstIndex() error {
	val, closer, err := s.db.Get(firstIndexKey)
	if errors.Is(err, pebble.ErrNotFound) {
		// If not found, it implies we start from index 1 after the initial empty snapshot
		s.firstIndex = s.snapshot.Metadata.Index + 1
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	s.firstIndex = binary.BigEndian.Uint64(val)
	return nil
}

func (s *PebbleStorage) loadLastIndex() error {
	val, closer, err := s.db.Get(lastIndexKey)
	if errors.Is(err, pebble.ErrNotFound) {
		// If not found, it implies the log is empty
		s.lastIndex = s.snapshot.Metadata.Index
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	s.lastIndex = binary.BigEndian.Uint64(val)
	return nil
}

// InitialState implements the raft.Storage interface.
func (s *PebbleStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hs, err := s.getHardState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}

	return hs, s.snapshot.Metadata.ConfState, nil
}

// Entries implements the raft.Storage interface.
func (s *PebbleStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo < s.firstIndex {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIndex+1 {
		s.logger.Panic("Entries hi is out of bound", zap.Uint64("hi", hi), zap.Uint64("lastIndex", s.lastIndex))
	}

	startKey := s.entryKey(lo)
	endKey := s.entryKey(hi)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var entries []raftpb.Entry
	var totalSize uint64 = 0

	for iter.First(); iter.Valid(); iter.Next() {
		var entry raftpb.Entry
		if err := entry.Unmarshal(iter.Value()); err != nil {
			return nil, err
		}
		totalSize += uint64(entry.Size())
		if len(entries) > 0 && totalSize > maxSize {
			break
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Term implements the raft.Storage interface.
func (s *PebbleStorage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if i < s.firstIndex-1 {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndex {
		return 0, raft.ErrUnavailable
	}
	// Special case for snapshot term
	if i == s.snapshot.Metadata.Index {
		return s.snapshot.Metadata.Term, nil
	}

	key := s.entryKey(i)
	val, closer, err := s.db.Get(key)
	if err != nil {
		return 0, err
	}
	defer closer.Close()

	var entry raftpb.Entry
	if err := entry.Unmarshal(val); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

// LastIndex implements the raft.Storage interface.
func (s *PebbleStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex, nil
}

// FirstIndex implements the raft.Storage interface.
func (s *PebbleStorage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndex, nil
}

// Snapshot implements the raft.Storage interface.
func (s *PebbleStorage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot, nil
}

// --- Write methods ---

// Append appends a slice of entries to the storage.
func (s *PebbleStorage) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	firstNewIndex := entries[0].Index
	if firstNewIndex > s.lastIndex+1 {
		s.logger.Panic("misaligned log append", zap.Uint64("first new index", firstNewIndex), zap.Uint64("last index", s.lastIndex))
	}
	// Truncate conflicting entries if any
	if firstNewIndex <= s.lastIndex {
		s.logger.Info("truncating log", zap.Uint64("from", firstNewIndex), zap.Uint64("to", s.lastIndex))
		if err := s.deleteEntries(firstNewIndex, s.lastIndex+1); err != nil {
			return err
		}
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for i := range entries {
		entry := &entries[i]
		key := s.entryKey(entry.Index)
		data, err := entry.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(key, data, pebble.NoSync); err != nil {
			return err
		}
	}

	// Update last index
	newLastIndex := entries[len(entries)-1].Index
	if err := s.setLastIndex(batch, newLastIndex); err != nil {
		return err
	}

	// Use Sync for durability, equivalent to WAL
	if err := s.db.Apply(batch, pebble.Sync); err != nil {
		return err
	}

	// Update in-memory cache
	s.lastIndex = newLastIndex
	return nil
}

func (s *PebbleStorage) SaveReadyState(rd raft.Ready) error {
	// 如果 Ready 中没有任何需要持久化的内容，直接返回
	if raft.IsEmptyHardState(rd.HardState) && len(rd.Entries) == 0 && raft.IsEmptySnap(rd.Snapshot) {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. 处理和保存 HardState
	if !raft.IsEmptyHardState(rd.HardState) {
		data, err := rd.HardState.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(hardStateKey, data, pebble.NoSync); err != nil {
			return err
		}
	}

	// 2. 处理和保存 Snapshot
	if !raft.IsEmptySnap(rd.Snapshot) {
		// 应用快照的逻辑比较复杂，我们将其封装
		if err := s.applySnapshotInBatch(batch, rd.Snapshot); err != nil {
			return err
		}
	}

	// 3. 处理和保存 Entries
	if len(rd.Entries) > 0 {
		// 追加日志的逻辑也需要封装
		if err := s.appendInBatch(batch, rd.Entries); err != nil {
			return err
		}
	}

	// 4. 原子性地将所有变更写入磁盘
	// pebble.Sync 选项至关重要，它确保数据被刷新到磁盘，提供了与 WAL 相同的持久性保证。
	if err := s.db.Apply(batch, pebble.Sync); err != nil {
		return err
	}

	// 5. 成功写入磁盘后，更新内存中的缓存状态
	if !raft.IsEmptySnap(rd.Snapshot) {
		s.snapshot = rd.Snapshot
		s.firstIndex = rd.Snapshot.Metadata.Index + 1
		if s.lastIndex < rd.Snapshot.Metadata.Index {
			s.lastIndex = rd.Snapshot.Metadata.Index
		}
	}
	if len(rd.Entries) > 0 {
		s.lastIndex = rd.Entries[len(rd.Entries)-1].Index
	}

	return nil
}

// AllEntries retrieves all log entries currently stored in Pebble.
// This is primarily for debugging and testing purposes.
func (s *PebbleStorage) AllEntries() ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 我们使用 entryPrefix (0x01) 作为扫描的下界 (LowerBound)，
	// 使用 hardStateKey (0x02) 作为上界 (UpperBound)。
	// 这样可以确保迭代器只扫描所有以 entryPrefix 开头的键，即所有的日志条目。
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: entryPrefix,
		UpperBound: hardStateKey,
	})
	if err != nil {
		return nil, err
	}
	// 确保迭代器在使用后被关闭，释放资源。
	defer iter.Close()

	var entries []raftpb.Entry

	// 从第一个有效的键开始迭代
	for iter.First(); iter.Valid(); iter.Next() {
		// 创建一个空的 Entry 结构体用于接收反序列化后的数据
		var entry raftpb.Entry

		// iter.Value() 返回的是一个 []byte，需要将其反序列化为 raftpb.Entry 对象。
		// 如果反序列化失败，可能意味着数据损坏。
		if err := entry.Unmarshal(iter.Value()); err != nil {
			return nil, err
		}

		// 将成功解析的日志条目追加到结果切片中
		entries = append(entries, entry)
	}

	// 检查迭代过程中是否发生了错误
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return entries, nil
}

// --- 内部辅助方法，用于在 batch 中执行操作 ---

func (s *PebbleStorage) appendInBatch(batch *pebble.Batch, entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	firstNewIndex := entries[0].Index
	// 截断可能冲突的旧日志
	if firstNewIndex <= s.lastIndex {
		s.logger.Info("truncating log in batch", zap.Uint64("from", firstNewIndex))
		if err := s.deleteEntriesInBatch(batch, firstNewIndex, s.lastIndex+1); err != nil {
			return err
		}
	}

	// 写入新日志
	for i := range entries {
		entry := &entries[i]
		key := s.entryKey(entry.Index)
		data, err := entry.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(key, data, pebble.NoSync); err != nil {
			return err
		}
	}

	// 更新 lastIndex 元数据
	newLastIndex := entries[len(entries)-1].Index
	return s.setLastIndex(batch, newLastIndex)
}

func (s *PebbleStorage) applySnapshotInBatch(batch *pebble.Batch, snap raftpb.Snapshot) error {
	snapIndex := snap.Metadata.Index

	if s.snapshot.Metadata.Index >= snapIndex {
		s.logger.Info("skipping applying old snapshot", zap.Uint64("current_index", s.snapshot.Metadata.Index), zap.Uint64("new_index", snapIndex))
		return raft.ErrSnapOutOfDate
	}

	s.logger.Info("applying snapshot in batch", zap.Uint64("index", snapIndex))

	// 1. 保存快照元数据
	data, err := snap.Marshal()
	if err != nil {
		return err
	}
	if err := batch.Set(snapshotKey, data, pebble.NoSync); err != nil {
		return err
	}

	// 2. 更新日志元数据
	newFirstIndex := snapIndex + 1
	if err := s.setFirstIndex(batch, newFirstIndex); err != nil {
		return err
	}
	if s.lastIndex < snapIndex {
		if err := s.setLastIndex(batch, snapIndex); err != nil {
			return err
		}
	}

	// 3. 删除被快照覆盖的旧日志
	if s.firstIndex < newFirstIndex {
		if err := s.deleteEntriesInBatch(batch, s.firstIndex, newFirstIndex); err != nil {
			return err
		}
	}

	return nil
}

// 你需要将之前独立的 Append 和 ApplySnapshot 方法删除或重构，
// 因为它们的逻辑已经被整合到 SaveReadyState 和其辅助方法中了。

// SetHardState saves the current HardState.
func (s *PebbleStorage) SetHardState(st raftpb.HardState) error {
	data, err := st.Marshal()
	if err != nil {
		return err
	}
	return s.db.Set(hardStateKey, data, pebble.Sync)
}

// getHardState retrieves the HardState.
func (s *PebbleStorage) getHardState() (raftpb.HardState, error) {
	var hs raftpb.HardState
	val, closer, err := s.db.Get(hardStateKey)
	if errors.Is(err, pebble.ErrNotFound) {
		// A new node will not have a HardState, return an empty one.
		return hs, nil
	}
	if err != nil {
		return hs, err
	}
	defer closer.Close()

	if err := hs.Unmarshal(val); err != nil {
		return hs, err
	}
	return hs, nil
}

// ApplySnapshot saves a snapshot and compacts the log.
func (s *PebbleStorage) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.applySnapshot(snap)
}

func (s *PebbleStorage) applySnapshot(snap raftpb.Snapshot) error {
	snapIndex := snap.Metadata.Index
	snapTerm := snap.Metadata.Term

	// Check if we are applying an old snapshot
	if s.snapshot.Metadata.Index >= snapIndex {
		s.logger.Info("skipping applying old snapshot", zap.Uint64("current_index", s.snapshot.Metadata.Index), zap.Uint64("new_index", snapIndex))
		return raft.ErrSnapOutOfDate
	}

	s.logger.Info("applying snapshot", zap.Uint64("index", snapIndex))

	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. Save the snapshot metadata
	data, err := snap.Marshal()
	if err != nil {
		return err
	}
	if err := batch.Set(snapshotKey, data, pebble.NoSync); err != nil {
		return err
	}

	// 2. Update HardState from snapshot
	// This is important because the Commit index might be updated by the snapshot.
	hs, err := s.getHardState()
	if err != nil {
		return err
	}
	if snapIndex > hs.Commit {
		hs.Commit = snapIndex
	}
	if snapTerm > hs.Term {
		hs.Term = snapTerm
	}
	hsData, err := hs.Marshal()
	if err != nil {
		return err
	}
	if err := batch.Set(hardStateKey, hsData, pebble.NoSync); err != nil {
		return err
	}

	// 3. Update log indices
	newFirstIndex := snapIndex + 1
	if err := s.setFirstIndex(batch, newFirstIndex); err != nil {
		return err
	}
	// The last index might also need updating if the snapshot is ahead of the log
	if s.lastIndex < snapIndex {
		if err := s.setLastIndex(batch, snapIndex); err != nil {
			return err
		}
	}

	// 4. Delete compacted entries
	if s.firstIndex < newFirstIndex {
		if err := s.deleteEntriesInBatch(batch, s.firstIndex, newFirstIndex); err != nil {
			return err
		}
	}

	// Apply batch with Sync
	if err := s.db.Apply(batch, pebble.Sync); err != nil {
		return err
	}

	// Update in-memory state
	s.snapshot = snap
	s.firstIndex = newFirstIndex
	if s.lastIndex < snapIndex {
		s.lastIndex = snapIndex
	}

	return nil
}

// CreateSnapshot is a helper for higher-level logic. raft.Storage doesn't require it,
// but it's useful for the application.
func (s *PebbleStorage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if i < s.snapshot.Metadata.Index {
		return raftpb.Snapshot{}, raft.ErrSnapOutOfDate
	}
	if i > s.lastIndex {
		s.logger.Panic("snapshot index is out of bound", zap.Uint64("index", i), zap.Uint64("lastIndex", s.lastIndex))
	}

	// Get term of the snapshot index
	term, err := s.term(i) // internal term access
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	newSnap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     i,
			Term:      term,
			ConfState: *cs,
		},
		Data: data,
	}

	// ApplySnapshot will handle persistence and log cleaning
	if err := s.applySnapshot(newSnap); err != nil {
		return raftpb.Snapshot{}, err
	}

	return newSnap, nil
}

func (s *PebbleStorage) setLastIndex(batch *pebble.Batch, index uint64) error {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, index)
	return batch.Set(lastIndexKey, val, pebble.NoSync)
}

func (s *PebbleStorage) entryKey(index uint64) []byte {
	key := make([]byte, len(entryPrefix)+8)
	copy(key, entryPrefix)
	binary.BigEndian.PutUint64(key[len(entryPrefix):], index)
	return key
}

func (s *PebbleStorage) deleteEntries(lo, hi uint64) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.deleteEntriesInBatch(batch, lo, hi); err != nil {
		return err
	}
	return s.db.Apply(batch, pebble.Sync)
}

// term is the internal unlocked version of Term
func (s *PebbleStorage) term(i uint64) (uint64, error) {
	if i < s.firstIndex-1 {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndex {
		return 0, raft.ErrUnavailable
	}
	if i == s.snapshot.Metadata.Index {
		return s.snapshot.Metadata.Term, nil
	}
	key := s.entryKey(i)
	val, closer, err := s.db.Get(key)
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	var entry raftpb.Entry
	if err := entry.Unmarshal(val); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (s *PebbleStorage) setFirstIndex(batch *pebble.Batch, index uint64) error {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, index)
	return batch.Set(firstIndexKey, val, pebble.NoSync)
}

func (s *PebbleStorage) deleteEntriesInBatch(batch *pebble.Batch, lo, hi uint64) error {
	startKey := s.entryKey(lo)
	endKey := s.entryKey(hi)
	return batch.DeleteRange(startKey, endKey, pebble.NoSync)
}

// Close closes the storage.
func (s *PebbleStorage) Close() error {
	return s.db.Close()
}
