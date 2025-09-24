package main

import (
	"log"
	"reflect"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

// newTestPebbleStorage 是一个辅助函数，用于创建一个用于测试的 PebbleStorage 实例。
// 它会创建一个临时目录来存放 Pebble 数据库，并在测试结束后自动清理。
func newTestPebbleStorage(t *testing.T) *PebbleStorage {
	t.Helper()

	// t.TempDir() 创建一个临时目录，并返回其路径。
	// Go 的测试框架会在测试完成后自动删除这个目录及其内容。
	dir := t.TempDir()

	storage, err := NewPebbleStorage(dir)
	if err != nil {
		t.Fatalf("failed to create PebbleStorage: %v", err)
	}
	// 在测试结束时关闭数据库
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			log.Printf("failed to close pebble db: %v", err)
		}
	})

	return storage
}

// TestPebbleStorage_AppendAndRead 测试 Append 方法后，
// 能否通过 AllEntries 和 Entries 方法正确地读出数据。
func TestPebbleStorage_AppendAndRead(t *testing.T) {
	// --- Arrange (准备) ---
	storage := newTestPebbleStorage(t)

	// 准备要追加的测试日志条目
	testEntries := []raftpb.Entry{
		{Term: 1, Index: 1, Data: []byte("entry-1")},
		{Term: 1, Index: 2, Data: []byte("entry-2")},
		{Term: 2, Index: 3, Data: []byte("entry-3")},
		{Term: 2, Index: 4, Data: []byte("entry-4")},
	}

	// --- Act (执行) ---
	err := storage.Append(testEntries)
	if err != nil {
		t.Fatalf("Append() failed: %v", err)
	}

	// --- Assert (断言/验证) ---

	// 1. 使用 AllEntries() 验证
	t.Run("Verify with AllEntries", func(t *testing.T) {
		all, err := storage.AllEntries()
		if err != nil {
			t.Fatalf("AllEntries() failed: %v", err)
		}

		// 验证读出的日志数量是否正确
		if len(all) != len(testEntries) {
			t.Errorf("expected %d entries from AllEntries, but got %d", len(testEntries), len(all))
		}

		// 使用 reflect.DeepEqual 深度比较内容是否完全一致
		if !reflect.DeepEqual(testEntries, all) {
			t.Errorf("AllEntries() returned unexpected entries.\n got: %+v\nwant: %+v", all, testEntries)
		}
	})

	// 2. 使用 Entries() 验证不同范围的读取
	t.Run("Verify with Entries", func(t *testing.T) {
		testCases := []struct {
			name        string
			lo, hi      uint64
			maxSize     uint64
			wantEntries []raftpb.Entry
		}{
			{
				name:        "read all entries",
				lo:          1,
				hi:          5, // hi is exclusive, so [1, 5) means indexes 1, 2, 3, 4
				maxSize:     1024 * 1024,
				wantEntries: testEntries,
			},
			{
				name:        "read a partial slice of entries",
				lo:          2,
				hi:          4, // Indexes 2, 3
				maxSize:     1024 * 1024,
				wantEntries: testEntries[1:3], // testEntries[1] and testEntries[2]
			},
			{
				name:        "read just the last entry",
				lo:          4,
				hi:          5, // Index 4
				maxSize:     1024 * 1024,
				wantEntries: testEntries[3:],
			},
			{
				name: "read with size limit",
				lo:   1,
				hi:   5,
				// 设置一个只能容纳前两个条目的大小限制
				maxSize:     uint64(testEntries[0].Size() + testEntries[1].Size()),
				wantEntries: testEntries[0:2],
			},
			{
				name:        "read an empty range",
				lo:          5,
				hi:          5,
				maxSize:     1024 * 1024,
				wantEntries: []raftpb.Entry{}, // 期望得到空切片
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				gotEntries, err := storage.Entries(tc.lo, tc.hi, tc.maxSize)
				if err != nil {
					t.Fatalf("Entries() failed for range [%d, %d): %v", tc.lo, tc.hi, err)
				}

				// 如果期望结果是 nil，需要特殊处理，因为 DeepEqual(nil, []T{}) 是 false
				if len(tc.wantEntries) == 0 && len(gotEntries) == 0 {
					return // Success
				}

				if !reflect.DeepEqual(tc.wantEntries, gotEntries) {
					t.Errorf("Entries() for range [%d, %d) returned unexpected entries.\n got: %+v\nwant: %+v", tc.lo, tc.hi, gotEntries, tc.wantEntries)
				}
			})
		}
	})
}
