package main

import (
	"go.etcd.io/raft/v3/raftpb"
	"log"
	"testing"
)

func BenchmarkWrite1000EntryBatch1000(b *testing.B) { benchmarkWriteEntry(b, 100, 1000) }

func benchmarkWriteEntry(b *testing.B, size int, batch int) {
	dir := b.TempDir()

	storage, err := NewPebbleStorage(dir)
	if err != nil {
		b.Fatalf("failed to create PebbleStorage: %v", err)
	}
	b.Cleanup(func() {
		if err := storage.Close(); err != nil {
			log.Printf("failed to close pebble db: %v", err)
		}
	})

	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i)
	}
	e := &raftpb.Entry{Data: data}

	entries := make([]raftpb.Entry, 0)

	b.ResetTimer()
	n := 0
	b.SetBytes(int64(e.Size()))
	for i := 0; i < b.N; i++ {
		entries = append(entries, *e)
		n++
		if n > batch {
			err := storage.Append(entries)
			if err != nil {
				b.Fatal(err)
			}
			entries = entries[:0]
			n = 0
		}
	}
}
