package tests

import (
	"fmt"
	"testing"
)

func BenchmarkLinearizableKVPut(b *testing.B) {
	kc := newKVCluster(3)
	defer kc.cleanup()

	srv, _ := kc.getLeaderKV()
	if srv == nil {
		b.Fatal("failed to find cluster leader")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		val := fmt.Sprintf("bench_val_%d", i)
		_ = srv.Put("bench-client", int64(i), key, val)
	}
}

func BenchmarkLinearizableKVGet(b *testing.B) {
	kc := newKVCluster(3)
	defer kc.cleanup()

	srv, _ := kc.getLeaderKV()
	if srv == nil {
		b.Fatal("failed to find cluster leader")
	}

	_ = srv.Put("bench-client", 1, "read_hotspot", "static_value")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _ = srv.Get("read_hotspot")
	}
}
