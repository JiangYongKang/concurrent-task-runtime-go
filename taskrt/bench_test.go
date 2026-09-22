package taskrt

import (
	"context"
	"testing"
	"time"
)

// BenchmarkMixedWorkload 大量短任务与少量长任务混合下的调度开销。
func BenchmarkMixedWorkload(b *testing.B) {
	rt := New(Config{Workers: 8, QueueCapacity: 4096})
	defer rt.Shutdown(context.Background())
	long := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return nil
		}
	}
	short := func(ctx context.Context) error { return nil }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn := short
		if i%1000 == 0 { // 千分之一长任务
			fn = long
		}
		h, err := rt.Submit(Task{Priority: Priority(i % 3), Func: fn})
		if err != nil {
			b.Fatal(err)
		}
		if i%1000 == 0 {
			<-h.Done() // 防止队列被长任务占满导致基准失真
		}
	}
}

// TestMixedWorkloadThroughput 混合负载下短任务吞吐不得被长任务拖垮。
func TestMixedWorkloadThroughput(t *testing.T) {
	rt := New(Config{Workers: 4, QueueCapacity: 8192})
	defer shutdownRt(rt)

	// 少量长任务占住部分 worker。
	var longs []*Handle
	for i := 0; i < 2; i++ {
		h, err := rt.Submit(Task{Priority: PriorityLow, Func: func(ctx context.Context) error {
			time.Sleep(300 * time.Millisecond)
			return nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		longs = append(longs, h)
	}
	const n = 2000
	handles := make([]*Handle, 0, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		h, err := rt.Submit(Task{Priority: Priority(i % 3), Func: func(ctx context.Context) error { return nil }})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	for _, h := range handles {
		waitResult(t, h)
	}
	elapsed := time.Since(start)
	for _, h := range longs { // 等待长任务收尾后再核对统计
		waitResult(t, h)
	}
	t.Logf("输入: 2个300ms长任务 + %d个短任务, workers=4", n)
	t.Logf("判定依据: 短任务全部完成耗时=%v, 应远小于串行上界且不被长任务阻塞(阈值3s)", elapsed)
	if elapsed > 3*time.Second {
		t.Fatalf("混合负载下吞吐退化: %v", elapsed)
	}
	if got := rt.Stats().Completed; got != int64(n+2) {
		t.Fatalf("completed=%d, want %d", got, n+2)
	}
}
