package taskrt

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPriorityNoStarvation 验证老化机制：持续涌入的高优先级任务
// 不得完全饿死先到的低优先级任务。
func TestPriorityNoStarvation(t *testing.T) {
	rt := New(Config{
		MaxConcurrency: 1, // 串行执行，制造竞争
		QueueCapacity:  4096,
		AgingInterval:  20 * time.Millisecond,
	})
	defer rt.Shutdown(context.Background())

	// 阻塞闸：先占住唯一执行槽，让后续任务排队。
	gate := make(chan struct{})
	first, err := rt.Submit(Task{ID: "blocker", Priority: 9, Func: func(ctx context.Context) (any, error) {
		<-gate
		return nil, nil
	}})
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	// 等 blocker 进入执行态。
	deadline := time.Now().Add(2 * time.Second)
	for rt.Metrics().Running == 0 {
		if time.Now().After(deadline) {
			t.Fatal("blocker 未进入执行态")
		}
		time.Sleep(time.Millisecond)
	}

	// 低优先级任务先入队。
	low, err := rt.Submit(Task{ID: "low", Priority: 0, Func: func(ctx context.Context) (any, error) {
		return "low-done", nil
	}})
	if err != nil {
		t.Fatalf("submit low: %v", err)
	}

	// 持续提交高优先级任务，试图饿死 low。
	stop := make(chan struct{})
	var highWg sync.WaitGroup
	highWg.Add(1)
	go func() {
		defer highWg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			h, err := rt.Submit(Task{ID: "high", Priority: 5, Func: func(ctx context.Context) (any, error) {
				time.Sleep(5 * time.Millisecond) // 每个高优任务占用 5ms，给老化留出窗口
				return nil, nil
			}})
			if err == nil {
				h.Wait()
			}
			i++
		}
	}()

	close(gate) // 放行，调度开始
	first.Wait()

	select {
	case <-low.Done():
		res := low.Wait()
		t.Logf("输入: 低优先级任务( prio=0 )先入队, 高优先级任务( prio=5 )持续涌入, 并发=1, 老化间隔=20ms")
		t.Logf("判定: 低优先级任务在 5s 内完成, status=%s value=%v", res.Status, res.Value)
		if res.Status != StatusCompleted {
			t.Fatalf("低优先级任务终态异常: %s", res.Status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("低优先级任务被饿死：5s 内未获得执行")
	}
	close(stop)
	highWg.Wait()
}

// TestGroupQuotaNotExceeded 验证分组并发配额：任何时刻分组的
// 并发执行数不得超过配额，且其他分组不受拖累。
func TestGroupQuotaNotExceeded(t *testing.T) {
	rt := New(Config{
		MaxConcurrency: 8,
		QueueCapacity:  256,
		GroupQuotas:    map[string]int{"limited": 2},
	})
	defer rt.Shutdown(context.Background())

	var curLimited, maxLimited atomic.Int64
	var wg sync.WaitGroup
	track := func(group string) func(ctx context.Context) (any, error) {
		return func(ctx context.Context) (any, error) {
			if group == "limited" {
				cur := curLimited.Add(1)
				for {
					old := maxLimited.Load()
					if cur <= old || maxLimited.CompareAndSwap(old, cur) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				curLimited.Add(-1)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
			return nil, nil
		}
	}

	var handles []*Handle
	for i := 0; i < 20; i++ {
		h, err := rt.Submit(Task{ID: "lim", Group: "limited", Func: track("limited")})
		if err != nil {
			t.Fatalf("submit limited: %v", err)
		}
		handles = append(handles, h)
	}
	for i := 0; i < 10; i++ {
		h, err := rt.Submit(Task{ID: "free", Group: "free", Func: track("free")})
		if err != nil {
			t.Fatalf("submit free: %v", err)
		}
		handles = append(handles, h)
	}
	for _, h := range handles {
		wg.Add(1)
		go func(h *Handle) { defer wg.Done(); h.Wait() }(h)
	}
	wg.Wait()

	t.Logf("输入: limited 组 20 个任务(配额=2), free 组 10 个任务(不限), 全局并发=8")
	t.Logf("判定: limited 组观测到的最大并发 = %d", maxLimited.Load())
	if maxLimited.Load() > 2 {
		t.Fatalf("分组配额被越额执行: 最大并发 %d > 配额 2", maxLimited.Load())
	}
	s := rt.Metrics()
	if s.Completed != 30 {
		t.Fatalf("Completed=%d, 应为 30（配额只限速不丢任务）", s.Completed)
	}
}
