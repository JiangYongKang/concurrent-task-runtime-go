package taskrt

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestPauseResumeBasic 验证分组暂停/恢复核心语义：
//   - 暂停只影响该分组的“启动决策”，已在执行的任务自然结束；
//   - 暂停期间该分组排队任务不启动，其他分组照常调度、不被拖累；
//   - 恢复后排队任务重新参与调度，并遵守恢复当时生效的配额。
func TestPauseResumeBasic(t *testing.T) {
	rt := New(Config{
		MaxConcurrency: 4,
		QueueCapacity:  64,
	})
	defer rt.Shutdown(context.Background())

	var aRan, aMax, aCur atomic.Int64
	var bDone atomic.Int64
	var phase atomic.Int32
	gates := []chan struct{}{make(chan struct{}), make(chan struct{})}

	aFn := func(ctx context.Context) (any, error) {
		p := phase.Load()
		n := aCur.Add(1)
		for {
			old := aMax.Load()
			if n <= old || aMax.CompareAndSwap(old, n) {
				break
			}
		}
		<-gates[p]
		aCur.Add(-1)
		aRan.Add(1)
		return nil, nil
	}

	// 1 个 a 组任务先占一个执行槽。
	arunning, err := rt.Submit(Task{ID: "a-run", Group: "a", Func: aFn})
	if err != nil {
		t.Fatalf("submit a-run: %v", err)
	}
	waitForRunning(t, rt, 1, "a-run 进入执行")

	// 暂停 a 组。
	if err := rt.PauseGroup("a"); err != nil {
		t.Fatalf("PauseGroup(a): %v", err)
	}
	if !rt.IsGroupPaused("a") {
		t.Fatalf("IsGroupPaused(a) 应为 true")
	}
	t.Logf("输入: a 组 1 任务在执行时暂停 a, 再提交 3 个 a 任务与 5 个 b 组任务")

	// 暂停期间提交 3 个 a 任务：必须排队、不得启动。
	var aHandles []*Handle
	for i := 0; i < 3; i++ {
		h, err := rt.Submit(Task{ID: "a-wait", Group: "a", Func: aFn})
		if err != nil {
			t.Fatalf("submit a-wait %d: %v", i, err)
		}
		aHandles = append(aHandles, h)
	}
	// 其他分组 b 提交 5 个短任务：应照常全部完成，不受 a 暂停拖累。
	for i := 0; i < 5; i++ {
		h, err := rt.Submit(Task{ID: "b", Group: "b", Func: func(ctx context.Context) (any, error) {
			bDone.Add(1)
			return nil, nil
		}})
		if err != nil {
			t.Fatalf("submit b %d: %v", i, err)
		}
		h.Wait()
	}

	if bDone.Load() != 5 {
		t.Fatalf("暂停期间 b 组仅完成 %d/5", bDone.Load())
	}
	s := rt.Metrics()
	t.Logf("判定: 暂停期间 b 组完成=%d, a 组 Running=%d(在执行任务保留) Queued=%d",
		bDone.Load(), s.Running, s.Queued)
	if s.Running != 1 || s.Queued != 3 {
		t.Fatalf("暂停 a 期间: Running=%d Queued=%d, 期望 1/3", s.Running, s.Queued)
	}

	// a 组在执行任务自然结束：暂停不取消已在执行的任务。
	close(gates[0])
	arunning.Wait()
	if aRan.Load() != 1 {
		t.Fatalf("a 在执行任务应自然结束, aRan=%d", aRan.Load())
	}
	time.Sleep(50 * time.Millisecond)
	if s := rt.Metrics(); s.Running != 0 || s.Queued != 3 {
		t.Fatalf("在执行任务结束后: Running=%d Queued=%d, 期望 0/3（暂停中不得补位）", s.Running, s.Queued)
	}
	t.Logf("判定: a 在执行任务自然结束(aRan=1), 暂停仍生效, 3 个排队任务无一启动")

	// 恢复前把 a 组配额设为 2：恢复后必须遵守恢复当时生效的配额。
	if err := rt.UpdateConfig(ConfigUpdate{GroupQuotas: map[string]int{"a": 2}}); err != nil {
		t.Fatalf("设置 a 配额=2: %v", err)
	}
	phase.Store(1) // 排队任务启动后阻塞在 gates[1]，便于观测峰值
	if err := rt.ResumeGroup("a"); err != nil {
		t.Fatalf("ResumeGroup(a): %v", err)
	}
	if rt.IsGroupPaused("a") {
		t.Fatalf("IsGroupPaused(a) 恢复后应为 false")
	}
	waitForRunning(t, rt, 2, "恢复后按配额=2 放行")
	waitForQueued(t, rt, 1, "恢复后剩余 1 个排队")
	time.Sleep(30 * time.Millisecond)
	if s := rt.Metrics(); s.Running != 2 || s.Queued != 1 {
		t.Fatalf("恢复后: Running=%d Queued=%d, 期望 2/1（遵守恢复时配额 2）", s.Running, s.Queued)
	}
	if aMax.Load() > 2 {
		t.Fatalf("恢复后 a 组峰值=%d > 当时配额 2", aMax.Load())
	}
	t.Logf("输入: 恢复前将 a 配额设为 2 后恢复")
	t.Logf("判定: Running=2 Queued=1, a 组观测峰值=%d(<=2)", aMax.Load())

	// 放开阶段 1 闸门，全部收敛。
	close(gates[1])
	for _, h := range aHandles {
		h.Wait()
	}
	s = rt.Metrics()
	t.Logf("终态快照: %+v, a 组总执行=%d, b 组完成=%d", s, aRan.Load(), bDone.Load())
	if s.Completed != 9 || s.Queued != 0 || s.Running != 0 {
		t.Fatalf("Completed=%d Queued=%d Running=%d, 期望 9/0/0", s.Completed, s.Queued, s.Running)
	}
}

// TestPauseErrors 验证重复暂停与恢复未暂停分组返回明确错误。
func TestPauseErrors(t *testing.T) {
	rt := New(Config{MaxConcurrency: 2})
	defer rt.Shutdown(context.Background())

	if err := rt.PauseGroup("g"); err != nil {
		t.Fatalf("首次暂停: %v", err)
	}
	err := rt.PauseGroup("g")
	if !errors.Is(err, ErrGroupPaused) {
		t.Fatalf("重复暂停 err=%v, 期望 ErrGroupPaused", err)
	}
	t.Logf("判定: 重复暂停被拒: %v", err)

	err = rt.ResumeGroup("never")
	if !errors.Is(err, ErrGroupNotPaused) {
		t.Fatalf("恢复未暂停分组 err=%v, 期望 ErrGroupNotPaused", err)
	}
	t.Logf("判定: 恢复未暂停分组被拒: %v", err)

	if err := rt.ResumeGroup("g"); err != nil {
		t.Fatalf("正常恢复: %v", err)
	}
	err = rt.ResumeGroup("g")
	if !errors.Is(err, ErrGroupNotPaused) {
		t.Fatalf("再次恢复 err=%v, 期望 ErrGroupNotPaused", err)
	}
	t.Logf("判定: 恢复后再次恢复被拒: %v", err)
}

// TestPauseSubmitCancelTimeout 验证暂停期间提交与取消语义与正常一致：
// 排队任务可立即取消并拿到 Canceled 终态；暂停不触发超时
// （超时从开始执行计时），恢复执行后超时仍正常生效。
func TestPauseSubmitCancelTimeout(t *testing.T) {
	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16})
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	if _, err := rt.Submit(Task{ID: "blocker", Group: "x", Func: func(ctx context.Context) (any, error) {
		<-release
		return nil, nil
	}}); err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	waitForRunning(t, rt, 1, "blocker 进入执行")

	if err := rt.PauseGroup("p"); err != nil {
		t.Fatalf("pause: %v", err)
	}

	var ran, callbacks atomic.Int64
	// 暂停期间提交：正常入队（不拒绝）。
	h, err := rt.Submit(Task{
		ID:      "p-task",
		Group:   "p",
		Timeout: 300 * time.Millisecond,
		Func: func(ctx context.Context) (any, error) {
			ran.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		OnComplete: func(res Result) { callbacks.Add(1) },
	})
	if err != nil {
		t.Fatalf("暂停期间提交应正常入队: %v", err)
	}
	t.Logf("输入: 暂停 p 组期间提交 1 个排队任务(超时 300ms), 入队后立即取消")

	// 暂停期间取消排队任务：必须立即得到终态，不得卡住。
	done := make(chan Result, 1)
	go func() { done <- h.Wait() }()
	h.Cancel()
	select {
	case res := <-done:
		if res.Status != StatusCanceled {
			t.Fatalf("暂停期间取消排队任务 status=%s, 期望 Canceled", res.Status)
		}
		t.Logf("判定: 排队任务取消后立即拿到终态 %s, 任务体执行=%d 回调=%d",
			res.Status, ran.Load(), callbacks.Load())
	case <-time.After(time.Second):
		t.Fatal("暂停期间取消排队任务后 1s 内未拿到终态")
	}
	if ran.Load() != 0 || callbacks.Load() != 0 {
		t.Fatalf("被取消任务 ran=%d callbacks=%d, 期望 0/0", ran.Load(), callbacks.Load())
	}

	// 再提交一个带超时的任务：暂停中即使等待超过 Timeout 也不会超时
	// （超时从开始执行计时）；恢复后开始执行并按超时收敛。
	h2, err := rt.Submit(Task{
		ID:      "p-timeout",
		Group:   "p",
		Timeout: 80 * time.Millisecond,
		Func: func(ctx context.Context) (any, error) {
			ran.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("submit p-timeout: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // 暂停中等待 > Timeout
	select {
	case <-h2.Done():
		t.Fatalf("暂停中的排队任务不应因超时而终结（超时从开始执行计时）")
	default:
	}
	t.Logf("输入: 暂停中排队任务等待 300ms(>其超时 80ms)")
	t.Logf("判定: 仍处于排队、未提前终结")

	if err := rt.ResumeGroup("p"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	// blocker 仍占着唯一执行槽，p-timeout 还不能执行；放开 blocker。
	close(release)
	res := h2.Wait()
	t.Logf("输入: 恢复并释放执行槽后, p-timeout 开始执行(超时 80ms)")
	t.Logf("判定: status=%s 任务体执行=%d", res.Status, ran.Load())
	if res.Status != StatusTimedOut {
		t.Fatalf("status=%s, 期望 TimedOut", res.Status)
	}
	if ran.Load() != 1 {
		t.Fatalf("p-timeout 应恰好执行 1 次, ran=%d", ran.Load())
	}
	s := rt.Metrics()
	t.Logf("终态快照: %+v", s)
	if s.Canceled != 1 || s.TimedOut != 1 {
		t.Fatalf("Canceled=%d TimedOut=%d, 期望 1/1", s.Canceled, s.TimedOut)
	}
}
