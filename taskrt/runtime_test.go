package taskrt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shutdownRt 以超时兜底关停运行时，避免阻塞型任务在测试失败路径上挂死。
func shutdownRt(rt *Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = rt.Shutdown(ctx)
}

// waitResult 等待任务结论，超时视为测试失败。
func waitResult(t *testing.T, h *Handle) Result {
	t.Helper()
	select {
	case r, ok := <-h.Done():
		if !ok {
			t.Fatalf("done channel closed without result")
		}
		return r
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for task result")
		return Result{}
	}
}

func TestQueueOverflowReject(t *testing.T) {
	const workers, capacity = 1, 2
	rt := New(Config{Workers: workers, QueueCapacity: capacity})
	defer shutdownRt(rt)

	block := make(chan struct{})
	started := make(chan struct{})
	// 占满唯一的工作协程。
	h0, err := rt.Submit(Task{Func: func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}})
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	<-started

	// 填满排队容量。
	queued := make([]*Handle, 0, capacity)
	for i := 0; i < capacity; i++ {
		h, err := rt.Submit(Task{Func: func(ctx context.Context) error { return nil }})
		if err != nil {
			t.Fatalf("submit queued %d: %v", i, err)
		}
		queued = append(queued, h)
	}

	// 超容提交必须被拒绝且原因可区分。
	_, err = rt.Submit(Task{Func: func(ctx context.Context) error { return nil }})
	t.Logf("输入: workers=%d capacity=%d 已占用worker=1 已排队=%d, 第4个任务提交结果 err=%v",
		workers, capacity, len(queued), err)
	re, ok := err.(*RejectError)
	if !ok {
		t.Fatalf("判定失败: 期望 *RejectError, 实际 %T (%v)", err, err)
	}
	if re.Reason != RejectQueueFull {
		t.Fatalf("判定失败: 期望原因 %v, 实际 %v", RejectQueueFull, re.Reason)
	}
	t.Logf("判定依据: 拒绝原因为 %q, 与队列溢出场景一致", re.Reason)

	close(block)
	for _, h := range queued {
		if r := waitResult(t, h); r.Status != StatusCompleted {
			t.Fatalf("queued task status = %v, want completed", r.Status)
		}
	}
	_ = h0
	if got := rt.Stats().Rejected; got != 1 {
		t.Fatalf("拒绝计数=%d, 期望1", got)
	}
}

func TestGroupQuotaEnforced(t *testing.T) {
	const maxConcurrent = 1
	rt := New(Config{
		Workers:       4,
		QueueCapacity: 64,
		GroupQuotas:   map[string]GroupQuota{"g": {MaxConcurrent: maxConcurrent, MaxQueued: 2}},
	})
	defer shutdownRt(rt)

	var cur, maxSeen atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	const n = 6
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		h, err := rt.Submit(Task{Group: "g", Func: func(ctx context.Context) error {
			c := cur.Add(1)
			for {
				m := maxSeen.Load()
				if c <= m || maxSeen.CompareAndSwap(m, c) {
					break
				}
			}
			<-release
			cur.Add(-1)
			return nil
		}})
		if err != nil {
			t.Logf("第 %d 个任务被拒绝: %v (MaxQueued=2, 执行占1+排队2=3 个名额)", i, err)
			continue
		}
		handles = append(handles, h)
		wg.Add(1)
		go func(h *Handle) { defer wg.Done(); waitResult(t, h) }(h)
	}
	t.Logf("输入: 分组g配额 MaxConcurrent=%d MaxQueued=2, 提交 %d 个任务, 接纳 %d 个",
		maxConcurrent, n, len(handles))

	// 并发上限为1：任意时刻在途执行不得超过1。
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	t.Logf("判定依据: 观察到的最大并发=%d, 配额上限=%d", maxSeen.Load(), maxConcurrent)
	if maxSeen.Load() > maxConcurrent {
		t.Fatalf("配额越界: 最大并发 %d > 上限 %d", maxSeen.Load(), maxConcurrent)
	}
	// 排队配额为2：并发1 + 排队2 = 最多3个在系统内，其余必须被拒绝。
	if len(handles) > 3 {
		t.Fatalf("排队配额越界: 接纳 %d > 3", len(handles))
	}
}

func TestPriorityNoStarvation(t *testing.T) {
	const weight = 4
	rt := New(Config{Workers: 1, QueueCapacity: 128, FairnessWeight: weight})
	defer shutdownRt(rt)

	block := make(chan struct{})
	entered := make(chan struct{})
	// 先占住 worker，让后续任务全部排队，保证调度顺序可观测。
	hb, err := rt.Submit(Task{Priority: PriorityNormal, Func: func(ctx context.Context) error {
		close(entered)
		<-block
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-entered

	const nHigh, nLow = 12, 3
	var mu sync.Mutex
	var order []string
	mk := func(tag string) Task {
		return Task{Func: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, tag)
			mu.Unlock()
			return nil
		}}
	}
	var all, lows []*Handle
	for i := 0; i < nHigh; i++ {
		tk := mk(fmt.Sprintf("H%d", i))
		tk.Priority = PriorityHigh
		h, err := rt.Submit(tk)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, h)
	}
	for i := 0; i < nLow; i++ {
		tk := mk(fmt.Sprintf("L%d", i))
		tk.Priority = PriorityLow
		h, err := rt.Submit(tk)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, h)
		lows = append(lows, h)
	}
	close(block)
	if r := waitResult(t, hb); r.Status != StatusCompleted {
		t.Fatalf("blocker: %v", r.Status)
	}
	for _, h := range all {
		if r := waitResult(t, h); r.Status != StatusCompleted {
			t.Fatalf("任务未正常完成(可能饿死): status=%v err=%v", r.Status, r.Err)
		}
	}

	// 统计低优先级首次完成前，高优先级最大连续执行数。
	mu.Lock()
	maxConsec, consec := 0, 0
	for _, tag := range order {
		if tag[0] == 'H' {
			consec++
			if consec > maxConsec {
				maxConsec = consec
			}
		} else {
			consec = 0
		}
	}
	mu.Unlock()
	t.Logf("输入: 高优先级=%d 低优先级=%d FairnessWeight=%d, 执行顺序=%v",
		nHigh, nLow, weight, order)
	t.Logf("判定依据: 高优先级最大连续执行=%d, 应 <= weight(%d)+1(在途)", maxConsec, weight)
	if maxConsec > weight+1 {
		t.Fatalf("低优先级饥饿: 高优先级连续执行 %d 次, 超过阈值 %d", maxConsec, weight+1)
	}
}

func TestCancelQueuedTask(t *testing.T) {
	rt := New(Config{Workers: 1, QueueCapacity: 8})
	defer shutdownRt(rt)

	block := make(chan struct{})
	entered := make(chan struct{})
	if _, err := rt.Submit(Task{Func: func(ctx context.Context) error {
		close(entered)
		<-block
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	<-entered

	var ran atomic.Bool
	h, err := rt.Submit(Task{Func: func(ctx context.Context) error {
		ran.Store(true)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	h.Cancel()
	// 取消可能发生在任务排队中或已出队待执行(in-transit)两种时序下，
	// 两种路径都必须得出 cancelled 结论；放开阻塞任务以便后者被工作协程终结。
	close(block)
	r := waitResult(t, h)
	t.Logf("输入: worker被占满时提交任务并立即取消, 结果 status=%v err=%v", r.Status, r.Err)
	t.Logf("判定依据: 取消应得 StatusCancelled 且任务体不得执行(ran=%v)", ran.Load())
	if r.Status != StatusCancelled {
		t.Fatalf("status = %v, want cancelled", r.Status)
	}
	if ran.Load() {
		t.Fatal("被取消的任务产生了执行副作用")
	}
}

func TestTaskTimeout(t *testing.T) {
	rt := New(Config{Workers: 2, QueueCapacity: 8})
	defer shutdownRt(rt)

	// 协作式任务：阻塞直到 ctx 到期。
	h1, err := rt.Submit(Task{Timeout: 50 * time.Millisecond, Func: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	// 非协作任务：忽略 ctx 长睡，超时仍须按时终结。
	h2, err := rt.Submit(Task{Timeout: 50 * time.Millisecond, Func: func(ctx context.Context) error {
		time.Sleep(2 * time.Second)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	r1 := waitResult(t, h1)
	r2 := waitResult(t, h2)
	elapsed := time.Since(start)
	t.Logf("输入: 两个任务 Timeout=50ms (协作式/非协作式), 结果 r1=%v r2=%v 耗时=%v",
		r1.Status, r2.Status, elapsed)
	t.Logf("判定依据: 均应为 StatusTimeout, 且非协作任务也须在超时附近终结(<<2s)")
	if r1.Status != StatusTimeout || r2.Status != StatusTimeout {
		t.Fatalf("status = %v/%v, want timeout/timeout", r1.Status, r2.Status)
	}
	if elapsed > time.Second {
		t.Fatalf("超时强制终结失效: 耗时 %v", elapsed)
	}
	if got := rt.Stats().TimedOut; got != 2 {
		t.Fatalf("timedOut = %d, want 2", got)
	}
}

func TestCancelRaceExactlyOnce(t *testing.T) {
	rt := New(Config{Workers: 4, QueueCapacity: 256})
	defer shutdownRt(rt)

	const n = 200
	handles := make([]*Handle, 0, n)
	for i := 0; i < n; i++ {
		h, err := rt.Submit(Task{Priority: Priority(i % 3), Func: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
				return nil
			}
		}})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	// 并发取消一半任务，与执行路径竞争。
	var wg sync.WaitGroup
	for i := 0; i < n; i += 2 {
		wg.Add(1)
		go func(h *Handle) { defer wg.Done(); h.Cancel() }(handles[i])
	}
	wg.Wait()

	counts := map[Status]int{}
	for _, h := range handles {
		r := waitResult(t, h)
		counts[r.Status]++
	}
	s := rt.Stats()
	t.Logf("输入: %d 个任务, 并发取消其中 %d 个", n, n/2)
	t.Logf("判定依据: 每个任务恰好一个结论; submitted(%d) == 终结总数(%d); 统计 completed=%d cancelled=%d",
		s.Submitted, len(handles), s.Completed, s.Cancelled)
	if s.Submitted != int64(n) {
		t.Fatalf("submitted = %d, want %d", s.Submitted, n)
	}
	if s.Completed+s.Cancelled+s.Failed+s.TimedOut != int64(n) {
		t.Fatalf("终结计数 %d 与提交数 %d 不一致(重复或漏计)",
			s.Completed+s.Cancelled+s.Failed+s.TimedOut, n)
	}
	if counts[StatusCompleted]+counts[StatusCancelled] != n {
		t.Fatalf("结论缺失: %v", counts)
	}
	if s.Started > s.Submitted {
		t.Fatalf("started(%d) > submitted(%d), 存在重复执行", s.Started, s.Submitted)
	}
}

func TestBusinessFailureIsolation(t *testing.T) {
	rt := New(Config{Workers: 2, QueueCapacity: 16})
	defer shutdownRt(rt)

	bizErr := errors.New("biz failure")
	hFail, err := rt.Submit(Task{Func: func(ctx context.Context) error { return bizErr }})
	if err != nil {
		t.Fatal(err)
	}
	hPanic, err := rt.Submit(Task{Func: func(ctx context.Context) error { panic("boom") }})
	if err != nil {
		t.Fatal(err)
	}
	hOK, err := rt.Submit(Task{Func: func(ctx context.Context) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}

	rFail, rPanic, rOK := waitResult(t, hFail), waitResult(t, hPanic), waitResult(t, hOK)
	t.Logf("输入: 业务错误/panic/正常 各1个任务, 结果: %v/%v/%v", rFail.Status, rPanic.Status, rOK.Status)
	t.Logf("判定依据: 业务失败与panic应计为failed且err可区分, 后续任务不受影响")
	if rFail.Status != StatusFailed || !errors.Is(rFail.Err, bizErr) {
		t.Fatalf("业务失败上报错误: %v %v", rFail.Status, rFail.Err)
	}
	if rPanic.Status != StatusFailed || rPanic.Err == nil {
		t.Fatalf("panic未被隔离为失败: %v %v", rPanic.Status, rPanic.Err)
	}
	if rOK.Status != StatusCompleted {
		t.Fatalf("失败任务污染后续调度: %v", rOK.Status)
	}
	s := rt.Stats()
	if s.Failed != 2 || s.Completed != 1 {
		t.Fatalf("统计错乱: failed=%d completed=%d", s.Failed, s.Completed)
	}
}
