package taskrt

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// gatedGroupTasks 构造按“启动时所处阶段”阻塞在对应闸门的分组任务，
// 并记录每个阶段该分组的观测峰值并发。
func gatedGroupTasks(phase *atomic.Int32, cur *atomic.Int64, maxObs []atomic.Int64, gates []chan struct{}) func(ctx context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		p := phase.Load()
		n := cur.Add(1)
		for {
			old := maxObs[p].Load()
			if n <= old || maxObs[p].CompareAndSwap(old, n) {
				break
			}
		}
		<-gates[p]
		cur.Add(-1)
		return nil, nil
	}
}

// TestUpdateGroupQuotaScaleDownUpRemove 验证分组单独配额的运行期
// 调小、调大与移除：调小时在执行任务不受影响、新任务按新配额放行；
// 移除单独配额后回落到默认配额。
func TestUpdateGroupQuotaScaleDownUpRemove(t *testing.T) {
	rt := New(Config{
		MaxConcurrency:    8,
		QueueCapacity:     64,
		DefaultGroupQuota: 0, // 默认不限
	})
	defer rt.Shutdown(context.Background())

	var phase atomic.Int32
	var cur atomic.Int64
	maxObs := make([]atomic.Int64, 4)
	gates := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}
	fn := gatedGroupTasks(&phase, &cur, maxObs, gates)

	// 阶段 0：lim 组无单独配额（默认不限），6 个任务全部执行。
	for i := 0; i < 6; i++ {
		if _, err := rt.Submit(Task{ID: "p0", Group: "lim", Func: fn}); err != nil {
			t.Fatalf("submit p0 %d: %v", i, err)
		}
	}
	waitForRunning(t, rt, 6, "阶段0占满")
	if maxObs[0].Load() != 6 {
		t.Fatalf("阶段0 lim 峰值=%d, 期望 6", maxObs[0].Load())
	}

	// 调小 lim 配额为 2，再提交 4 个：在执行的 6 个不受影响，新任务不放行。
	if err := rt.UpdateConfig(ConfigUpdate{GroupQuotas: map[string]int{"lim": 2}}); err != nil {
		t.Fatalf("设置 lim=2: %v", err)
	}
	var hs []*Handle
	for i := 0; i < 4; i++ {
		h, err := rt.Submit(Task{ID: "p1", Group: "lim", Func: fn})
		if err != nil {
			t.Fatalf("submit p1 %d: %v", i, err)
		}
		hs = append(hs, h)
	}
	t.Logf("输入: lim 组 6 任务在执行(原不限额), 运行期设配额=2, 再提交 4 个")
	waitForQueued(t, rt, 4, "调小后排队")
	time.Sleep(50 * time.Millisecond)
	if s := rt.Metrics(); s.Running != 6 || s.Queued != 4 {
		t.Fatalf("配额=2 后 50ms: Running=%d Queued=%d, 期望 6/4", s.Running, s.Queued)
	}
	t.Logf("判定: 在执行任务不受影响(Running=6), 新任务无一放行(Queued=4)")

	// 阶段 1：放开旧任务，配额=2，应恰好 2 个新任务执行、2 个排队。
	phase.Store(1)
	close(gates[0])
	waitForRunning(t, rt, 2, "配额=2 收敛")
	waitForQueued(t, rt, 2, "配额=2 剩余排队")
	time.Sleep(50 * time.Millisecond)
	if s := rt.Metrics(); s.Running != 2 || s.Queued != 2 {
		t.Fatalf("配额=2 观察窗口: Running=%d Queued=%d, 期望 2/2", s.Running, s.Queued)
	}
	if maxObs[1].Load() > 2 {
		t.Fatalf("配额=2 期间 lim 峰值=%d > 2", maxObs[1].Load())
	}
	t.Logf("判定: 配额=2 严格生效, Running=2 Queued=2, 阶段峰值=%d", maxObs[1].Load())

	// 调大 lim 配额为 4：应再放行 2 个，排队清空。
	phase.Store(2)
	if err := rt.UpdateConfig(ConfigUpdate{GroupQuotas: map[string]int{"lim": 4}}); err != nil {
		t.Fatalf("设置 lim=4: %v", err)
	}
	waitForRunning(t, rt, 4, "配额=4 补位")
	waitForQueued(t, rt, 0, "配额=4 排队清空")
	if maxObs[2].Load() > 4 {
		t.Fatalf("配额=4 期间 lim 峰值=%d > 4", maxObs[2].Load())
	}
	t.Logf("输入: 排队 2 个期间 lim 配额 2→4")
	t.Logf("判定: Running=4(<=4) Queued=0, 阶段峰值=%d", maxObs[2].Load())

	// 配额=4 已满时再提交 3 个，必然排队；随后移除 lim 单独配额，
	// 回落到默认配额（不限），3 个应全部放行（受全局 8 约束：4+3=7）。
	for i := 0; i < 3; i++ {
		h, err := rt.Submit(Task{ID: "p3", Group: "lim", Func: fn})
		if err != nil {
			t.Fatalf("submit p3 %d: %v", i, err)
		}
		hs = append(hs, h)
	}
	waitForQueued(t, rt, 3, "移除前排队")
	phase.Store(3)
	if err := rt.UpdateConfig(ConfigUpdate{RemoveGroupQuotas: []string{"lim"}}); err != nil {
		t.Fatalf("移除 lim 配额: %v", err)
	}
	waitForRunning(t, rt, 7, "移除后回落默认配额")
	waitForQueued(t, rt, 0, "移除后排队清空")
	if _, ok := rt.CurrentConfig().GroupQuotas["lim"]; ok {
		t.Fatalf("lim 单独配额应已移除")
	}
	t.Logf("输入: lim 配额=4 且占满, 再排 3 个后移除 lim 单独配额(默认不限)")
	t.Logf("判定: Running=7(<=全局 8) Queued=0, 单独配额表已移除 lim")

	close(gates[1])
	close(gates[2])
	close(gates[3])
	for _, h := range hs {
		h.Wait()
	}
	s := rt.Metrics()
	t.Logf("终态快照: %+v, 各阶段 lim 峰值=[6,%d,%d,%d]", s, maxObs[1].Load(), maxObs[2].Load(), maxObs[3].Load())
	if s.Completed != 13 || s.Queued != 0 || s.Running != 0 {
		t.Fatalf("Completed=%d Queued=%d Running=%d, 期望 13/0/0", s.Completed, s.Queued, s.Running)
	}
}

// TestUpdateDefaultGroupQuota 验证默认配额的运行期调大与调小：
// 没有单独配额的分组始终受当时生效的默认配额约束。
func TestUpdateDefaultGroupQuota(t *testing.T) {
	rt := New(Config{
		MaxConcurrency:    8,
		QueueCapacity:     64,
		DefaultGroupQuota: 1, // 初始默认配额=1
	})
	defer rt.Shutdown(context.Background())

	gate := make(chan struct{})
	var cur, maxSeen atomic.Int64
	fn := func(ctx context.Context) (any, error) {
		n := cur.Add(1)
		for {
			old := maxSeen.Load()
			if n <= old || maxSeen.CompareAndSwap(old, n) {
				break
			}
		}
		<-gate
		cur.Add(-1)
		return nil, nil
	}

	// 默认配额=1：4 个无单独配额的同组任务，只有 1 个能执行。
	var hs []*Handle
	for i := 0; i < 4; i++ {
		h, err := rt.Submit(Task{ID: "x", Group: "gx", Func: fn})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		hs = append(hs, h)
	}
	waitForRunning(t, rt, 1, "默认配额=1")
	time.Sleep(50 * time.Millisecond)
	if s := rt.Metrics(); s.Running != 1 || s.Queued != 3 {
		t.Fatalf("默认配额=1: Running=%d Queued=%d, 期望 1/3", s.Running, s.Queued)
	}
	if maxSeen.Load() > 1 {
		t.Fatalf("默认配额=1 期间峰值=%d > 1", maxSeen.Load())
	}
	t.Logf("输入: 默认配额=1, 提交 4 个 gx 组任务(无单独配额)")
	t.Logf("判定: Running=1 Queued=3, 观测峰值=%d", maxSeen.Load())

	// 调大默认配额到 3：应再放行 2 个。
	if err := rt.UpdateConfig(ConfigUpdate{DefaultGroupQuota: ptrInt(3)}); err != nil {
		t.Fatalf("默认配额=3: %v", err)
	}
	waitForRunning(t, rt, 3, "默认配额=3")
	time.Sleep(30 * time.Millisecond)
	if s := rt.Metrics(); s.Running != 3 || s.Queued != 1 {
		t.Fatalf("默认配额=3: Running=%d Queued=%d, 期望 3/1", s.Running, s.Queued)
	}
	t.Logf("输入: 默认配额 1→3, 判定: Running=3(<=3) Queued=1, 峰值=%d", maxSeen.Load())

	// 调为 0（不限制）：最后 1 个也放行。
	if err := rt.UpdateConfig(ConfigUpdate{DefaultGroupQuota: ptrInt(0)}); err != nil {
		t.Fatalf("默认配额=0: %v", err)
	}
	waitForRunning(t, rt, 4, "默认配额不限")
	if maxSeen.Load() > 4 {
		t.Fatalf("峰值=%d > 4", maxSeen.Load())
	}
	t.Logf("输入: 默认配额 3→0(不限), 判定: Running=4, 峰值=%d", maxSeen.Load())

	// 调小方向：全部收敛后，把默认配额调回 1，新提交 3 个只准 1 个执行。
	close(gate)
	for _, h := range hs {
		h.Wait()
	}
	if err := rt.UpdateConfig(ConfigUpdate{DefaultGroupQuota: ptrInt(1)}); err != nil {
		t.Fatalf("默认配额调回 1: %v", err)
	}
	gate2 := make(chan struct{})
	var cur2, max2 atomic.Int64
	fn2 := func(ctx context.Context) (any, error) {
		n := cur2.Add(1)
		for {
			old := max2.Load()
			if n <= old || max2.CompareAndSwap(old, n) {
				break
			}
		}
		<-gate2
		cur2.Add(-1)
		return nil, nil
	}
	for i := 0; i < 3; i++ {
		h, err := rt.Submit(Task{ID: "y", Group: "gx", Func: fn2})
		if err != nil {
			t.Fatalf("submit down %d: %v", i, err)
		}
		hs = append(hs, h)
	}
	waitForRunning(t, rt, 1, "调小后默认配额=1")
	time.Sleep(50 * time.Millisecond)
	if max2.Load() > 1 {
		t.Fatalf("调小后默认配额=1 期间峰值=%d > 1", max2.Load())
	}
	t.Logf("输入: 收敛后默认配额调回 1, 再提交 3 个")
	t.Logf("判定: Running=1, 观测峰值=%d", max2.Load())

	close(gate2)
	for i := 4; i < len(hs); i++ {
		hs[i].Wait()
	}
	s := rt.Metrics()
	t.Logf("终态快照: %+v", s)
	if s.Completed != 7 || s.Queued != 0 || s.Running != 0 {
		t.Fatalf("Completed=%d Queued=%d Running=%d, 期望 7/0/0", s.Completed, s.Queued, s.Running)
	}
}

// waitForQueued 轮询直到 Queued 指标等于 want。
func waitForQueued(t *testing.T, rt *Runtime, want int64, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if rt.Metrics().Queued == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: Queued=%d, 期望 %d", what, rt.Metrics().Queued, want)
		}
		time.Sleep(time.Millisecond)
	}
}
