package taskrt

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// waitForRunning 轮询直到 Running 指标达到 want（或超时失败）。
func waitForRunning(t *testing.T, rt *Runtime, want int64, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if rt.Metrics().Running >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: Running=%d, 期望达到 %d", what, rt.Metrics().Running, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestUpdateMaxConcurrencyScaleDownUp 验证运行期先调小再调大全局并发：
// 调小后不放行新任务（在执行任务不受影响）；旧任务收敛后严格按新上限=1
// 放行；再调大为 3 后排队任务立即补到 3。任何时刻并发不超过当时上限。
func TestUpdateMaxConcurrencyScaleDownUp(t *testing.T) {
	rt := New(Config{MaxConcurrency: 4, QueueCapacity: 64})
	defer rt.Shutdown(context.Background())

	// 三个阶段各自独立的闸门与峰值统计：
	// phase0：初始上限 4；phase1：调小后上限 1；phase2：调大后上限 3。
	gates := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}
	var maxObs [4]atomic.Int64
	var cur atomic.Int64
	var phase atomic.Int32
	phase.Store(0)

	taskFn := func(ctx context.Context) (any, error) {
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

	// 阶段 0：4 个任务占满初始并发 4。
	for i := 0; i < 4; i++ {
		if _, err := rt.Submit(Task{ID: "p0", Func: taskFn}); err != nil {
			t.Fatalf("submit p0 %d: %v", i, err)
		}
	}
	waitForRunning(t, rt, 4, "阶段0占满")
	if maxObs[0].Load() != 4 {
		t.Fatalf("阶段0峰值=%d, 期望 4", maxObs[0].Load())
	}

	// 调小到 1，并提交 4 个新任务（此时旧任务仍在执行）。
	if err := rt.UpdateConfig(ConfigUpdate{MaxConcurrency: 1}); err != nil {
		t.Fatalf("调小并发: %v", err)
	}
	var queued []*Handle
	for i := 0; i < 4; i++ {
		h, err := rt.Submit(Task{ID: "p1", Func: taskFn})
		if err != nil {
			t.Fatalf("submit p1 %d: %v", i, err)
		}
		queued = append(queued, h)
	}
	t.Logf("输入: 初始并发=4(4 个任务在执行), 运行期调小为 1, 再提交 4 个任务")
	time.Sleep(50 * time.Millisecond)
	if s := rt.Metrics(); s.Running != 4 || s.Queued != 4 {
		t.Fatalf("调小后 50ms: Running=%d Queued=%d, 期望 4/4", s.Running, s.Queued)
	}
	t.Logf("判定: 在执行任务不受影响(Running=4), 新任务无一放行(Queued=4)")

	// 阶段切换到 1 并放开旧任务：新上限=1，收敛后应恰有 1 个新任务在执行。
	phase.Store(1)
	close(gates[0])
	waitForRunning(t, rt, 1, "调小后收敛")
	deadline := time.Now().Add(2 * time.Second)
	for rt.Metrics().Queued != 3 {
		if time.Now().After(deadline) {
			t.Fatalf("调小后 Queued=%d, 期望 3", rt.Metrics().Queued)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // 观察窗口：确认不会偷偷放行第 2 个
	if s := rt.Metrics(); s.Running != 1 || s.Queued != 3 {
		t.Fatalf("上限=1 观察窗口: Running=%d Queued=%d, 期望 1/3", s.Running, s.Queued)
	}
	if maxObs[1].Load() > 1 {
		t.Fatalf("上限=1 期间观测到峰值 %d", maxObs[1].Load())
	}
	t.Logf("判定: 新上限=1 生效, Running=1 Queued=3, 阶段峰值=%d", maxObs[1].Load())

	// 调大到 3：应立即再放行 2 个排队任务，且不超过 3。
	phase.Store(2)
	if err := rt.UpdateConfig(ConfigUpdate{MaxConcurrency: 3}); err != nil {
		t.Fatalf("调大并发: %v", err)
	}
	waitForRunning(t, rt, 3, "调大后补位")
	time.Sleep(50 * time.Millisecond)
	if s := rt.Metrics(); s.Running != 3 || s.Queued != 1 {
		t.Fatalf("上限=3: Running=%d Queued=%d, 期望 3/1", s.Running, s.Queued)
	}
	if maxObs[2].Load() > 3 {
		t.Fatalf("上限=3 期间观测到峰值 %d", maxObs[2].Load())
	}
	t.Logf("输入: 排队 3 个期间并发从 1 调大为 3")
	t.Logf("判定: Running=3(<=3) Queued=1, 阶段峰值=%d", maxObs[2].Load())

	// 最后 1 个任务在阶段 3 放行，全部收敛。
	phase.Store(3)
	close(gates[1])
	close(gates[2])
	waitForRunning(t, rt, 1, "最后任务放行")
	close(gates[3])
	for _, h := range queued {
		h.Wait()
	}
	s := rt.Metrics()
	t.Logf("终态快照: %+v, 各阶段峰值=[4,%d,%d,%d]", s, maxObs[1].Load(), maxObs[2].Load(), maxObs[3].Load())
	if s.Completed != 8 || s.Queued != 0 || s.Running != 0 {
		t.Fatalf("Completed=%d Queued=%d Running=%d, 期望 8/0/0", s.Completed, s.Queued, s.Running)
	}
}

// TestUpdateConfigRejected 验证非法与冲突调整被整体拒绝，
// 拒绝后配置保持原样（不存在半更新）。
func TestUpdateConfigRejected(t *testing.T) {
	rt := New(Config{
		MaxConcurrency:    4,
		DefaultGroupQuota: 2,
		GroupQuotas:       map[string]int{"g1": 1},
	})
	defer rt.Shutdown(context.Background())
	t.Logf("初始配置: max=4 defaultQuota=2 quotas={g1:1}")

	check := func(what string, u ConfigUpdate, want error) {
		t.Helper()
		err := rt.UpdateConfig(u)
		if !errors.Is(err, want) {
			t.Fatalf("%s: err=%v, 期望 %v", what, err, want)
		}
		t.Logf("判定: %s 被拒: %v", what, err)
	}
	check("并发上限为负", ConfigUpdate{MaxConcurrency: -3}, ErrInvalidMaxConcurrency)
	check("默认配额为负", ConfigUpdate{DefaultGroupQuota: ptrInt(-1)}, ErrInvalidQuota)
	check("分组配额为负", ConfigUpdate{GroupQuotas: map[string]int{"g2": -2}}, ErrInvalidQuota)
	check("设置与移除冲突", ConfigUpdate{
		GroupQuotas:       map[string]int{"g2": 3},
		RemoveGroupQuotas: []string{"g2"},
	}, ErrQuotaConflict)

	// 零值 MaxConcurrency 语义为"不变更"，合法。
	if err := rt.UpdateConfig(ConfigUpdate{MaxConcurrency: 0}); err != nil {
		t.Fatalf("MaxConcurrency=0(不变更) 应合法: %v", err)
	}

	// 所有拒绝后，配置必须保持初始值（原子性，无半更新）。
	cur := rt.CurrentConfig()
	t.Logf("拒绝后配置: max=%d defaultQuota=%d quotas=%v", cur.MaxConcurrency, cur.DefaultGroupQuota, cur.GroupQuotas)
	if cur.MaxConcurrency != 4 || cur.DefaultGroupQuota != 2 || cur.GroupQuotas["g1"] != 1 {
		t.Fatalf("拒绝非法变更后配置被污染: %+v", cur)
	}
	if _, ok := cur.GroupQuotas["g2"]; ok {
		t.Fatalf("g2 不应存在于配额表（冲突变更必须整体不生效）")
	}
}

func ptrInt(v int) *int { return &v }

// TestUpdateConfigAtomicCombined 验证一次变更可同时调整多项，
// 且整体生效；并支持为从未出现过的分组新增单独配额、
// 移除不存在的分组配额（幂等，合法）。
func TestUpdateConfigAtomicCombined(t *testing.T) {
	rt := New(Config{MaxConcurrency: 4, DefaultGroupQuota: 2, GroupQuotas: map[string]int{"g1": 1}})
	defer rt.Shutdown(context.Background())

	def := 5
	if err := rt.UpdateConfig(ConfigUpdate{
		MaxConcurrency:    7,
		DefaultGroupQuota: &def,
		GroupQuotas:       map[string]int{"g1": 3, "brand-new": 2},
		RemoveGroupQuotas: []string{"never-existed"},
	}); err != nil {
		t.Fatalf("组合变更: %v", err)
	}
	cur := rt.CurrentConfig()
	t.Logf("输入: 一次变更 max 4→7, default 2→5, g1 1→3, 新增 brand-new=2, 移除不存在的 never-existed")
	t.Logf("判定: 当前配置 max=%d default=%d quotas=%v", cur.MaxConcurrency, cur.DefaultGroupQuota, cur.GroupQuotas)
	if cur.MaxConcurrency != 7 || cur.DefaultGroupQuota != 5 ||
		cur.GroupQuotas["g1"] != 3 || cur.GroupQuotas["brand-new"] != 2 {
		t.Fatalf("组合变更未整体生效: %+v", cur)
	}
	if _, ok := cur.GroupQuotas["never-existed"]; ok {
		t.Fatalf("never-existed 不应出现")
	}

	// 验证“合法部分 + 非法部分”混合时整体拒绝：brand-new 保持上一步的 2，
	// g9 为负值，合法的 max 调整也必须回滚。
	err := rt.UpdateConfig(ConfigUpdate{
		MaxConcurrency: 2,
		GroupQuotas:    map[string]int{"g9": -1},
	})
	if !errors.Is(err, ErrInvalidQuota) {
		t.Fatalf("混合非法变更 err=%v, 期望 ErrInvalidQuota", err)
	}
	cur = rt.CurrentConfig()
	t.Logf("输入: 同一变更里 max 7→2 合法但 g9=-1 非法")
	t.Logf("判定: 被拒后 max=%d(仍为 7, 合法部分一并回滚), g9存在=%v",
		cur.MaxConcurrency, cur.GroupQuotas["g9"] != 0)
	if cur.MaxConcurrency != 7 {
		t.Fatalf("非法变更污染了 MaxConcurrency: %d", cur.MaxConcurrency)
	}
	if _, ok := cur.GroupQuotas["g9"]; ok {
		t.Fatalf("g9 不应落地（整体回滚）")
	}
}
