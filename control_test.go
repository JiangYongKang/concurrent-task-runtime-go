package taskrt

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// intPtr 是测试里取整数指针的小工具。
func intPtr(v int) *int { return &v }

// maxTracker 统计并发执行的峰值。
type maxTracker struct {
	cur atomic.Int64
	max atomic.Int64
}

func (m *maxTracker) enter() {
	n := m.cur.Add(1)
	for {
		old := m.max.Load()
		if n <= old || m.max.CompareAndSwap(old, n) {
			return
		}
	}
}

func (m *maxTracker) exit() { m.cur.Add(-1) }

// waitStarted 在超时前等待 n 个任务进入执行，返回实际收到的数量。
func waitStarted(started <-chan string, n int, timeout time.Duration) int {
	got := 0
	timer := time.After(timeout)
	for got < n {
		select {
		case <-started:
			got++
		case <-timer:
			return got
		}
	}
	return got
}

// TestUpdateMaxConcurrencyIncrease 验证：运行期调大并发上限后，
// 排队任务立即按新上限被调度，且峰值不超过新上限。
func TestUpdateMaxConcurrencyIncrease(t *testing.T) {
	rt := New(Config{MaxConcurrency: 1, QueueCapacity: 16})
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	started := make(chan string, 8)
	var peak maxTracker
	mk := func(id string) Task {
		return Task{ID: id, Group: "g", Func: func(ctx context.Context) (any, error) {
			peak.enter()
			defer peak.exit()
			started <- id
			<-release
			return nil, nil
		}}
	}
	var handles []*Handle
	for _, id := range []string{"a", "b", "c"} {
		h, err := rt.Submit(mk(id))
		if err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
		handles = append(handles, h)
	}

	if got := waitStarted(started, 2, 100*time.Millisecond); got != 1 {
		t.Fatalf("并发上限=1 时应有且仅有 1 个任务在执行, 实际 %d", got)
	}
	if err := rt.UpdateConfig(ConfigUpdate{MaxConcurrency: intPtr(3)}); err != nil {
		t.Fatalf("调大并发上限被拒: %v", err)
	}
	if got := waitStarted(started, 2, 2*time.Second); got != 2 {
		t.Fatalf("调大到 3 后应有 2 个排队任务被调度, 实际 %d", got)
	}
	t.Logf("输入: 3 个阻塞任务, 并发上限 1 -> 3")
	t.Logf("判定: 调整后执行峰值=%d (期望 3, 上限 3)", peak.max.Load())
	if peak.max.Load() != 3 {
		t.Fatalf("调整后峰值并发=%d, 期望 3", peak.max.Load())
	}
	close(release)
	for _, h := range handles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("任务 %s 终态 %s, 期望 Completed", res.TaskID, res.Status)
		}
	}
}

// TestUpdateMaxConcurrencyDecrease 验证：调小并发上限后，执行中的任务
// 照常跑完，之后新调度的任务严格遵守新上限。
func TestUpdateMaxConcurrencyDecrease(t *testing.T) {
	rt := New(Config{MaxConcurrency: 2, QueueCapacity: 16})
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	started := make(chan string, 8)
	var latePeak maxTracker // 只统计调小之后才提交的任务
	mk := func(id string, late bool) Task {
		return Task{ID: id, Group: "g", Func: func(ctx context.Context) (any, error) {
			if late {
				peak := &latePeak
				peak.enter()
				defer peak.exit()
				time.Sleep(20 * time.Millisecond) // 放大重叠窗口
			}
			started <- id
			<-release
			return nil, nil
		}}
	}
	var handles []*Handle
	for _, id := range []string{"a", "b"} {
		h, _ := rt.Submit(mk(id, false))
		handles = append(handles, h)
	}
	if got := waitStarted(started, 2, 2*time.Second); got != 2 {
		t.Fatalf("初始应有 2 个任务在执行, 实际 %d", got)
	}
	// 调小到 1 之后再提交两个任务，它们必须串行执行。
	if err := rt.UpdateConfig(ConfigUpdate{MaxConcurrency: intPtr(1)}); err != nil {
		t.Fatalf("调小并发上限被拒: %v", err)
	}
	for _, id := range []string{"c", "d"} {
		h, _ := rt.Submit(mk(id, true))
		handles = append(handles, h)
	}
	close(release)
	for _, h := range handles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("任务 %s 终态 %s, 期望 Completed", res.TaskID, res.Status)
		}
	}
	t.Logf("输入: 2 个执行中任务 + 调小到 1 后提交 2 个任务, 上限 2 -> 1")
	t.Logf("判定: 调小后新任务的并发峰值=%d (期望 <=1)", latePeak.max.Load())
	if latePeak.max.Load() > 1 {
		t.Fatalf("调小上限后新任务峰值并发=%d, 超过新上限 1", latePeak.max.Load())
	}
}

// TestUpdateGroupQuotas 验证：运行期新增分组配额、调大默认配额、
// 去除分组配额（回落默认）都立即生效，且任何时刻不越额。
func TestUpdateGroupQuotas(t *testing.T) {
	rt := New(Config{MaxConcurrency: 8, QueueCapacity: 32, DefaultGroupQuota: 1})
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	started := make(chan string, 16)
	var peakA, peakB maxTracker
	mk := func(id, group string, peak *maxTracker) Task {
		return Task{ID: id, Group: group, Func: func(ctx context.Context) (any, error) {
			peak.enter()
			defer peak.exit()
			started <- id
			<-release
			return nil, nil
		}}
	}
	var handles []*Handle
	submit := func(id, group string, peak *maxTracker) {
		h, err := rt.Submit(mk(id, group, peak))
		if err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
		handles = append(handles, h)
	}
	submit("a1", "a", &peakA)
	submit("a2", "a", &peakA)
	submit("b1", "b", &peakB)
	submit("b2", "b", &peakB)

	// 默认配额 1：每组只能跑 1 个。
	if got := waitStarted(started, 4, 100*time.Millisecond); got != 2 {
		t.Fatalf("默认配额=1 时应只有 2 个任务在执行, 实际 %d", got)
	}
	// 给 a 组单独配额 2：a2 应被调度，b2 仍等待。
	if err := rt.UpdateConfig(ConfigUpdate{SetGroupQuotas: map[string]int{"a": 2}}); err != nil {
		t.Fatalf("新增 a 组配额被拒: %v", err)
	}
	if got := waitStarted(started, 1, 2*time.Second); got != 1 {
		t.Fatalf("a 组配额调到 2 后 a2 应开始执行")
	}
	if got := waitStarted(started, 1, 100*time.Millisecond); got != 0 {
		t.Fatalf("b 组仍受默认配额 1 约束, b2 不应开始")
	}
	// 默认配额调到 2：b2 应开始。
	if err := rt.UpdateConfig(ConfigUpdate{DefaultGroupQuota: intPtr(2)}); err != nil {
		t.Fatalf("调大默认配额被拒: %v", err)
	}
	if got := waitStarted(started, 1, 2*time.Second); got != 1 {
		t.Fatalf("默认配额调到 2 后 b2 应开始执行")
	}
	// 去除 a 组单独配额：回落到默认值 2，配置可观测。
	if err := rt.UpdateConfig(ConfigUpdate{RemoveGroupQuotas: []string{"a"}}); err != nil {
		t.Fatalf("去除 a 组配额被拒: %v", err)
	}
	cfg := rt.Config()
	if _, ok := cfg.GroupQuotas["a"]; ok {
		t.Fatalf("a 组单独配额应已被去除, 实际配置 %+v", cfg.GroupQuotas)
	}
	t.Logf("输入: a/b 各 2 个阻塞任务, 默认配额 1->2, a 组单独配额 2->去除")
	t.Logf("判定: a 组峰值=%d (<=2), b 组峰值=%d (<=2), 当前默认配额=%d",
		peakA.max.Load(), peakB.max.Load(), cfg.DefaultGroupQuota)
	if peakA.max.Load() > 2 || peakB.max.Load() > 2 {
		t.Fatalf("分组峰值越额: a=%d b=%d", peakA.max.Load(), peakB.max.Load())
	}
	close(release)
	for _, h := range handles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("任务 %s 终态 %s, 期望 Completed", res.TaskID, res.Status)
		}
	}
}

// TestUpdateGroupQuotaDecrease 验证：调小分组配额后，执行中的任务照常完成，
// 排队任务只有在配额空出后才会被调度。
func TestUpdateGroupQuotaDecrease(t *testing.T) {
	rt := New(Config{MaxConcurrency: 8, QueueCapacity: 32, DefaultGroupQuota: 2})
	defer rt.Shutdown(context.Background())

	started := make(chan string, 8)
	gates := make(map[string]chan struct{})
	mk := func(id string) Task {
		gate := make(chan struct{})
		gates[id] = gate
		return Task{ID: id, Group: "g", Func: func(ctx context.Context) (any, error) {
			started <- id
			<-gate // 每个任务只响应自己的放行信号
			return nil, nil
		}}
	}
	var handles []*Handle
	for _, id := range []string{"g1", "g2", "g3"} {
		h, err := rt.Submit(mk(id))
		if err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
		handles = append(handles, h)
	}
	if got := waitStarted(started, 2, 2*time.Second); got != 2 {
		t.Fatalf("配额=2 时应有 2 个任务在执行, 实际 %d", got)
	}
	// 配额 2 -> 1：放行一个后，g3 不得立即补上（在跑数仍等于新配额）。
	if err := rt.UpdateConfig(ConfigUpdate{DefaultGroupQuota: intPtr(1)}); err != nil {
		t.Fatalf("调小默认配额被拒: %v", err)
	}
	close(gates["g1"])
	if got := waitStarted(started, 1, 150*time.Millisecond); got != 0 {
		t.Fatalf("配额调小到 1 后, 仍有 1 个在执行, g3 不应开始")
	}
	close(gates["g2"])
	if got := waitStarted(started, 1, 2*time.Second); got != 1 {
		t.Fatalf("在跑数降到 0 后 g3 应被调度")
	}
	close(gates["g3"])
	for _, h := range handles {
		if res := h.Wait(); res.Status != StatusCompleted {
			t.Fatalf("任务 %s 终态 %s, 期望 Completed", res.TaskID, res.Status)
		}
	}
	t.Logf("输入: 3 个阻塞任务同组, 默认配额 2 -> 1, 逐个放行")
	t.Logf("判定: 调小后在跑数回落到 0 之前 g3 未开始, 最终全部 Completed")
}

// TestUpdateConfigInvalid 验证：非法或无意义的调整被整体拒绝，
// 且运行时配置保持原样（原子性）。
func TestUpdateConfigInvalid(t *testing.T) {
	rt := New(Config{
		MaxConcurrency:    2,
		QueueCapacity:     16,
		DefaultGroupQuota: 1,
		GroupQuotas:       map[string]int{"x": 3},
	})
	defer rt.Shutdown(context.Background())
	before := rt.Config()

	cases := []struct {
		name   string
		update ConfigUpdate
	}{
		{"并发上限为0", ConfigUpdate{MaxConcurrency: intPtr(0)}},
		{"并发上限为负", ConfigUpdate{MaxConcurrency: intPtr(-2)}},
		{"默认配额为负", ConfigUpdate{DefaultGroupQuota: intPtr(-1)}},
		{"分组配额为0", ConfigUpdate{SetGroupQuotas: map[string]int{"y": 0}}},
		{"分组配额为负", ConfigUpdate{SetGroupQuotas: map[string]int{"y": -3}}},
		{"去除不存在的分组配额", ConfigUpdate{RemoveGroupQuotas: []string{"nope"}}},
		{"同组既设又删", ConfigUpdate{
			SetGroupQuotas:    map[string]int{"x": 5},
			RemoveGroupQuotas: []string{"x"},
		}},
		{"合法项与非法项混合", ConfigUpdate{
			MaxConcurrency: intPtr(9),
			SetGroupQuotas: map[string]int{"y": -1},
		}},
	}
	for _, c := range cases {
		err := rt.UpdateConfig(c.update)
		t.Logf("用例 %q: err=%v", c.name, err)
		if !errors.Is(err, ErrInvalidConfigUpdate) {
			t.Fatalf("用例 %q 应返回 ErrInvalidConfigUpdate, 实际 %v", c.name, err)
		}
	}
	after := rt.Config()
	t.Logf("判定: 拒绝前后配置一致? before=%+v after=%+v", before, after)
	if after.MaxConcurrency != before.MaxConcurrency ||
		after.DefaultGroupQuota != before.DefaultGroupQuota ||
		len(after.GroupQuotas) != len(before.GroupQuotas) ||
		after.GroupQuotas["x"] != 3 {
		t.Fatalf("非法调整污染了配置: before=%+v after=%+v", before, after)
	}
}

// TestUpdateConfigAfterShutdown 验证：关停后调整配置返回 ErrRuntimeClosed。
func TestUpdateConfigAfterShutdown(t *testing.T) {
	rt := New(Config{MaxConcurrency: 2, QueueCapacity: 16})
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	err := rt.UpdateConfig(ConfigUpdate{MaxConcurrency: intPtr(4)})
	t.Logf("关停后调参: err=%v", err)
	if !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("关停后调参应返回 ErrRuntimeClosed, 实际 %v", err)
	}
}
