package taskrt

import "sync"

// GroupQuota 单个分组的配额配置。
type GroupQuota struct {
	MaxConcurrent int // 该分组最大并发执行数，<=0 表示不限
	MaxQueued     int // 该分组最大排队数，<=0 表示不限
}

// quotaTracker 跟踪各分组在途执行与排队计数，全部方法并发安全。
type quotaTracker struct {
	mu       sync.Mutex
	limits   map[string]GroupQuota
	inFlight map[string]int
	queued   map[string]int
}

func newQuotaTracker(limits map[string]GroupQuota) *quotaTracker {
	cp := make(map[string]GroupQuota, len(limits))
	for g, q := range limits {
		cp[g] = q
	}
	return &quotaTracker{
		limits:   cp,
		inFlight: make(map[string]int),
		queued:   make(map[string]int),
	}
}

func (t *quotaTracker) limit(group string) GroupQuota {
	if q, ok := t.limits[group]; ok {
		return q
	}
	return GroupQuota{} // 未配置的分组不受限
}

// admitQueued 尝试登记一个排队名额，超出 MaxQueued 时返回 false。
func (t *quotaTracker) admitQueued(group string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	lim := t.limit(group)
	if lim.MaxQueued > 0 && t.queued[group] >= lim.MaxQueued {
		return false
	}
	t.queued[group]++
	return true
}

// releaseQueued 释放一个排队名额（任务离队列时调用，恰好一次）。
func (t *quotaTracker) releaseQueued(group string) {
	t.mu.Lock()
	t.queued[group]--
	t.mu.Unlock()
}

// canRun 报告该分组当前是否有并发执行余量（不扣减）。
func (t *quotaTracker) canRun(group string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	lim := t.limit(group)
	return lim.MaxConcurrent <= 0 || t.inFlight[group] < lim.MaxConcurrent
}

// acquire 扣减一个执行名额，无余量时返回 false。
func (t *quotaTracker) acquire(group string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	lim := t.limit(group)
	if lim.MaxConcurrent > 0 && t.inFlight[group] >= lim.MaxConcurrent {
		return false
	}
	t.inFlight[group]++
	return true
}

// release 释放一个执行名额（任务执行结束时调用，恰好一次）。
func (t *quotaTracker) release(group string) {
	t.mu.Lock()
	t.inFlight[group]--
	t.mu.Unlock()
}
