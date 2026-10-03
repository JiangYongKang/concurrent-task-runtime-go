package taskrt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// 任务终态原因，用于 context.Cause 区分取消来源。
var (
	errUserCanceled   = errors.New("taskrt: task canceled by caller")
	errTaskTimeout    = errors.New("taskrt: task execution timed out")
	errForcedShutdown = errors.New("taskrt: runtime shutdown forced cancellation")
)

// ErrShutdownTimeout 表示优雅关停未能在时限内收敛。
var ErrShutdownTimeout = errors.New("taskrt: shutdown timed out with tasks still running")

const (
	stateActive = iota
	stateDraining
	stateClosed
)

// entry 是运行时对任务的内部表示。
type entry struct {
	task       Task
	seq        uint64
	enqueuedAt time.Time
	startedAt  time.Time
	handle     *Handle

	started     bool
	done        bool // 已到达终态（exactly-once 保证）
	cancelCause context.CancelCauseFunc
	timer       *time.Timer
}

// Runtime 是进程内并发任务运行时。
//
// 调度模型：单一调度协程在互斥锁下做调度决策（O(队列长度) 扫描，
// 队列长度受 QueueCapacity 约束，故开销有界），工作协程数不超过
// MaxConcurrency。有效优先级 = 基础优先级 + 等待时长/AgingInterval，
// 保证低优先级任务随等待时间提升优先级，不会被高优先级饿死。
type Runtime struct {
	cfg     Config
	metrics Metrics

	mu           sync.Mutex
	cond         *sync.Cond
	queue        []*entry
	groupQueued  map[string]int
	groupRunning map[string]int
	pausedGroups map[string]bool
	runningSet   map[*entry]struct{}
	running      int
	seq          uint64
	state        int
	drained      chan struct{}
	wg           sync.WaitGroup
}

// New 创建并启动运行时。
func New(cfg Config) *Runtime {
	cfg = cfg.withDefaults()
	r := &Runtime{
		cfg:          cfg,
		queue:        nil,
		groupQueued:  make(map[string]int),
		groupRunning: make(map[string]int),
		pausedGroups: make(map[string]bool),
		runningSet:   make(map[*entry]struct{}),
		drained:      make(chan struct{}),
	}
	r.cond = sync.NewCond(&r.mu)
	r.wg.Add(1)
	go r.dispatchLoop()
	return r
}

// Submit 提交任务。容量不足或已关停时返回可区分的 *RejectError，
// 被拒绝的任务不会入队、不会执行、不计入执行统计。
func (r *Runtime) Submit(t Task) (*Handle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != stateActive {
		r.metrics.rejected.Add(1)
		return nil, &RejectError{Reason: RejectShutdown, Group: t.Group}
	}
	if len(r.queue) >= r.cfg.QueueCapacity {
		r.metrics.rejected.Add(1)
		return nil, &RejectError{Reason: RejectQueueFull, Group: t.Group}
	}
	if lim := r.cfg.GroupQueueLimit; lim > 0 && r.groupQueued[t.Group] >= lim {
		r.metrics.rejected.Add(1)
		return nil, &RejectError{Reason: RejectGroupQueueFull, Group: t.Group}
	}

	r.seq++
	if t.ID == "" {
		t.ID = fmt.Sprintf("task-%d", r.seq)
	}
	e := &entry{task: t, seq: r.seq, enqueuedAt: time.Now()}
	h := &Handle{done: make(chan struct{})}
	h.cancel = func() { r.cancelEntry(e, errUserCanceled) }
	e.handle = h

	r.queue = append(r.queue, e)
	r.groupQueued[t.Group]++
	r.metrics.accepted.Add(1)
	r.metrics.queued.Add(1)
	r.cond.Signal()
	return h, nil
}

// Metrics 返回当前统计快照。
func (r *Runtime) Metrics() Snapshot {
	return r.metrics.Snapshot()
}

// dispatchLoop 是唯一做调度决策的协程。
func (r *Runtime) dispatchLoop() {
	defer r.wg.Done()
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if r.state == stateClosed {
			return
		}
		if r.state == stateDraining && len(r.queue) == 0 && r.running == 0 {
			r.state = stateClosed
			close(r.drained)
			r.cond.Broadcast()
			return
		}
		if r.running < r.cfg.MaxConcurrency {
			if e := r.pickLocked(); e != nil {
				r.startLocked(e)
				continue
			}
		}
		r.cond.Wait()
	}
}

// pickLocked 扫描队列，选出有效优先级最高且分组配额未用尽的任务。
// 复杂度 O(len(queue))，队列长度受 QueueCapacity 约束。
func (r *Runtime) pickLocked() *entry {
	now := time.Now()
	best := -1
	bestEff := 0
	for i, e := range r.queue {
		if r.pausedGroups[e.task.Group] {
			continue // 被暂停的分组在恢复前不得开始执行
		}
		if q := r.cfg.quotaFor(e.task.Group); q > 0 && r.groupRunning[e.task.Group] >= q {
			continue // 配额用尽的分组不得越额执行
		}
		eff := e.task.Priority + int(now.Sub(e.enqueuedAt)/r.cfg.AgingInterval)
		if best == -1 || eff > bestEff {
			best = i
			bestEff = eff
		}
	}
	if best == -1 {
		return nil
	}
	e := r.queue[best]
	r.queue = append(r.queue[:best], r.queue[best+1:]...)
	return e
}

// startLocked 将任务投入执行。
func (r *Runtime) startLocked(e *entry) {
	e.started = true
	e.startedAt = time.Now()
	r.running++
	r.groupQueued[e.task.Group]--
	r.groupRunning[e.task.Group]++
	r.runningSet[e] = struct{}{}
	r.metrics.queued.Add(-1)
	r.metrics.running.Add(1)
	r.metrics.started.Add(1)

	ctx, cancel := context.WithCancelCause(context.Background())
	e.cancelCause = cancel
	if e.task.Timeout > 0 {
		e.timer = time.AfterFunc(e.task.Timeout, func() { cancel(errTaskTimeout) })
	}
	r.wg.Add(1)
	go r.run(e, ctx)
}

// run 执行任务并按取消原因判定终态。
func (r *Runtime) run(e *entry, ctx context.Context) {
	defer r.wg.Done()

	value, ferr := func() (v any, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("taskrt: task panicked: %v", p)
			}
		}()
		return e.task.Func(ctx)
	}()
	if e.timer != nil {
		e.timer.Stop()
	}
	// 在执行结束这一刻确定取消原因；之后到达的取消不影响结论。
	cause := context.Cause(ctx)

	var status Status
	switch {
	case cause == errTaskTimeout:
		status = StatusTimedOut
	case cause != nil:
		status = StatusCanceled
	case ferr != nil:
		status = StatusFailed
	default:
		status = StatusCompleted
	}

	r.mu.Lock()
	e.done = true
	r.running--
	delete(r.runningSet, e)
	r.groupRunning[e.task.Group]--
	r.metrics.running.Add(-1)
	r.mu.Unlock()
	r.cond.Broadcast()

	// 被取消或超时的任务：丢弃结果值，不触发回调。
	res := Result{
		TaskID:     e.task.ID,
		Group:      e.task.Group,
		Status:     status,
		EnqueuedAt: e.enqueuedAt,
		StartedAt:  e.startedAt,
		FinishedAt: time.Now(),
	}
	switch status {
	case StatusCompleted:
		res.Value = value
		r.metrics.completed.Add(1)
	case StatusFailed:
		res.Err = ferr
		r.metrics.failed.Add(1)
	case StatusTimedOut:
		res.Err = errTaskTimeout
		r.metrics.timedOut.Add(1)
	case StatusCanceled:
		res.Err = cause
		r.metrics.canceled.Add(1)
	}
	e.handle.result = res
	close(e.handle.done)
	if (status == StatusCompleted || status == StatusFailed) && e.task.OnComplete != nil {
		e.task.OnComplete(res)
	}
}

// cancelEntry 取消任务：排队中的直接移除并终结，执行中的取消其 ctx。
func (r *Runtime) cancelEntry(e *entry, cause error) {
	r.mu.Lock()
	if e.done {
		r.mu.Unlock()
		return
	}
	if e.started {
		if e.cancelCause != nil {
			e.cancelCause(cause)
		}
		r.mu.Unlock()
		return
	}
	e.done = true
	r.removeQueuedLocked(e)
	r.groupQueued[e.task.Group]--
	r.metrics.queued.Add(-1)
	r.metrics.canceled.Add(1)
	res := Result{
		TaskID:     e.task.ID,
		Group:      e.task.Group,
		Status:     StatusCanceled,
		Err:        cause,
		EnqueuedAt: e.enqueuedAt,
		FinishedAt: time.Now(),
	}
	r.mu.Unlock()

	e.handle.result = res
	close(e.handle.done)
	r.cond.Broadcast()
}

func (r *Runtime) removeQueuedLocked(e *entry) {
	for i, q := range r.queue {
		if q == e {
			r.queue = append(r.queue[:i], r.queue[i+1:]...)
			return
		}
	}
}

// Shutdown 优雅关停：立即拒收新任务；已排队与执行中的任务在
// ShutdownTimeout 内继续收敛；超时后强制取消排队任务并取消执行中
// 任务的 ctx，再等待一个宽限期。任务体若不响应 ctx 取消，关停会
// 返回 ErrShutdownTimeout（此时无法回收其工作协程，属任务体责任）。
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	switch r.state {
	case stateClosed:
		r.mu.Unlock()
		return nil
	case stateDraining:
		drained := r.drained
		r.mu.Unlock()
		select {
		case <-drained:
			r.wg.Wait()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.state = stateDraining
	r.cond.Broadcast()
	r.mu.Unlock()

	timer := time.NewTimer(r.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-r.drained:
		r.wg.Wait()
		return nil
	case <-timer.C:
	case <-ctx.Done():
	}

	// 强制收敛：取消全部排队任务与执行中任务的 ctx。
	r.forceCancel()

	grace := time.NewTimer(r.cfg.ShutdownTimeout)
	defer grace.Stop()
	select {
	case <-r.drained:
		r.wg.Wait()
		return nil
	case <-grace.C:
		return ErrShutdownTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}

// forceCancel 终结所有排队任务并取消所有执行中任务的 ctx。
func (r *Runtime) forceCancel() {
	r.mu.Lock()
	queued := r.queue
	r.queue = nil
	handles := make([]*Handle, 0, len(queued))
	results := make([]Result, 0, len(queued))
	now := time.Now()
	for _, e := range queued {
		e.done = true
		r.groupQueued[e.task.Group]--
		r.metrics.queued.Add(-1)
		r.metrics.canceled.Add(1)
		handles = append(handles, e.handle)
		results = append(results, Result{
			TaskID:     e.task.ID,
			Group:      e.task.Group,
			Status:     StatusCanceled,
			Err:        errForcedShutdown,
			EnqueuedAt: e.enqueuedAt,
			FinishedAt: now,
		})
	}
	r.groupQueued = make(map[string]int)
	cancels := make([]context.CancelCauseFunc, 0, len(r.runningSet))
	for e := range r.runningSet {
		if e.cancelCause != nil {
			cancels = append(cancels, e.cancelCause)
		}
	}
	r.mu.Unlock()

	for i, h := range handles {
		h.result = results[i]
		close(h.done)
	}
	for _, c := range cancels {
		c(errForcedShutdown)
	}
	r.cond.Broadcast()
}
