package taskrt

import (
	"context"
	"errors"
	"fmt"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ErrForcedShutdown 表示任务因关停超时而被强制结束。
var ErrForcedShutdown = errors.New("task aborted by forced shutdown")

// Config 运行时配置。
type Config struct {
	Workers       int                   // 并发执行上限，<=0 时取 NumCPU
	QueueCapacity int                   // 全局排队容量，<=0 时取 1024
	GroupQuotas   map[string]GroupQuota // 分组配额，未配置的分组不受限
	// FairnessWeight 防饥饿：每连续调度 Weight 个高优先级任务后，
	// 强制让位一次给最低非空优先级档位。<=0 时取 8。
	FairnessWeight int
	// ShutdownTimeout 优雅关停时限，超时后强制结束。<=0 表示仅受 Shutdown 入参 ctx 约束。
	ShutdownTimeout time.Duration
}

func (c *Config) normalize() {
	if c.Workers <= 0 {
		c.Workers = goruntime.NumCPU()
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 1024
	}
	if c.FairnessWeight <= 0 {
		c.FairnessWeight = 8
	}
}

// Runtime 进程内并发任务运行时。
type Runtime struct {
	cfg    Config
	stats  *Stats
	queue  *schedQueue
	quota  *quotaTracker
	workCh chan *queuedTask

	shuttingDown atomic.Bool
	forceCh      chan struct{}
	shutdownOnce sync.Once
	forceOnce    sync.Once
	wg           sync.WaitGroup // 调度与工作协程
}

// New 创建并启动运行时。
func New(cfg Config) *Runtime {
	cfg.normalize()
	r := &Runtime{
		cfg:     cfg,
		stats:   &Stats{},
		queue:   newSchedQueue(cfg.FairnessWeight),
		quota:   newQuotaTracker(cfg.GroupQuotas),
		workCh:  make(chan *queuedTask),
		forceCh: make(chan struct{}),
	}
	r.wg.Add(1)
	go r.dispatchLoop()
	for i := 0; i < cfg.Workers; i++ {
		r.wg.Add(1)
		go r.workerLoop()
	}
	return r
}

// Submit 提交任务；被拒绝时返回 *RejectError，可通过 Reason 区分原因。
// 成功时返回的 Handle 可用于取消与等待唯一结论。
func (r *Runtime) Submit(t Task) (*Handle, error) {
	if t.Func == nil {
		return nil, errors.New("taskrt: nil task func")
	}
	if r.shuttingDown.Load() {
		r.stats.rejected.Add(1)
		return nil, &RejectError{Reason: RejectShuttingDown, Group: t.Group}
	}
	if !r.quota.admitQueued(t.Group) {
		r.stats.rejected.Add(1)
		return nil, &RejectError{Reason: RejectGroupQuota, Group: t.Group}
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if t.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, t.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	h := &Handle{
		done:    make(chan Result, 1),
		cancel:  cancel,
		ctx:     ctx,
		settled: make(chan struct{}),
	}
	h.status.Store(int32(StatusQueued))

	_, res := r.queue.push(&t, h, r.cfg.QueueCapacity)
	if res != pushed {
		r.quota.releaseQueued(t.Group)
		r.stats.rejected.Add(1)
		cancel()
		reason := RejectQueueFull
		if res == pushClosed {
			reason = RejectShuttingDown
		}
		return nil, &RejectError{Reason: reason, Group: t.Group}
	}

	r.stats.submitted.Add(1)
	go r.watchCancel(&t, h)
	return h, nil
}

// watchCancel 看护排队中的任务：被取消/超时且仍在队列时，直接出队并终结。
// 已被调度或已终结的任务由执行路径负责结论，看护协程随即退出。
func (r *Runtime) watchCancel(t *Task, h *Handle) {
	select {
	case <-h.ctx.Done():
		if r.queue.remove(h) {
			r.quota.releaseQueued(t.Group)
			r.finalize(h, statusFromCtx(h.ctx), h.ctx.Err())
			return
		}
		// 已出队：等待执行路径给出结论后退出。
		<-h.settled
	case <-h.settled:
	}
}

// dispatchLoop 单调度协程：按优先级+公平规则取任务并投递给工作协程。
func (r *Runtime) dispatchLoop() {
	defer r.wg.Done()
	defer close(r.workCh)
	for {
		qt := r.queue.pop(r.quota.canRun)
		if qt == nil { // 队列已关闭且排空
			return
		}
		g := qt.task.Group
		r.quota.releaseQueued(g)
		if !r.quota.acquire(g) {
			// 单调度协程下不可达；防御性处理，避免状态错乱。
			r.finalize(qt.h, StatusCancelled, errors.New("taskrt: quota acquire failed"))
			continue
		}
		r.workCh <- qt
	}
}

// workerLoop 工作协程：执行被投递的任务。
func (r *Runtime) workerLoop() {
	defer r.wg.Done()
	for qt := range r.workCh {
		r.runTask(qt)
	}
}

// runTask 执行单个任务并给出唯一结论；业务 panic 被隔离为失败。
func (r *Runtime) runTask(qt *queuedTask) {
	t, h := qt.task, qt.h
	r.queue.leaveTransit()
	defer func() {
		r.quota.release(t.Group)
		r.queue.wake() // 释放配额后唤醒调度器重新评估
	}()

	if err := h.ctx.Err(); err != nil { // 出队后、执行前已被取消/超时
		r.finalize(h, statusFromCtx(h.ctx), err)
		return
	}
	h.status.Store(int32(StatusRunning))
	r.stats.started.Add(1)

	resCh := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				resCh <- fmt.Errorf("taskrt: panic in task: %v", p)
			}
		}()
		resCh <- t.Func(h.ctx)
	}()

	select {
	case err := <-resCh:
		if err == nil {
			r.finalize(h, StatusCompleted, nil)
		} else {
			r.finalize(h, StatusFailed, err)
		}
	case <-h.ctx.Done():
		r.finalize(h, statusFromCtx(h.ctx), h.ctx.Err())
	case <-r.forceCh:
		r.finalize(h, StatusCancelled, ErrForcedShutdown)
	}
}

// finalize 落地任务唯一结论并更新统计；并发调用下仅首次生效。
func (r *Runtime) finalize(h *Handle, st Status, err error) {
	if !h.finalized.CompareAndSwap(false, true) {
		return
	}
	h.status.Store(int32(st))
	switch st {
	case StatusCompleted:
		r.stats.completed.Add(1)
	case StatusFailed:
		r.stats.failed.Add(1)
	case StatusCancelled:
		r.stats.cancelled.Add(1)
	case StatusTimeout:
		r.stats.timedOut.Add(1)
	}
	h.done <- Result{Status: st, Err: err}
	close(h.done)
	close(h.settled)
	h.cancel() // 释放定时器等上下文资源
}

func statusFromCtx(ctx context.Context) Status {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return StatusTimeout
	}
	return StatusCancelled
}

// Stats 返回统计快照。
func (r *Runtime) Stats() Snapshot { return r.stats.Snapshot() }

// Shutdown 优雅关停：不再接收新任务，等待已排队与执行中的任务收敛；
// 超过时限（cfg.ShutdownTimeout 或 ctx）后强制终结剩余任务并返回错误。
// 返回后运行时不遗留任何运行时持有的后台协程。
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.shutdownOnce.Do(func() {
		r.shuttingDown.Store(true)
		r.queue.close()
	})
	if r.cfg.ShutdownTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.ShutdownTimeout)
		defer cancel()
	}

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}

	// 强制阶段：排空队列并终结所有排队任务，再通知执行中任务放弃。
	r.forceOnce.Do(func() {
		for _, qt := range r.queue.drain() {
			r.quota.releaseQueued(qt.task.Group)
			r.finalize(qt.h, StatusCancelled, ErrForcedShutdown)
		}
		close(r.forceCh)
	})
	<-done
	return ctx.Err()
}
