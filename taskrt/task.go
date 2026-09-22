package taskrt

import (
	"context"
	"sync/atomic"
	"time"
)

// Priority 任务优先级，数值越大优先级越高。
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	priorityCount // 内部：优先级档位数
)

// Status 任务生命周期状态。
type Status int

const (
	StatusQueued Status = iota
	StatusRunning
	StatusCompleted
	StatusFailed
	StatusCancelled
	StatusTimeout
)

func (s Status) String() string {
	switch s {
	case StatusQueued:
		return "queued"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	case StatusTimeout:
		return "timeout"
	}
	return "unknown"
}

// TaskFunc 任务体。ctx 在任务被取消、超时或运行时关停时关闭。
type TaskFunc func(ctx context.Context) error

// Task 描述一个待提交的任务。
type Task struct {
	Group    string        // 归属分组，空串归入默认分组
	Priority Priority      // 调度优先级
	Timeout  time.Duration // 单任务执行超时，<=0 表示不限制
	Func     TaskFunc
}

// Result 任务的最终结论，每个被接纳的任务恰好产生一次。
type Result struct {
	Status Status
	Err    error // 业务失败原因；超时/取消时为对应语义错误
}

// Handle 提交成功后返回的句柄，用于取消与等待结果。
type Handle struct {
	done      chan Result
	cancel    context.CancelFunc
	status    atomic.Int32
	ctx       context.Context
	settled   chan struct{} // finalize 时关闭，用于释放看护协程
	finalized atomic.Bool   // 保证结论恰好落地一次
}

// Done 返回结果通道，任务终结时恰好收到一个 Result 后关闭。
func (h *Handle) Done() <-chan Result { return h.done }

// Cancel 请求取消任务；已终结的任务调用为空操作。
func (h *Handle) Cancel() {
	if h.cancel != nil {
		h.cancel()
	}
}

// Status 返回任务当前状态。
func (h *Handle) Status() Status { return Status(h.status.Load()) }
