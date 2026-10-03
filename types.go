// Package taskrt 提供进程内并发任务运行时：优先级调度、分组配额、
// 取消与超时、可观测统计、优雅关停，以及运行期动态调参与分组暂停/恢复。
package taskrt

import (
	"context"
	"fmt"
	"time"
)

// Status 表示任务的终态或中间态。
type Status int

const (
	StatusPending   Status = iota // 已入队，等待调度
	StatusRunning                 // 正在执行
	StatusCompleted               // 执行成功
	StatusFailed                  // 业务失败（Func 返回 error）
	StatusCanceled                // 被取消
	StatusTimedOut                // 执行超时
	StatusRejected                // 被调度层拒绝（未被执行）
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "Pending"
	case StatusRunning:
		return "Running"
	case StatusCompleted:
		return "Completed"
	case StatusFailed:
		return "Failed"
	case StatusCanceled:
		return "Canceled"
	case StatusTimedOut:
		return "TimedOut"
	case StatusRejected:
		return "Rejected"
	default:
		return "Unknown"
	}
}

// RejectReason 区分调度层拒绝的原因。
type RejectReason int

const (
	RejectQueueFull      RejectReason = iota + 1 // 全局排队容量已满
	RejectGroupQueueFull                         // 分组排队配额已满
	RejectShutdown                               // 运行时已关停
)

func (r RejectReason) String() string {
	switch r {
	case RejectQueueFull:
		return "QueueFull"
	case RejectGroupQueueFull:
		return "GroupQueueFull"
	case RejectShutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// RejectError 是可区分的拒绝错误，调用方可用 AsReject 判定原因。
type RejectError struct {
	Reason RejectReason
	Group  string
}

func (e *RejectError) Error() string {
	if e.Group != "" {
		return fmt.Sprintf("taskrt: task rejected (%s, group=%q)", e.Reason, e.Group)
	}
	return fmt.Sprintf("taskrt: task rejected (%s)", e.Reason)
}

// AsReject 从 err 中提取 *RejectError。
func AsReject(err error) (*RejectError, bool) {
	for err != nil {
		if re, ok := err.(*RejectError); ok {
			return re, true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}

// Task 描述一个待执行任务。
type Task struct {
	ID       string                                 // 任务标识，为空时由运行时生成
	Group    string                                 // 归属分组，用于配额与公平调度
	Priority int                                    // 基础优先级，越大越优先
	Timeout  time.Duration                          // 执行超时（从开始执行计时），0 表示不限制
	Func     func(ctx context.Context) (any, error) // 任务体
	// OnComplete 仅在任务真实完成（Completed/Failed）后调用；
	// 被取消、超时或拒绝的任务不会触发回调。
	OnComplete func(res Result)
}

// Result 是任务的终态结论，一次性写入、之后只读。
type Result struct {
	TaskID     string
	Group      string
	Status     Status
	Value      any   // 仅 Completed 时有效
	Err        error // Failed 时为业务错误；Canceled/TimedOut/Rejected 时为原因说明
	EnqueuedAt time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

// Config 是运行时配置。
type Config struct {
	MaxConcurrency    int            // 并发执行上限，<=0 时取 4
	QueueCapacity     int            // 全局排队容量，<=0 时取 1024
	AgingInterval     time.Duration  // 每等待该时长有效优先级 +1，防止饥饿；<=0 时取 100ms
	DefaultGroupQuota int            // 分组默认并发配额，<=0 表示不限制
	GroupQuotas       map[string]int // 指定分组的并发配额，覆盖默认值
	GroupQueueLimit   int            // 单分组排队上限，<=0 表示不限制
	ShutdownTimeout   time.Duration  // 优雅关停收敛时限，<=0 时取 5s
}

func (c Config) withDefaults() Config {
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 4
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 1024
	}
	if c.AgingInterval <= 0 {
		c.AgingInterval = 100 * time.Millisecond
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 5 * time.Second
	}
	return c
}

func (c Config) quotaFor(group string) int {
	if c.GroupQuotas != nil {
		if q, ok := c.GroupQuotas[group]; ok {
			return q
		}
	}
	return c.DefaultGroupQuota
}
