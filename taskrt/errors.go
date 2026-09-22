package taskrt

import "fmt"

// RejectReason 区分调度层拒绝任务的原因。
type RejectReason int

const (
	RejectQueueFull    RejectReason = iota // 排队容量已满
	RejectShuttingDown                     // 运行时已关停，不再接收新任务
	RejectGroupQuota                       // 分组配额已用尽
)

func (r RejectReason) String() string {
	switch r {
	case RejectQueueFull:
		return "queue full"
	case RejectShuttingDown:
		return "runtime shutting down"
	case RejectGroupQuota:
		return "group quota exhausted"
	}
	return "unknown"
}

// RejectError 表示任务在调度层被拒绝，Reason 可区分具体原因。
type RejectError struct {
	Reason RejectReason
	Group  string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("task rejected: %s (group=%q)", e.Reason, e.Group)
}
