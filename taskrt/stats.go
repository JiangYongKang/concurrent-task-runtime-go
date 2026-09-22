package taskrt

import "sync/atomic"

// Stats 运行时统计，所有计数器并发安全。
type Stats struct {
	submitted atomic.Int64 // 被接纳进入运行时的任务数
	completed atomic.Int64
	failed    atomic.Int64
	cancelled atomic.Int64
	timedOut  atomic.Int64
	rejected  atomic.Int64
	started   atomic.Int64 // 真实开始执行的次数
}

// Snapshot 统计快照。
type Snapshot struct {
	Submitted int64
	Started   int64
	Completed int64
	Failed    int64
	Cancelled int64
	TimedOut  int64
	Rejected  int64
}

// Snapshot 返回当前统计的一致性快照。
func (s *Stats) Snapshot() Snapshot {
	return Snapshot{
		Submitted: s.submitted.Load(),
		Started:   s.started.Load(),
		Completed: s.completed.Load(),
		Failed:    s.failed.Load(),
		Cancelled: s.cancelled.Load(),
		TimedOut:  s.timedOut.Load(),
		Rejected:  s.rejected.Load(),
	}
}
