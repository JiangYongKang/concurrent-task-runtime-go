package taskrt

import "sync/atomic"

// Metrics 记录运行时统计，全部通过原子操作更新，并发安全。
// 口径约定：
//   - Accepted   = 被接收（入队成功）的任务数
//   - Started    = 真实开始执行的任务数
//   - Completed+Failed+Canceled+TimedOut 中，后三者与 Completed 之和
//     在静默时刻恒等于 Started + CanceledBeforeStart（取消可能发生在执行前）
//   - 每个任务在其生命周期内只被终态计数一次
type Metrics struct {
	accepted  atomic.Int64
	rejected  atomic.Int64
	started   atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
	canceled  atomic.Int64
	timedOut  atomic.Int64
	queued    atomic.Int64 // 当前排队数
	running   atomic.Int64 // 当前执行数
}

// Snapshot 是某一时刻的统计快照。
type Snapshot struct {
	Accepted  int64
	Rejected  int64
	Started   int64
	Completed int64
	Failed    int64
	Canceled  int64
	TimedOut  int64
	Queued    int64
	Running   int64
}

// Snapshot 读取当前统计快照。
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Accepted:  m.accepted.Load(),
		Rejected:  m.rejected.Load(),
		Started:   m.started.Load(),
		Completed: m.completed.Load(),
		Failed:    m.failed.Load(),
		Canceled:  m.canceled.Load(),
		TimedOut:  m.timedOut.Load(),
		Queued:    m.queued.Load(),
		Running:   m.running.Load(),
	}
}
