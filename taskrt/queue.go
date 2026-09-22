package taskrt

import (
	"container/list"
	"sync"
)

// queuedTask 队列中的任务项。
type queuedTask struct {
	task *Task
	h    *Handle
	seq  uint64 // 提交序号，用于同优先级 FIFO
}

// schedQueue 支持优先级与分组公平调度的内部队列。
// 调度规则：默认取最高非空优先级档位的队首任务；
// 每连续调度 fairnessWeight 个高优先级任务后，强制让位一次
// 给最低非空优先级档位，保证低优先级不被饿死。
// 分组配额由调用方通过 allow 谓词注入，配额用尽的分组本轮被跳过。
type schedQueue struct {
	mu   sync.Mutex
	cond *sync.Cond

	levels [priorityCount]*list.List // 每优先级一条 FIFO 队列
	size   int
	seq    uint64
	// inTransit 已被调度但尚未开始执行的任务数。
	// 容量核算口径为 size+inTransit，即"已接纳未执行"的任务总数，
	// 避免任务在投递途中脱离队列统计导致超容。
	inTransit int

	fairnessWeight int
	sinceYield     int // 距上次让位已连续调度的高优先级任务数
	closed         bool
}

func newSchedQueue(fairnessWeight int) *schedQueue {
	if fairnessWeight <= 0 {
		fairnessWeight = 8
	}
	q := &schedQueue{fairnessWeight: fairnessWeight}
	for i := range q.levels {
		q.levels[i] = list.New()
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// pushResult 入队结果。
type pushResult int

const (
	pushed pushResult = iota
	pushFull
	pushClosed
)

// push 入队，返回全局序号与结果；maxLen>0 时限制队列长度。
// 容量检查与入队在同一临界区内完成，避免竞态下超容。
func (q *schedQueue) push(t *Task, h *Handle, maxLen int) (uint64, pushResult) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, pushClosed
	}
	if maxLen > 0 && q.size+q.inTransit >= maxLen {
		return 0, pushFull
	}
	p := t.Priority
	if p < 0 {
		p = 0
	}
	if p >= priorityCount {
		p = priorityCount - 1
	}
	q.seq++
	q.levels[p].PushBack(&queuedTask{task: t, h: h, seq: q.seq})
	q.size++
	q.cond.Signal()
	return q.seq, pushed
}

// pop 取出一个可调度的任务；allow 报告某分组当前是否被配额放行。
// 队列为空时阻塞，直至有任务入队或队列关闭；关闭且排空后返回 nil。
func (q *schedQueue) pop(allow func(group string) bool) *queuedTask {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if qt := q.pickLocked(allow); qt != nil {
			q.inTransit++
			return qt
		}
		if q.closed && q.size == 0 {
			return nil
		}
		q.cond.Wait()
	}
}

// pickLocked 尝试按调度规则取任务，无可调度项时返回 nil。
func (q *schedQueue) pickLocked(allow func(group string) bool) *queuedTask {
	if q.size == 0 {
		return nil
	}
	// 防饥饿让位：连续服务 fairnessWeight 个高优先级任务后，
	// 若存在更低优先级任务则强制服务最低非空档位。
	if q.sinceYield >= q.fairnessWeight {
		if qt := q.takeFromLocked(q.lowestNonEmptyLocked(), allow); qt != nil {
			q.sinceYield = 0
			return qt
		}
	}
	// 常规路径：从最高非空优先级取。
	if qt := q.takeFromLocked(q.highestNonEmptyLocked(), allow); qt != nil {
		q.sinceYield++
		return qt
	}
	// 最高档位被配额阻塞时，降级尝试其它档位，避免整体停摆。
	for p := int(priorityCount) - 2; p >= 0; p-- {
		if qt := q.takeFromLocked(Priority(p), allow); qt != nil {
			return qt
		}
	}
	return nil
}

func (q *schedQueue) highestNonEmptyLocked() Priority {
	for p := int(priorityCount) - 1; p >= 0; p-- {
		if q.levels[p].Len() > 0 {
			return Priority(p)
		}
	}
	return -1
}

func (q *schedQueue) lowestNonEmptyLocked() Priority {
	for p := 0; p < int(priorityCount); p++ {
		if q.levels[p].Len() > 0 {
			return Priority(p)
		}
	}
	return -1
}

// takeFromLocked 从指定档位取出第一个被 allow 放行的任务。
func (q *schedQueue) takeFromLocked(p Priority, allow func(string) bool) *queuedTask {
	if p < 0 {
		return nil
	}
	l := q.levels[p]
	for e := l.Front(); e != nil; e = e.Next() {
		qt := e.Value.(*queuedTask)
		if allow == nil || allow(qt.task.Group) {
			l.Remove(e)
			q.size--
			return qt
		}
	}
	return nil
}

// remove 摘除指定任务（用于取消排队中的任务），成功返回 true。
func (q *schedQueue) remove(h *Handle) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for p := range q.levels {
		l := q.levels[p]
		for e := l.Front(); e != nil; e = e.Next() {
			if e.Value.(*queuedTask).h == h {
				l.Remove(e)
				q.size--
				return true
			}
		}
	}
	return false
}

// drain 取出全部剩余任务（关停时用于统一取消）。
func (q *schedQueue) drain() []*queuedTask {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []*queuedTask
	for p := range q.levels {
		l := q.levels[p]
		for e := l.Front(); e != nil; {
			next := e.Next()
			out = append(out, e.Value.(*queuedTask))
			l.Remove(e)
			e = next
		}
	}
	q.size = 0
	return out
}

// leaveTransit 标记一个已调度任务开始执行（不再占用排队容量）。
func (q *schedQueue) leaveTransit() {
	q.mu.Lock()
	q.inTransit--
	q.mu.Unlock()
}

// wake 唤醒调度等待者（配额释放后重新评估可调度任务）。
func (q *schedQueue) wake() {
	q.mu.Lock()
	q.mu.Unlock()
	q.cond.Broadcast()
}

// close 关闭队列并唤醒所有等待者。
func (q *schedQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// len 返回当前排队任务数。
func (q *schedQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}
