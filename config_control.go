package taskrt

import (
	"errors"
	"fmt"
)

// 运行期控制类操作的错误哨兵，调用方可用 errors.Is 精确判定。
var (
	// ErrInvalidMaxConcurrency 表示把全局并发上限改为非正数。
	ErrInvalidMaxConcurrency = errors.New("taskrt: max concurrency must be positive")
	// ErrInvalidQuota 表示把某个配额改为负数（0 表示不限制，合法）。
	ErrInvalidQuota = errors.New("taskrt: group quota must be non-negative")
	// ErrQuotaConflict 表示同一分组在一次变更中同时出现在
	// GroupQuotas 与 RemoveGroupQuotas 里，语义冲突。
	ErrQuotaConflict = errors.New("taskrt: group appears in both set and remove quota lists")
	// ErrGroupPaused 表示对已暂停的分组再次执行暂停。
	ErrGroupPaused = errors.New("taskrt: group already paused")
	// ErrGroupNotPaused 表示对未暂停的分组执行恢复。
	ErrGroupNotPaused = errors.New("taskrt: group is not paused")
	// ErrControlRejected 表示关停流程已开始，运行期控制操作被拒绝。
	ErrControlRejected = errors.New("taskrt: control operation rejected after shutdown started")
)

// ConfigUpdate 描述一次运行期配置变更。零值字段表示该项不变更。
// 一次变更整体校验、整体生效，不会出现半更新状态。
type ConfigUpdate struct {
	// MaxConcurrency 为正数时，将全局并发上限改为该值。
	MaxConcurrency int
	// DefaultGroupQuota 非 nil 时，将分组默认配额改为该值（0 表示不限制）。
	DefaultGroupQuota *int
	// GroupQuotas 为非 nil 时，其中每个分组的单独配额被设置/覆盖（0 表示不限制）。
	GroupQuotas map[string]int
	// RemoveGroupQuotas 中的分组将移除单独配额，回落到默认配额。
	RemoveGroupQuotas []string
}

// UpdateConfig 在运行期原子地调整并发上限与分组配额。
//
// 仅支持调整 MaxConcurrency、DefaultGroupQuota、各分组单独配额；
// 排队容量、老化间隔等构造期参数不可运行期变更。变更整体校验、整体
// 生效：任一字段非法即整体拒绝，运行时配置保持原样，不会出现半更新。
//
// 生效后正在执行的任务不受影响、照常跑完；调度器立即按新配置工作。
// 把上限调小时不会中断在执行任务，因此在执行任务自然收敛前，实际
// 并发数可能瞬时高于新上限，但调度器不会再放行任何会突破新上限的
// 新任务。关停流程开始后（Draining/Closed）调用将返回
// ErrControlRejected。
func (r *Runtime) UpdateConfig(u ConfigUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != stateActive {
		return ErrControlRejected
	}

	// 1) 逐项校验，构造候选配置；校验期间只读快照，不触碰 r.cfg。
	next := r.cfg
	if u.MaxConcurrency != 0 {
		if u.MaxConcurrency < 0 {
			return ErrInvalidMaxConcurrency
		}
		next.MaxConcurrency = u.MaxConcurrency
	}
	if u.DefaultGroupQuota != nil {
		if *u.DefaultGroupQuota < 0 {
			return fmt.Errorf("%w: default quota %d", ErrInvalidQuota, *u.DefaultGroupQuota)
		}
		next.DefaultGroupQuota = *u.DefaultGroupQuota
	}

	conflict := make(map[string]struct{}, len(u.GroupQuotas)+len(u.RemoveGroupQuotas))
	if u.GroupQuotas != nil || u.RemoveGroupQuotas != nil {
		next.GroupQuotas = cloneQuotaMap(r.cfg.GroupQuotas)
		if next.GroupQuotas == nil {
			next.GroupQuotas = make(map[string]int)
		}
		for g, q := range u.GroupQuotas {
			if q < 0 {
				return fmt.Errorf("%w: group %q quota %d", ErrInvalidQuota, g, q)
			}
			conflict[g] = struct{}{}
		}
		for _, g := range u.RemoveGroupQuotas {
			if _, dup := conflict[g]; dup {
				return fmt.Errorf("%w: group %q", ErrQuotaConflict, g)
			}
			conflict[g] = struct{}{}
		}
		// 2) 全部校验通过后才落地修改。
		for g, q := range u.GroupQuotas {
			next.GroupQuotas[g] = q
		}
		for _, g := range u.RemoveGroupQuotas {
			delete(next.GroupQuotas, g)
		}
		if len(next.GroupQuotas) == 0 {
			next.GroupQuotas = nil
		}
	}

	r.cfg = next
	r.cond.Broadcast() // 立即唤醒调度器按新配置决策
	return nil
}

// PauseGroup 暂停指定分组。
//
// 暂停只影响调度决策：该分组已在执行的任务照常跑完，排队中的任务在
// 恢复前不得开始执行；其他分组的调度完全不受影响。暂停期间提交、
// 取消、超时的语义与正常运行一致（提交照常入队并受队列容量约束，
// 排队任务仍可被取消并立即得到 Canceled 终态）。
// 对已暂停的分组再次暂停返回 ErrGroupPaused；关停流程开始后返回
// ErrControlRejected。
func (r *Runtime) PauseGroup(group string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != stateActive {
		return ErrControlRejected
	}
	if _, ok := r.paused[group]; ok {
		return fmt.Errorf("%w: group %q", ErrGroupPaused, group)
	}
	r.paused[group] = struct{}{}
	return nil
}

// ResumeGroup 恢复指定分组：其排队任务立即重新参与公平调度，
// 并遵守恢复当时生效的配额。对未暂停的分组调用返回
// ErrGroupNotPaused；关停流程开始后返回 ErrControlRejected
// （关停的优雅收敛阶段会自动放开暂停，无需也不能手动恢复）。
func (r *Runtime) ResumeGroup(group string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != stateActive {
		return ErrControlRejected
	}
	if _, ok := r.paused[group]; !ok {
		return fmt.Errorf("%w: group %q", ErrGroupNotPaused, group)
	}
	delete(r.paused, group)
	r.cond.Broadcast() // 立即唤醒调度器，被暂停分组的任务重新参与选择
	return nil
}

// IsGroupPaused 返回指定分组当前是否处于暂停状态。
func (r *Runtime) IsGroupPaused(group string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.paused[group]
	return ok
}

// CurrentConfig 返回当前生效配置的深拷贝。
func (r *Runtime) CurrentConfig() Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := r.cfg
	cp.GroupQuotas = cloneQuotaMap(r.cfg.GroupQuotas)
	return cp
}
