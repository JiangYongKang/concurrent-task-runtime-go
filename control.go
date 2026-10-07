package taskrt

import (
	"errors"
	"fmt"
)

// ErrInvalidConfigUpdate 表示运行期配置调整非法或无意义，已被整体拒绝。
var ErrInvalidConfigUpdate = errors.New("taskrt: invalid config update")

// ErrRuntimeClosed 表示运行时已开始或完成关停，不再接受配置调整。
var ErrRuntimeClosed = errors.New("taskrt: runtime is shutting down or closed")

// ConfigUpdate 描述一次运行期配置调整。所有字段都是可选的：
// 只调整被指定的项，未指定的项保持原值。调整是原子的：
// 任一项非法则整次调整被拒绝，运行时保持调整前的配置。
type ConfigUpdate struct {
	MaxConcurrency    *int           // 非 nil 时调整并发执行上限，必须 > 0
	DefaultGroupQuota *int           // 非 nil 时调整默认分组配额，必须 >= 0（0 表示不限）
	SetGroupQuotas    map[string]int // 新增或覆盖指定分组的配额，值必须 > 0
	RemoveGroupQuotas []string       // 去除指定分组的单独配额（回落到默认配额），目标必须存在
}

// UpdateConfig 在不重启运行时的前提下原子地应用配置调整。
// 校验失败时返回可 errors.Is 判定为 ErrInvalidConfigUpdate 的错误，
// 运行时保持调整前的配置；关停开始后返回 ErrRuntimeClosed。
// 正在执行的任务不受影响；后续调度立即按新配置执行，
// 任何时刻都不会突破调整后的并发上限与分组配额。
func (r *Runtime) UpdateConfig(u ConfigUpdate) error {
	if err := validateUpdate(u); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != stateActive {
		return ErrRuntimeClosed
	}
	// 依赖当前配置的校验也全部前置，保证任何失败都发生在修改之前。
	for _, g := range u.RemoveGroupQuotas {
		if _, ok := r.cfg.GroupQuotas[g]; !ok {
			return fmt.Errorf("%w: group %q has no explicit quota to remove", ErrInvalidConfigUpdate, g)
		}
	}

	if u.MaxConcurrency != nil {
		r.cfg.MaxConcurrency = *u.MaxConcurrency
	}
	if u.DefaultGroupQuota != nil {
		r.cfg.DefaultGroupQuota = *u.DefaultGroupQuota
	}
	if len(u.SetGroupQuotas) > 0 && r.cfg.GroupQuotas == nil {
		r.cfg.GroupQuotas = make(map[string]int, len(u.SetGroupQuotas))
	}
	for g, q := range u.SetGroupQuotas {
		r.cfg.GroupQuotas[g] = q
	}
	for _, g := range u.RemoveGroupQuotas {
		delete(r.cfg.GroupQuotas, g)
	}

	// 上限或配额可能放宽，唤醒调度协程重新评估队列。
	r.cond.Broadcast()
	return nil
}

// validateUpdate 做与运行时状态无关的纯校验，保证非法调整在加锁前即被拒绝。
func validateUpdate(u ConfigUpdate) error {
	if u.MaxConcurrency != nil && *u.MaxConcurrency <= 0 {
		return fmt.Errorf("%w: MaxConcurrency must be > 0, got %d", ErrInvalidConfigUpdate, *u.MaxConcurrency)
	}
	if u.DefaultGroupQuota != nil && *u.DefaultGroupQuota < 0 {
		return fmt.Errorf("%w: DefaultGroupQuota must be >= 0 (0 = unlimited), got %d", ErrInvalidConfigUpdate, *u.DefaultGroupQuota)
	}
	for g, q := range u.SetGroupQuotas {
		if q <= 0 {
			return fmt.Errorf("%w: quota for group %q must be > 0, got %d (use RemoveGroupQuotas to drop an override)", ErrInvalidConfigUpdate, g, q)
		}
	}
	for _, g := range u.RemoveGroupQuotas {
		if _, conflict := u.SetGroupQuotas[g]; conflict {
			return fmt.Errorf("%w: group %q appears in both SetGroupQuotas and RemoveGroupQuotas", ErrInvalidConfigUpdate, g)
		}
	}
	return nil
}

// Config 返回当前生效的配置快照。
func (r *Runtime) Config() Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	cfg := r.cfg
	if r.cfg.GroupQuotas != nil {
		cfg.GroupQuotas = make(map[string]int, len(r.cfg.GroupQuotas))
		for g, q := range r.cfg.GroupQuotas {
			cfg.GroupQuotas[g] = q
		}
	}
	return cfg
}

// PauseGroup 暂停指定分组：其排队任务在恢复前不得开始执行，
// 已在执行的任务自然结束；其它分组照常调度。幂等。
// 暂停不影响提交、取消与超时语义。关停进入收敛阶段后，暂停标记
// 不再拦阻调度：该分组的排队任务仍受并发上限与分组配额约束、
// 与其它任务一起被尽快收敛，跑不完的才在强制收敛时终结为 Canceled。
func (r *Runtime) PauseGroup(group string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pausedGroups[group] = true
}

// ResumeGroup 恢复指定分组：其排队任务重新参与公平调度，
// 并遵守恢复当时生效的配额。幂等。
func (r *Runtime) ResumeGroup(group string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pausedGroups[group] {
		delete(r.pausedGroups, group)
		// 恢复后可能有任务立即可调度，唤醒调度协程。
		r.cond.Broadcast()
	}
}

// IsPaused 报告指定分组当前是否处于暂停状态。
func (r *Runtime) IsPaused(group string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pausedGroups[group]
}
