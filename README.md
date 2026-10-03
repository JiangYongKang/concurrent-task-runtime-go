# concurrent-task-runtime-go

进程内并发任务运行时：在**优先级、分组配额、取消与关停**等条件下，
保证任务的执行结果与资源占用可预期。仅依赖 Go 标准库。

## 快速开始

```go
rt := taskrt.New(taskrt.Config{
    MaxConcurrency: 8,
    QueueCapacity:  1024,
    GroupQuotas:    map[string]int{"batch": 2},
})

h, err := rt.Submit(taskrt.Task{
    ID:       "job-1",
    Group:    "batch",
    Priority: 5,
    Timeout:  2 * time.Second,
    Func: func(ctx context.Context) (any, error) {
        // 业务逻辑；应响应 ctx 取消
        return "done", nil
    },
    OnComplete: func(res taskrt.Result) { /* 仅 Completed/Failed 触发 */ },
})
if err != nil {
    if re, ok := taskrt.AsReject(err); ok {
        // re.Reason: QueueFull / GroupQueueFull / Shutdown
    }
}
res := h.Wait() // 阻塞到终态
_ = rt.Shutdown(context.Background())
```

## 调度与配额规则

- **并发上限**：同时在执行的任务数不超过 `MaxConcurrency`（默认 4）。
- **排队容量**：全局排队数不超过 `QueueCapacity`（默认 1024）；单分组排队数
  不超过 `GroupQueueLimit`（默认不限）。超限即拒绝，绝不静默丢弃。
- **拒绝可区分**：拒绝返回 `*RejectError`，用 `AsReject` 提取原因：
  `RejectQueueFull`（全局满）、`RejectGroupQueueFull`（分组排队满）、
  `RejectShutdown`（已关停）。被拒绝的任务不入队、不执行、不计入执行统计。
- **优先级 + 老化防饥饿**：调度器每次选取**有效优先级**最高的任务：
  `有效优先级 = Priority + 等待时长 / AgingInterval`（默认 100ms 升 1 级）。
  低优先级任务等待越久优先级越高，因此高优先级流量无法完全饿死低优先级。
  同有效（老化后）有效优先级按入队先后 FIFO。
- **分组配额**：`GroupQuotas[group]`（缺省用 `DefaultGroupQuota`，0 表示不限）
  限制该分组**同时在执行**的任务数。配额用尽的分组任务留在队列中等待，
  不得越额执行；其他分组不受影响。
- **调度复杂度**：调度决策为 O(队列长度) 扫描，队列长度受 `QueueCapacity`
  约束，故开销与内存占用均有界；单调度协程 + 有界工作协程，无额外后台 goroutine。

## 取消、超时与终态

任务终态：`Completed` / `Failed` / `Canceled` / `TimedOut`（`Rejected` 仅出现在
提交返回值中，任务未入队）。

- `Handle.Cancel()`：排队中的任务直接移除并终结为 `Canceled`；执行中的任务
  其 `ctx` 被取消。
- `Task.Timeout`：从**开始执行**计时，超时终态为 `TimedOut`。
- **被取消或超时的任务不产生任何副作用**：结果值被丢弃（`Result.Value == nil`）、
  `OnComplete` 不触发；即使任务体在取消后返回了值也会被运行时丢弃。
- **三类失败区分上报**：业务失败（`Failed`，`Err` 为任务体返回的 error）、
  执行超时（`TimedOut`）、调度层拒绝（提交时返回 `*RejectError`）。
  任务体 panic 被 recover 并记为 `Failed`，不会污染运行时状态。
- 取消与完成的竞争由互斥锁 + `context.Cause` 裁定：每个任务**恰好**到达一个
  终态，`Handle.done` 恰好关闭一次。

## 统计口径

`Runtime.Metrics()` 返回快照，全部经原子操作更新、并发安全：

| 字段 | 口径 |
|---|---|
| `Accepted` | 入队成功的任务数 |
| `Rejected` | 被拒绝的任务数 |
| `Started` | 真实开始执行的任务数（取消发生在执行前则不计） |
| `Completed/Failed/Canceled/TimedOut` | 各终态计数，每任务恰好计一次 |
| `Queued/Running` | 当前排队/执行数 |

静默时刻恒等式：`Accepted = Completed + Failed + Canceled + TimedOut`，
且 `Started = Completed + Failed + 执行中被取消/超时的数量`。

## 运行期动态调参（不重启、不重建）

`UpdateConfig(ConfigUpdate)` 在运行中原子调整调度参数，**不需要也不能**
重建运行时；排队容量、老化间隔等构造期参数不可运行期变更：

```go
defaultQuota := 4
err := rt.UpdateConfig(taskrt.ConfigUpdate{
    MaxConcurrency:    16,               // 正数：调整全局并发上限
    DefaultGroupQuota: &defaultQuota,    // 非 nil：调整默认分组配额（0=不限）
    GroupQuotas:       map[string]int{"batch": 6}, // 设置/覆盖单独配额（0=不限）
    RemoveGroupQuotas: []string{"tmp"},  // 移除单独配额，回落默认配额
})
```

- **字段零值语义**：`MaxConcurrency=0` 表示“该项不变更”；区分“不变更”与
  “设为某值”的指针字段（`DefaultGroupQuota *int`）以及 map/slice 字段同理
  （`nil` 表示不变更，空 map/空 slice 表示清空/移除零项，是合法操作）。
- **整体校验、整体生效**：一次调用内的全部修改先在候选配置上校验，任一项
  非法即整体拒绝，内部配置保持原样，不存在“改了一半”的中间态。
  校验规则：`MaxConcurrency` 必须为正（要表达“不变更”请传 0）；各配额必须
  非负（负数非法，`0` 表示不限制）；同一分组不能在同一次调用里既出现在
  `GroupQuotas` 又出现在 `RemoveGroupQuotas`。
- **错误可判定**（`errors.Is`）：`ErrInvalidMaxConcurrency`、
  `ErrInvalidQuota`、`ErrQuotaConflict`；关停流程开始后调用返回
  `ErrControlRejected`。
- **生效语义**：变更生效后**正在执行的任务不受影响、照常跑完**；调度器立即
  按新配置决策，排队任务继续被调度。把上限**调小**时不会中断在执行任务，
  因此在它们自然收敛前，实际并发数可能瞬时高于新上限，但调度器不会再放行
  任何会突破新上限的新任务；把配额/上限**调大**时排队任务立即补位。
- 可用 `CurrentConfig()` 读取当前生效配置的**深拷贝**（与内部状态隔离，
  修改返回值不会影响运行时）。

## 分组暂停与恢复

`PauseGroup(group)` / `ResumeGroup(group)` 对单个分组做临时停启：

- **暂停只影响“启动决策”**：恢复之前该分组**排队中的任务不得开始执行**，
  但该分组**已经在执行的任务可以自然结束**；暂停不取消任何任务。
- **不拖累其他分组**：其他分组照常按各自配额调度，不会因为某分组被停下
  而变慢或被饿死。
- **恢复后重新参与公平调度**：排队任务立即回到老化优先级的公平选择中，
  并遵守**恢复当时生效**的配额（可以先 `UpdateConfig` 再 `ResumeGroup`）。
- **暂停期间其他操作语义不变**：提交照常入队（仍受全局/分组排队容量约束，
  超限照常返回可区分的拒绝）；排队任务可被 `Cancel()` 立即移除并拿到
  `Canceled` 终态；`Task.Timeout` 从**开始执行**计时，暂停排队期间不计时、
  不会提前超时，恢复执行后超时照常生效。
- 重复暂停返回 `ErrGroupPaused`，恢复未暂停的分组返回
  `ErrGroupNotPaused`；可用 `IsGroupPaused(group)` 查询状态。

### 暂停/调参与取消、关停的交互

- 全部控制操作（提交、取消、`UpdateConfig`、`PauseGroup`、`ResumeGroup`、
  `Shutdown`）在同一把调度锁下串行决策，与调度循环互斥：任何时刻的调度
  都不会突破当时生效的并发上限与分组配额，统计口径保持准确，每个任务
  恰好得到一个终态。
- **关停优先于暂停**：`Shutdown` 开始时会自动放开所有分组的暂停，使被暂停
  分组的排队任务也能在优雅收敛期内被调度执行；优雅期内未收敛的任务随
  [强制收敛](#优雅关停语义) 以 `Canceled` 终结。因此即使某分组长期暂停，
  关停也能正常收敛，关停返回后运行时彻底静止、无后台协程活动，所有已提交
  任务都能通过各自的 `Handle` 拿到结论。
- 关停流程开始后（Draining/Closed），`UpdateConfig`/`PauseGroup`/
  `ResumeGroup` 一律返回 `ErrControlRejected`；并发调用的 `Shutdown`
  仍然幂等。

## 优雅关停语义

`Shutdown(ctx)`：

1. 立即拒收新任务（`RejectShutdown`），可重复调用（幂等）。
2. 已排队与执行中的任务继续调度执行，在 `ShutdownTimeout`（默认 5s）内收敛。
3. 超时后**强制收敛**：排队任务全部以 `Canceled` 终结，执行中任务的 `ctx`
   被取消（`context.Cause` 为强制关停原因），再等待一个同等宽限期。
4. 全部后台协程（调度协程 + 工作协程）退出后返回 `nil`。
   若任务体不响应 `ctx` 取消，宽限期后返回 `ErrShutdownTimeout`——
   Go 无法强杀 goroutine，任务体必须响应 `ctx` 才能保证无泄漏。

## 本地验证

```bash
go build ./...
go vet ./...
go test -race -count=1 -v ./...
```

测试覆盖：优先级饥饿（`TestPriorityNoStarvation`）、配额越界
（`TestGroupQuotaNotExceeded`）、取消竞争（`TestCancelRace` 等）、
关停边界（`TestShutdownDrains` / `TestShutdownForced`，含协程泄漏检查）、
队列溢出（`TestQueueOverflow`）、混合负载开销（`TestMixedLoadOverhead`）。
运行期能力新增覆盖：

- 动态调参：`TestUpdateMaxConcurrencyScaleDownUp`（并发上限调小/调大的
  在执行不受影响与严格补位）、`TestUpdateGroupQuotaScaleDownUpRemove`
  （分组配额调小/调大/新增/移除）、`TestUpdateDefaultGroupQuota`
  （默认配额调大/调小/置 0）、`TestUpdateConfigRejected` 与
  `TestUpdateConfigAtomicCombined`（非法与冲突调整被拒、整体回滚）。
- 暂停/恢复：`TestPauseResumeBasic`（在执行自然结束、排队不启动、他组不受
  拖累、恢复遵守当时配额）、`TestPauseErrors`（重复暂停/误恢复报错）、
  `TestPauseSubmitCancelTimeout`（暂停期间提交、取消、超时语义不变）。
- 与关停交互：`TestShutdownDrainsPausedGroup`（被暂停分组排队任务优雅
  收敛、关停后控制操作被拒）、`TestShutdownForcedWithPausedGroup`
  （超时强收、句柄不卡死）、`TestConcurrentControlAndTraffic`（提交/取消/
  调参/暂停恢复与关停同时发生的并发压测：终态恰好一次、统计守恒）。

每个用例都在日志中打印输入参数与判定依据（`t.Logf`），可用 `-v` 复现结论。
