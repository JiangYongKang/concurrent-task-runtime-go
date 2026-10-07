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

## 运行期动态调参

无需重启或重建运行时，`UpdateConfig(ConfigUpdate)` 可原子地调整：

- `MaxConcurrency`：并发执行上限（必须 > 0）；
- `DefaultGroupQuota`：默认分组配额（必须 >= 0，0 表示不限）；
- `SetGroupQuotas`：新增或覆盖指定分组的配额（值必须 > 0）；
- `RemoveGroupQuotas`：去除指定分组的单独配额，回落到默认配额（目标必须存在）。

规则与语义：

- **原子生效**：所有校验（含"去除的配额必须存在""同组不能既设又删"等冲突
  检测）都在任何修改之前完成；任一项非法则整次拒绝，返回可
  `errors.Is(err, ErrInvalidConfigUpdate)` 判定的错误，配置保持原样。
- **只影响后续调度**：正在执行的任务不受影响、照常跑完；排队任务继续被调度，
  且任何时刻都不会突破**调整后**的并发上限与分组配额——调小上限时在跑任务
  自然退出前不会补位新任务。
- **关停后拒绝**：进入关停流程后调参返回 `ErrRuntimeClosed`。
- `Config()` 返回当前生效配置的快照（`GroupQuotas` 为拷贝，可安全读取）。

```go
n := 8
if err := rt.UpdateConfig(taskrt.ConfigUpdate{
    MaxConcurrency: &n,
    SetGroupQuotas: map[string]int{"batch": 4},
}); err != nil { /* errors.Is(err, taskrt.ErrInvalidConfigUpdate) */ }
```

## 分组暂停与恢复

- `PauseGroup(group)`：暂停指定分组。其**排队任务**在恢复前不得开始执行；
  **已在执行**的任务自然结束；其它分组照常调度，不会被拖慢或饿死。幂等。
- `ResumeGroup(group)`：恢复分组，排队任务重新参与公平调度（优先级 + 老化），
  并遵守恢复当时生效的配额。幂等。
- `IsPaused(group)`：查询暂停状态。
- 暂停期间**提交、取消、超时语义不变**：提交照常入队（仍受排队容量约束），
  `Handle.Cancel()` 照常把排队任务终结为 `Canceled`，每个任务仍恰好得到一个
  终态，不会因暂停而卡死。
- **与关停的交互**：暂停只是运行期的调度门禁，`Shutdown` 进入收敛阶段后
  该门禁自动放开——被暂停分组的排队任务与其它任务一起参与调度，仍严格受
  **并发上限与分组配额**约束：
  - 只要并发空位与分组配额够用，这些任务会在收敛阶段被尽快调度执行完，
    任务全部收敛后 `Shutdown` **立即返回**，不会等满 `ShutdownTimeout`；
  - 若到 `ShutdownTimeout` 仍未收敛（例如没有并发空位、配额限速跑不完），
    没来得及开始的排队任务被终结为 `Canceled`、`Result.Err` 为
    `ErrForcedShutdown`（可用 `errors.Is` 区分于调用方主动取消），
    执行中任务的 ctx 也以同一原因被取消；每个已提交任务都能等到终态结论，
    `Shutdown` 返回后运行时彻底静止、不残留后台协程。

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
- **取消原因可区分**：调用方主动取消与关停超时后的强制取消都表现为
  `Canceled`，但后者的 `Result.Err`（以及执行中任务的 `context.Cause`）
  为 `ErrForcedShutdown`，可用 `errors.Is` 区分。
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

## 优雅关停语义

`Shutdown(ctx)`：

1. 立即拒收新任务（`RejectShutdown`），可重复调用（幂等）。
2. 已排队与执行中的任务继续调度执行，在 `ShutdownTimeout`（默认 5s）内收敛。
   收敛阶段**分组暂停不再拦阻调度**：被暂停分组的排队任务照常参与调度，
   但并发上限与分组配额仍然生效，因此"能跑的尽快跑、跑不完的才取消"，
   任务全部收敛完关停立即返回，不会无谓地耗满关停时限。
3. 超时后**强制收敛**：排队任务全部以 `Canceled` 终结（`Result.Err` 为
   `ErrForcedShutdown`），执行中任务的 `ctx` 被取消（`context.Cause` 同为
   强制关停原因，可用 `errors.Is(res.Err, taskrt.ErrForcedShutdown)` 与
   调用方取消、执行超时区分），再等待一个同等宽限期。每个已提交任务在
   这一阶段都能拿到自己的终态结论，不会悬挂。
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
队列溢出（`TestQueueOverflow`）、混合负载开销（`TestMixedLoadOverhead`）、
运行期调参（`TestUpdateMaxConcurrencyIncrease/Decrease`、
`TestUpdateGroupQuotas`、`TestUpdateGroupQuotaDecrease`、
`TestUpdateConfigInvalid`、`TestUpdateConfigAfterShutdown`）、
分组暂停与恢复（`TestPauseGroupBlocksScheduling`、
`TestPauseRunningTasksFinish`、`TestPauseDoesNotStarveOthers`、
`TestPauseCancelAndSubmit`）、暂停分组随关停收敛
（`TestShutdownWithPausedGroup`：排队任务在收敛期跑完且关停快速返回）、
暂停 × 关停边界（`TestShutdownPausedGroupDrainRespectsQuotaAndFast`：
额度足够全部跑完且关停不等时限；`TestShutdownPausedGroupForcedAllTerminal`：
时限不够整体强制取消、原因可区分为 `ErrForcedShutdown` 且每个任务都有终态、
无协程泄漏；`TestShutdownPausedGroupPartialDrain`：部分跑完、剩余强制取消）
以及调参/暂停与关停并发压力
（`TestPauseResumeUpdateConcurrentShutdown`）。
每个用例都在日志中打印输入参数与判定依据（`t.Logf`），可用 `-v` 复现结论。
