# concurrent-task-runtime-go

进程内并发任务运行时（包 `taskrt`），提供优先级调度、分组配额、取消/超时、
可观测统计与优雅关停，仅依赖 Go 标准库。

## 快速开始

```go
rt := taskrt.New(taskrt.Config{
    Workers:        8,                 // 并发执行上限，默认 NumCPU
    QueueCapacity:  1024,              // 全局排队容量（含在途未执行），默认 1024
    FairnessWeight: 8,                 // 防饥饿让位权重，默认 8
    ShutdownTimeout: 5 * time.Second,  // 优雅关停时限
    GroupQuotas: map[string]taskrt.GroupQuota{
        "ingest": {MaxConcurrent: 4, MaxQueued: 100},
    },
})

h, err := rt.Submit(taskrt.Task{
    Group:    "ingest",
    Priority: taskrt.PriorityHigh,
    Timeout:  2 * time.Second,
    Func:     func(ctx context.Context) error { /* 业务逻辑 */ return nil },
})
if err != nil {
    // err 为 *taskrt.RejectError，可按 Reason 区分拒绝原因
}
res := <-h.Done() // 每个被接纳任务恰好产生一个结论
_ = rt.Shutdown(context.Background())
```

## 调度与配额规则

- **优先级**：`PriorityHigh / Normal / Low` 三档，默认取最高非空档位的队首任务，
  同档位按提交先后 FIFO。
- **防饥饿**：每连续调度 `FairnessWeight` 个高优先级任务后，强制让位一次给最低
  非空优先级档位，低优先级不会被完全饿死；高档位被分组配额阻塞时会降级尝试
  其它档位，避免整体停摆。
- **分组配额**：`GroupQuota.MaxConcurrent` 限制分组在途执行数，`MaxQueued` 限制
  分组排队数；未配置的分组不受限。配额用尽的分组在调度时被跳过，不得越额执行。
- **容量口径**：全局 `QueueCapacity` 统计"已接纳未执行"的任务（排队中 + 已出队
  待执行），容量检查与入队在同一临界区完成，竞态下不会超容。
- **拒绝语义**：超出容量/配额或已关停时，`Submit` 返回 `*RejectError`，
  `Reason` 分别为 `RejectQueueFull / RejectGroupQuota / RejectShuttingDown`，
  可区分、不静默丢弃。

## 取消、超时与结论

- `Handle.Cancel()` 取消任务；`Task.Timeout` 设置单任务超时。
- 被取消/超时的任务不会执行任务体，不产生任何业务副作用；排队中的任务会被
  直接摘除并终结，执行中的任务通过 `ctx` 感知（非协作任务也会在超时点被终结，
  但其 goroutine 需自行响应 `ctx` 才能退出）。
- 每个被接纳任务恰好产生一个 `Result`（`Completed/Failed/Cancelled/Timeout`），
  由 `Handle.Done()` 交付；业务错误、panic（隔离为失败）、超时、取消分别计数，
  互不影响后续调度。

## 关停语义

- `Shutdown(ctx)` 后不再接收新任务（返回 `RejectShuttingDown`）。
- 优雅阶段：继续执行已排队与执行中的任务直至全部收敛，返回 `nil`。
- 超过 `ShutdownTimeout` 或 `ctx` 时限后进入强制阶段：排队任务统一以
  `ErrForcedShutdown` 终结，执行中任务收到放弃信号；`Shutdown` 返回时限错误。
- `Shutdown` 返回后，运行时持有的调度/工作/看护协程全部退出，无后台泄漏。

## 可观测性

`Runtime.Stats()` 返回一致性快照：`Submitted / Started / Completed / Failed /
Cancelled / TimedOut / Rejected`。口径保证：

- `Submitted == Completed + Failed + Cancelled + TimedOut + 在途`；
- `Started` 为真实开始执行的次数，`Completed + Failed <= Started <= Submitted`；
- 所有计数器并发安全，`-race` 下无重复计数、漏计或半更新状态。

## 本地验证

```bash
go build ./...
go vet ./...
go test ./taskrt/ -race -count=1 -v   # 单测日志含输入与判定依据
go test ./taskrt/ -run xxx -bench MixedWorkload -benchtime 2000x
```

测试覆盖：优先级饥饿（`TestPriorityNoStarvation`）、配额越界
（`TestGroupQuotaEnforced`）、取消竞争（`TestCancelRaceExactlyOnce`）、
关停边界（`TestGracefulShutdownDrains / TestForcedShutdownOnTimeout /
TestShutdownRejectsNewTasks / TestShutdownNoGoroutineLeak`）、队列溢出
（`TestQueueOverflowReject`）、超时（`TestTaskTimeout`）、失败隔离
（`TestBusinessFailureIsolation`）、统计一致性（`TestStatsConsistencyMixed`）
与混合负载吞吐（`TestMixedWorkloadThroughput`）。
