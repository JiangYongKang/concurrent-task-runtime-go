package taskrt

// Handle 是已提交任务的句柄，用于等待结论或请求取消。
type Handle struct {
	done   chan struct{}
	result Result
	cancel func()
}

// Done 返回一个 channel，任务到达终态（含拒绝）时关闭。
func (h *Handle) Done() <-chan struct{} { return h.done }

// Wait 阻塞直到任务到达终态并返回结论。
func (h *Handle) Wait() Result {
	<-h.done
	return h.result
}

// Cancel 请求取消任务：排队中的任务将被移除，执行中的任务其 ctx 被取消。
// 被取消的任务不会产生结果值与回调。
func (h *Handle) Cancel() {
	if h.cancel != nil {
		h.cancel()
	}
}
