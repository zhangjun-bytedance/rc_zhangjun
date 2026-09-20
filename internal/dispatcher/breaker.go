package dispatcher

import (
	"sync"
	"time"

	"rc_zhangjun/internal/config"
)

// BreakerState 是熔断器状态。
type BreakerState string

const (
	// BreakerClosed 正常放行。
	BreakerClosed BreakerState = "closed"
	// BreakerOpen 熔断中，暂停领取该 endpoint 的任务。
	BreakerOpen BreakerState = "open"
	// BreakerHalfOpen 冷却结束，放少量探测请求试探下游是否恢复。
	BreakerHalfOpen BreakerState = "half_open"
)

// Verdict 是投递结果在「下游是否可用」这个维度上的判定。
//
// 注意它和 model.Outcome 不是一回事，这个区分很关键：
// 收到 400 在业务上是失败（Outcome=permanent），但在可用性上是成功
// ——对方活着、能解析请求、能回响应。用业务失败去触发熔断，
// 会让某个业务方的一批脏数据熔断掉整条通道，连带影响其他业务方。
// 熔断器只应该对「连不上 / 对方自称坏了（5xx）/ 被限流（429）」做出反应。
type Verdict int

const (
	// VerdictHealthy 下游可达且正常响应。
	VerdictHealthy Verdict = iota
	// VerdictUnhealthy 传输失败、5xx 或被限流。
	VerdictUnhealthy
	// VerdictIgnore 本次结果不反映下游状态（请求根本没发出，或本进程主动取消）。
	VerdictIgnore
)

// Breaker 是单个 endpoint 的熔断器。
//
// 这里的熔断有一个和常见实现不同的关键点：
// 熔断打开期间，任务根本不会被从队列里领取出来，因此不消耗重试预算。
//
// 常见的做法是照常取出任务、直接判失败、attempt+1。那样的话下游宕机 10 分钟
// 就能把所有在途通知的 8 次重试预算全部烧完，全部推进死信——
// 而这些通知本来只需要等下游恢复就能成功。熔断的意义正是"先别发，
// 等一等"，而不是"快速把失败次数用完"。
type Breaker struct {
	cfg config.Breaker

	mu                  sync.Mutex
	state               BreakerState
	consecutiveFailures int
	openUntil           time.Time
	probesInFlight      int
	// generation 用于识别过期的探测结果：半开期间如果状态已经翻转，
	// 迟到的探测结果不应该再影响新状态。
	generation uint64

	now    func() time.Time // 可注入，便于测试
	onTrip func()           // 熔断打开时的回调，用于打点
}

// NewBreaker 创建熔断器。onTrip 可为 nil。
func NewBreaker(cfg config.Breaker, onTrip func()) *Breaker {
	return &Breaker{cfg: cfg, state: BreakerClosed, now: time.Now, onTrip: onTrip}
}

// Allowance 返回当前允许发起的请求数上限（上限为 max）。
// 返回 0 表示熔断中，调度器应跳过该 endpoint。
func (b *Breaker) Allowance(max int) int {
	if !b.cfg.Enabled {
		return max
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == BreakerOpen && b.now().After(b.openUntil) {
		// 冷却结束，进入半开试探。
		b.state = BreakerHalfOpen
		b.probesInFlight = 0
		b.generation++
	}

	switch b.state {
	case BreakerOpen:
		return 0
	case BreakerHalfOpen:
		free := b.cfg.HalfOpenProbes - b.probesInFlight
		if free <= 0 {
			return 0
		}
		if free > max {
			free = max
		}
		return free
	default:
		return max
	}
}

// Acquire 登记 n 个即将发起的请求（仅半开状态下需要计数）。
// 返回当前 generation，供 Report 判断结果是否已过期。
func (b *Breaker) Acquire(n int) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerHalfOpen {
		b.probesInFlight += n
	}
	return b.generation
}

// Report 汇报一次投递结果对下游可用性的判定。
func (b *Breaker) Report(v Verdict, generation uint64) {
	if !b.cfg.Enabled {
		return
	}
	b.mu.Lock()
	tripped := false
	defer func() {
		b.mu.Unlock()
		// 回调在锁外执行，避免打点逻辑意外持有熔断器锁。
		if tripped && b.onTrip != nil {
			b.onTrip()
		}
	}()

	// 无论判定是什么，都必须先归还半开探测额度，否则半开状态会永久卡死。
	if b.state == BreakerHalfOpen && b.probesInFlight > 0 {
		b.probesInFlight--
	}
	if v == VerdictIgnore {
		return
	}
	// 状态已经翻过一轮，这个结果属于上一代，丢弃。
	// 否则一个迟到的失败会把刚刚恢复的熔断器又打开。
	if generation != b.generation {
		return
	}

	if v == VerdictHealthy {
		b.consecutiveFailures = 0
		if b.state != BreakerClosed {
			b.state = BreakerClosed
			b.generation++
		}
		return
	}

	b.consecutiveFailures++
	switch b.state {
	case BreakerHalfOpen:
		// 探测失败，立刻回到熔断并重新冷却。
		tripped = b.trip()
	case BreakerClosed:
		if b.consecutiveFailures >= b.cfg.FailureThreshold {
			tripped = b.trip()
		}
	}
}

// trip 打开熔断（调用方必须持有锁），返回是否发生了状态变化。
func (b *Breaker) trip() bool {
	b.state = BreakerOpen
	b.openUntil = b.now().Add(b.cfg.Cooldown)
	b.probesInFlight = 0
	b.generation++
	return true
}

// Snapshot 是熔断器的可观测快照。
type Snapshot struct {
	State               BreakerState `json:"state"`
	ConsecutiveFailures int          `json:"consecutive_failures"`
	OpenUntil           *time.Time   `json:"open_until,omitempty"`
}

// Snapshot 返回当前状态，供 /metrics 和 admin 接口展示。
func (b *Breaker) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := Snapshot{State: b.state, ConsecutiveFailures: b.consecutiveFailures}
	if b.state == BreakerOpen {
		t := b.openUntil
		s.OpenUntil = &t
	}
	return s
}
