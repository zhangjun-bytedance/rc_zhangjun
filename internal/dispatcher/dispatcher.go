// Package dispatcher 负责把队列里的通知调度出去：何时投、投多少、失败了怎么办。
//
// 调度模型是「单调度协程 + 每 endpoint 独立信号量 + 数据库 lease」：
//
//	┌─ 调度循环（1 个 goroutine）─────────────────────────────┐
//	│  for 每个 endpoint（轮转顺序，保证公平）:                │
//	│      额度 = min(该 endpoint 空闲并发, 熔断允许量, 批大小)│
//	│      从库里领取 ≤ 额度 条到期任务并打 lease              │
//	│      每条任务起一个 worker goroutine                    │
//	└─────────────────────────────────────────────────────────┘
//
// 为什么不是"一个全局 worker 池从全局队列取任务"：
// 那样一个卡死的供应商会把池子占满，拖垮所有其他供应商的通知（队头阻塞）。
// 按 endpoint 分别领取 + 每 endpoint 独立并发上限，把故障隔离在单个供应商内。
package dispatcher

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/delivery"
	"rc_zhangjun/internal/metrics"
	"rc_zhangjun/internal/model"
	"rc_zhangjun/internal/store"
)

// bookkeepingTimeout 是投递结束后写回结果的超时。
//
// 这个写回必须在进程关闭时也能完成——否则一次成功的投递会因为没能落库
// 而在重启后被重投一次。所以它用的是脱离了取消信号的 context。
const bookkeepingTimeout = 5 * time.Second

// Dispatcher 调度并执行通知投递。
type Dispatcher struct {
	cfg       config.Dispatcher
	endpoints map[string]config.Endpoint
	// names 是固定顺序的 endpoint 名列表，配合 rotation 实现轮转公平。
	names []string

	store   store.Store
	exec    *delivery.Executor
	metrics *metrics.Metrics
	log     *slog.Logger
	owner   string

	breakers map[string]*Breaker
	// sems 是每个 endpoint 的并发信号量。容量即该 endpoint 的并发上限。
	// 只有调度协程向里写，worker 结束时读出，因此调度协程的写入永不阻塞。
	sems map[string]chan struct{}

	rotation int
	jitter   jitterSource

	wake chan struct{}
	wg   sync.WaitGroup
}

// Options 是构造 Dispatcher 的依赖。
type Options struct {
	Config    config.Dispatcher
	Endpoints map[string]config.Endpoint
	Store     store.Store
	Executor  *delivery.Executor
	Metrics   *metrics.Metrics
	Logger    *slog.Logger
	Owner     string
	// Jitter 可选，用于测试中固定退避曲线。
	Jitter jitterSource
}

// New 创建调度器。
func New(o Options) *Dispatcher {
	d := &Dispatcher{
		cfg:       o.Config,
		endpoints: o.Endpoints,
		store:     o.Store,
		exec:      o.Executor,
		metrics:   o.Metrics,
		log:       o.Logger,
		owner:     o.Owner,
		breakers:  make(map[string]*Breaker, len(o.Endpoints)),
		sems:      make(map[string]chan struct{}, len(o.Endpoints)),
		wake:      make(chan struct{}, 1),
		jitter:    o.Jitter,
	}
	if d.jitter == nil {
		d.jitter = rand.Float64 // 顶层 math/rand 函数本身并发安全
	}
	if d.log == nil {
		d.log = slog.Default()
	}
	for name, ep := range o.Endpoints {
		d.names = append(d.names, name)
		epName := name
		d.breakers[name] = NewBreaker(ep.Breaker, func() {
			d.metrics.Inc(metrics.MetricBreakerTripsTotal, metrics.Label{Name: "endpoint", Value: epName})
			d.log.Warn("circuit breaker opened", "endpoint", epName,
				"cooldown", ep.Breaker.Cooldown.String())
		})
		d.sems[name] = make(chan struct{}, ep.Concurrency)
	}
	// 固定顺序，让轮转行为可预测（map 迭代顺序是随机的）。
	sort.Strings(d.names)
	d.registerGauges()
	return d
}

// Wake 提示调度器立刻检查一次队列。
//
// 有了它，首次投递延迟由入队动作驱动（毫秒级），而不是由轮询间隔决定。
// 轮询间隔退化为只影响"重试到期"的时间精度，于是可以配得比较宽松，
// 不必为了低延迟去做高频空转查询。非阻塞：已有待处理唤醒信号时直接返回。
func (d *Dispatcher) Wake() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// BreakerSnapshots 返回各 endpoint 的熔断器状态，供 admin 接口展示。
func (d *Dispatcher) BreakerSnapshots() map[string]Snapshot {
	out := make(map[string]Snapshot, len(d.breakers))
	for name, b := range d.breakers {
		out[name] = b.Snapshot()
	}
	return out
}

// Run 启动调度循环，直到 ctx 被取消。
//
// 返回前会走完优雅关闭流程：停止领取新任务 → 等在途投递自然完成 →
// 超过 grace 仍未完成的，取消其 HTTP 请求，让 worker 把失败结果正常写回并重新排期。
func (d *Dispatcher) Run(ctx context.Context, grace time.Duration) {
	// deliveryCtx 与 ctx 解耦：收到关闭信号后我们不立刻掐断在途请求，
	// 而是给它们 grace 时间跑完。硬掐只会把本来能成功的投递变成重复投递。
	deliveryCtx, cancelDelivery := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelDelivery()

	d.warnOrphanedEndpoints(ctx)

	var reaperWG sync.WaitGroup
	reaperWG.Add(1)
	go func() {
		defer reaperWG.Done()
		d.runReaper(ctx)
	}()

	d.log.Info("dispatcher started",
		"owner", d.owner, "endpoints", len(d.names),
		"poll_interval", d.cfg.PollInterval.String(),
		"lease_duration", d.cfg.LeaseDuration.String())

	timer := time.NewTimer(d.cfg.PollInterval)
	defer timer.Stop()

loop:
	for {
		dispatched := d.tick(ctx, deliveryCtx)
		if dispatched > 0 {
			// 上一轮领到了任务，说明可能还有堆积，立刻再来一轮，
			// 只让出一次调度机会避免饿死其他 goroutine。
			select {
			case <-ctx.Done():
				break loop
			default:
				continue
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(d.cfg.PollInterval)
		select {
		case <-ctx.Done():
			break loop
		case <-d.wake:
		case <-timer.C:
		}
	}

	d.log.Info("dispatcher draining", "grace", grace.String())
	reaperWG.Wait()

	if !waitWithTimeout(&d.wg, grace) {
		d.log.Warn("grace period expired, cancelling in-flight deliveries; " +
			"they will be retried according to their backoff schedule")
		cancelDelivery()
		// 再给一小段时间让 worker 把失败结果写回数据库。
		if !waitWithTimeout(&d.wg, bookkeepingTimeout+2*time.Second) {
			d.log.Error("some deliveries did not finish writing back; " +
				"their leases will expire and they will be recovered on restart")
			return
		}
	}
	d.log.Info("dispatcher stopped")
}

// tick 执行一轮调度，返回本轮领取的任务数。
func (d *Dispatcher) tick(ctx, deliveryCtx context.Context) int {
	if len(d.names) == 0 {
		return 0
	}
	total := 0
	// 轮转起点：避免列表靠前的 endpoint 在批量上限下长期抢占领取机会。
	d.rotation = (d.rotation + 1) % len(d.names)
	for i := 0; i < len(d.names); i++ {
		if ctx.Err() != nil {
			return total
		}
		name := d.names[(d.rotation+i)%len(d.names)]
		total += d.dispatchEndpoint(ctx, deliveryCtx, name)
	}
	return total
}

func (d *Dispatcher) dispatchEndpoint(ctx, deliveryCtx context.Context, name string) int {
	sem := d.sems[name]
	free := cap(sem) - len(sem)
	if free <= 0 {
		return 0 // 该 endpoint 并发已满，跳过（不影响其他 endpoint）
	}
	breaker := d.breakers[name]
	budget := breaker.Allowance(free)
	if budget <= 0 {
		return 0 // 熔断中：任务留在队列里，不消耗重试预算
	}
	if budget > d.cfg.BatchSize {
		budget = d.cfg.BatchSize
	}

	tasks, err := d.store.ClaimForEndpoint(ctx, name, d.owner, budget, d.cfg.LeaseDuration)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Error("claim failed", "endpoint", name, "error", err)
		}
		return 0
	}
	if len(tasks) == 0 {
		return 0
	}

	generation := breaker.Acquire(len(tasks))
	ep := d.endpoints[name]
	for _, task := range tasks {
		sem <- struct{}{} // 不会阻塞：领取量已受 free 限制，且只有本协程写入
		d.wg.Add(1)
		go func(n *model.Notification) {
			defer d.wg.Done()
			defer func() { <-sem }()
			d.deliverOne(deliveryCtx, n, ep, breaker, generation)
		}(task)
	}
	return len(tasks)
}

// deliverOne 执行单条通知的一次投递尝试并写回结果。
func (d *Dispatcher) deliverOne(ctx context.Context, n *model.Notification, ep config.Endpoint, breaker *Breaker, generation uint64) {
	attemptNo := n.Attempt + 1
	epLabel := metrics.Label{Name: "endpoint", Value: n.Endpoint}

	started := time.Now()
	res := d.exec.Deliver(ctx, n, attemptNo)

	d.metrics.Inc(metrics.MetricAttemptsTotal, epLabel,
		metrics.Label{Name: "outcome", Value: string(res.Outcome)})
	d.metrics.Observe(metrics.MetricDeliveryDuration, res.Duration.Seconds(), epLabel)
	breaker.Report(breakerVerdict(res), generation)

	// 写回一律使用脱离取消信号的 context：投递结果已经产生，
	// 不落库就等于丢失事实，会导致重启后重复投递一条已经成功的通知。
	bookCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
	defer cancel()

	attempt := &model.Attempt{
		NotificationID: n.ID,
		AttemptNo:      attemptNo,
		StartedAt:      started,
		DurationMS:     res.Duration.Milliseconds(),
		Outcome:        res.Outcome,
		StatusCode:     res.StatusCode,
		ResponseBody:   res.ResponseBody,
		TargetURL:      res.TargetURL,
	}
	if res.Err != nil {
		attempt.Error = res.Err.Error()
	}
	if err := d.store.RecordAttempt(bookCtx, attempt); err != nil {
		// 审计记录写失败不影响投递语义，只影响排障体验，因此不阻断主流程。
		d.log.Error("record attempt failed", "notification_id", n.ID, "error", err)
	}

	switch res.Outcome {
	case model.OutcomeSuccess:
		d.finishSuccess(bookCtx, n, attemptNo, res, epLabel)
	case model.OutcomePermanent:
		d.finishDead(bookCtx, n, attemptNo, res, "permanent", epLabel)
	default: // retryable
		d.scheduleRetry(bookCtx, n, ep, attemptNo, res, epLabel)
	}
}

func (d *Dispatcher) finishSuccess(ctx context.Context, n *model.Notification, attemptNo int, res delivery.Result, epLabel metrics.Label) {
	if err := d.store.MarkSucceeded(ctx, n.ID, d.owner, attemptNo, res.StatusCode); err != nil {
		d.logWriteback(n, attemptNo, err)
		return
	}
	d.metrics.Inc(metrics.MetricTerminalTotal, epLabel,
		metrics.Label{Name: "state", Value: string(model.StatusSucceeded)})
	d.log.Info("delivered", "notification_id", n.ID, "endpoint", n.Endpoint,
		"attempt", attemptNo, "status", res.StatusCode, "duration_ms", res.Duration.Milliseconds())
}

func (d *Dispatcher) finishDead(ctx context.Context, n *model.Notification, attemptNo int, res delivery.Result, reason string, epLabel metrics.Label) {
	msg := ""
	if res.Err != nil {
		msg = res.Err.Error()
	}
	err := d.store.MarkFailure(ctx, d.owner, store.FailureUpdate{
		ID: n.ID, Attempt: attemptNo, LastError: msg,
		LastStatusCode: res.StatusCode, Dead: true,
	})
	if err != nil {
		d.logWriteback(n, attemptNo, err)
		return
	}
	d.metrics.Inc(metrics.MetricTerminalTotal, epLabel,
		metrics.Label{Name: "state", Value: string(model.StatusDead)})
	// 死信是需要人介入的信号，所以用 Warn 而不是 Info：
	// 它应该能直接对应到一条告警规则。
	d.log.Warn("notification moved to dead letter", "notification_id", n.ID,
		"endpoint", n.Endpoint, "attempt", attemptNo, "reason", reason,
		"status", res.StatusCode, "error", msg, "response", res.ResponseBody)
}

func (d *Dispatcher) scheduleRetry(ctx context.Context, n *model.Notification, ep config.Endpoint, attemptNo int, res delivery.Result, epLabel metrics.Label) {
	if attemptNo >= n.MaxAttempts {
		d.finishDead(ctx, n, attemptNo, res, "retry budget exhausted", epLabel)
		return
	}
	delay := resolveDelay(ep.Backoff, attemptNo, res.RetryAfter, d.jitter)
	msg := ""
	if res.Err != nil {
		msg = res.Err.Error()
	}
	err := d.store.MarkFailure(ctx, d.owner, store.FailureUpdate{
		ID: n.ID, Attempt: attemptNo, LastError: msg,
		LastStatusCode: res.StatusCode,
		NextAttemptAt:  time.Now().UTC().Add(delay),
	})
	if err != nil {
		d.logWriteback(n, attemptNo, err)
		return
	}
	d.log.Info("delivery failed, retry scheduled", "notification_id", n.ID,
		"endpoint", n.Endpoint, "attempt", attemptNo, "max_attempts", n.MaxAttempts,
		"status", res.StatusCode, "retry_in", delay.String(), "error", msg)
}

// logWriteback 解释写回失败。
//
// ErrConflict 在这里是一个"预期内的异常"：lease 已被回收说明本实例被认为失联，
// 任务已经交给别人了，本次结果必须丢弃。把它和真正的数据库故障分开记录，
// 避免把可预期的竞态刷成 error 噪音、掩盖真问题。
func (d *Dispatcher) logWriteback(n *model.Notification, attemptNo int, err error) {
	if errors.Is(err, store.ErrConflict) {
		d.log.Warn("delivery result discarded: lease was reclaimed",
			"notification_id", n.ID, "attempt", attemptNo)
		return
	}
	d.log.Error("write back delivery result failed",
		"notification_id", n.ID, "attempt", attemptNo, "error", err)
}

// breakerVerdict 把投递结果翻译成「下游是否可用」的判定。
func breakerVerdict(res delivery.Result) Verdict {
	switch {
	case res.Err != nil && delivery.IsShutdownCancel(res.Err):
		// 我们自己掐掉的请求，不能算下游的错。
		return VerdictIgnore
	case res.StatusCode == 0 && !res.Transport:
		// 请求根本没发出（模板错误、endpoint 未配置），与下游状态无关。
		return VerdictIgnore
	case res.StatusCode == 0:
		return VerdictUnhealthy // 传输层失败：连不上、超时、连接重置
	case res.Outcome == model.OutcomeRetryable:
		return VerdictUnhealthy // 5xx / 429：下游自称有问题
	default:
		// 收到了明确的 HTTP 响应（含 4xx）：下游是活着的。
		return VerdictHealthy
	}
}

// runReaper 周期性回收过期 lease。
func (d *Dispatcher) runReaper(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		n, err := d.store.RecoverExpiredLeases(ctx, time.Now().UTC())
		if err != nil {
			if ctx.Err() == nil {
				d.log.Error("lease recovery failed", "error", err)
			}
			continue
		}
		if n > 0 {
			d.metrics.Add(metrics.MetricLeaseRecoveredTotal, float64(n))
			// 这条日志值得配告警：稳定出现说明有实例在异常退出，
			// 或者 lease_duration 相对 timeout 配得太短。
			d.log.Warn("recovered notifications with expired leases", "count", n)
			d.Wake()
		}
	}
}

// warnOrphanedEndpoints 检查是否存在指向已删除 endpoint 的待投递任务。
//
// 配置里删掉一个 endpoint 后，指向它的存量任务不会再被任何调度轮次领取，
// 会静默地永远停在 pending。这种"看起来正常、实际上永远不动"的状态
// 比直接报错危险得多，所以启动时主动扫一遍并告警。
func (d *Dispatcher) warnOrphanedEndpoints(ctx context.Context) {
	pending, err := d.store.List(ctx, store.ListFilter{Status: model.StatusPending, Limit: 500})
	if err != nil {
		d.log.Warn("orphan endpoint check skipped", "error", err)
		return
	}
	orphans := make(map[string]int)
	for _, n := range pending {
		if _, ok := d.endpoints[n.Endpoint]; !ok {
			orphans[n.Endpoint]++
		}
	}
	for name, count := range orphans {
		d.log.Error("pending notifications reference an unconfigured endpoint and will never be delivered",
			"endpoint", name, "count", count,
			"action", "re-add the endpoint to the config, or cancel these notifications via the admin API")
	}
}

// registerGauges 注册抓取时求值的 gauge。
//
// 这里只注册调度器自己掌握的运行时状态（并发占用、熔断状态）。
// 队列深度来自存储，由 api 层注册——那里是 /metrics 的所有者，
// 也是唯一必然持有 store 的地方。
func (d *Dispatcher) registerGauges() {
	d.metrics.SetGaugeFunc(metrics.MetricInFlight, func() []metrics.GaugeSample {
		samples := make([]metrics.GaugeSample, 0, len(d.sems))
		for name, sem := range d.sems {
			samples = append(samples, metrics.GaugeSample{
				Labels: []metrics.Label{{Name: "endpoint", Value: name}},
				Value:  float64(len(sem)),
			})
		}
		return samples
	})

	d.metrics.SetGaugeFunc(metrics.MetricBreakerState, func() []metrics.GaugeSample {
		states := []BreakerState{BreakerClosed, BreakerOpen, BreakerHalfOpen}
		samples := make([]metrics.GaugeSample, 0, len(d.breakers)*len(states))
		for name, b := range d.breakers {
			cur := b.Snapshot().State
			// 每个状态都输出一条（0 或 1），这样 Grafana 里状态切换是连续曲线，
			// 而不是时间序列突然消失再出现。
			for _, s := range states {
				v := 0.0
				if s == cur {
					v = 1
				}
				samples = append(samples, metrics.GaugeSample{
					Labels: []metrics.Label{
						{Name: "endpoint", Value: name},
						{Name: "state", Value: string(s)},
					},
					Value: v,
				})
			}
		}
		return samples
	})
}

// waitWithTimeout 等待 wg 完成，返回 true 表示在超时前完成。
func waitWithTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}
