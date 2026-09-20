package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/delivery"
	"rc_zhangjun/internal/model"
)

var (
	errTest = errors.New("boom")
	// errShutdown 模拟优雅关闭时 HTTP 客户端返回的包装错误，
	// 它必须能被 errors.Is(err, context.Canceled) 识别出来。
	errShutdown = fmt.Errorf(`Post "http://x": %w`, context.Canceled)
)

func testBreakerConfig() config.Breaker {
	return config.Breaker{
		Enabled:          true,
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		HalfOpenProbes:   1,
	}
}

// fakeClock 让熔断器的冷却时间在测试里可控，避免用 time.Sleep 把测试拖慢、拖脆。
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(cfg config.Breaker) (*Breaker, *fakeClock, *int) {
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	trips := 0
	b := NewBreaker(cfg, func() { trips++ })
	b.now = clock.now
	return b, clock, &trips
}

func TestBreakerTripsAfterConsecutiveFailures(t *testing.T) {
	b, _, trips := newTestBreaker(testBreakerConfig())

	for i := 0; i < 2; i++ {
		b.Report(VerdictUnhealthy, b.Acquire(1))
		if got := b.Snapshot().State; got != BreakerClosed {
			t.Fatalf("after %d failures state = %s, want closed", i+1, got)
		}
	}
	b.Report(VerdictUnhealthy, b.Acquire(1))
	if got := b.Snapshot().State; got != BreakerOpen {
		t.Fatalf("after 3 failures state = %s, want open", got)
	}
	if b.Allowance(10) != 0 {
		t.Error("an open breaker must not allow any deliveries")
	}
	if *trips != 1 {
		t.Errorf("onTrip called %d times, want 1", *trips)
	}
}

// 中间夹一次成功就应该清零失败计数——熔断看的是"连续"失败，
// 否则一个长期低频抖动的下游会被慢慢累积到熔断。
func TestBreakerSuccessResetsFailureCount(t *testing.T) {
	b, _, _ := newTestBreaker(testBreakerConfig())

	b.Report(VerdictUnhealthy, b.Acquire(1))
	b.Report(VerdictUnhealthy, b.Acquire(1))
	b.Report(VerdictHealthy, b.Acquire(1))
	b.Report(VerdictUnhealthy, b.Acquire(1))
	b.Report(VerdictUnhealthy, b.Acquire(1))

	if got := b.Snapshot().State; got != BreakerClosed {
		t.Fatalf("state = %s, want closed (failures were not consecutive)", got)
	}
}

func TestBreakerHalfOpenLimitsProbesAndClosesOnSuccess(t *testing.T) {
	b, clock, _ := newTestBreaker(testBreakerConfig())
	for i := 0; i < 3; i++ {
		b.Report(VerdictUnhealthy, b.Acquire(1))
	}

	// 冷却未结束：仍然一个都不放。
	clock.advance(29 * time.Second)
	if got := b.Allowance(10); got != 0 {
		t.Fatalf("allowance before cooldown = %d, want 0", got)
	}

	// 冷却结束：进半开，只放 1 个探测。
	clock.advance(2 * time.Second)
	if got := b.Allowance(10); got != 1 {
		t.Fatalf("half-open allowance = %d, want 1 (half_open_probes)", got)
	}
	gen := b.Acquire(1)
	if got := b.Allowance(10); got != 0 {
		t.Fatalf("allowance with a probe in flight = %d, want 0", got)
	}

	b.Report(VerdictHealthy, gen)
	if got := b.Snapshot().State; got != BreakerClosed {
		t.Fatalf("state after successful probe = %s, want closed", got)
	}
	if got := b.Allowance(10); got != 10 {
		t.Fatalf("closed allowance = %d, want 10", got)
	}
}

func TestBreakerHalfOpenProbeFailureReopens(t *testing.T) {
	b, clock, trips := newTestBreaker(testBreakerConfig())
	for i := 0; i < 3; i++ {
		b.Report(VerdictUnhealthy, b.Acquire(1))
	}
	clock.advance(31 * time.Second)
	b.Allowance(10) // 触发 open -> half_open 转换
	gen := b.Acquire(1)
	b.Report(VerdictUnhealthy, gen)

	if got := b.Snapshot().State; got != BreakerOpen {
		t.Fatalf("state after failed probe = %s, want open", got)
	}
	if *trips != 2 {
		t.Errorf("onTrip called %d times, want 2", *trips)
	}
}

// VerdictIgnore 必须归还半开探测额度，否则半开状态会永久卡死在
// "有一个探测在飞"，熔断器再也不会恢复。
func TestBreakerIgnoredVerdictReleasesProbeSlot(t *testing.T) {
	b, clock, _ := newTestBreaker(testBreakerConfig())
	for i := 0; i < 3; i++ {
		b.Report(VerdictUnhealthy, b.Acquire(1))
	}
	clock.advance(31 * time.Second)
	b.Allowance(10)
	gen := b.Acquire(1)
	b.Report(VerdictIgnore, gen)

	if got := b.Snapshot().State; got != BreakerHalfOpen {
		t.Fatalf("state = %s, want half_open (ignored results must not change state)", got)
	}
	if got := b.Allowance(10); got != 1 {
		t.Fatalf("allowance = %d, want 1 (probe slot must be released)", got)
	}
}

// 一个迟到的失败结果不能把刚刚恢复的熔断器又打开。
func TestBreakerDiscardsStaleGeneration(t *testing.T) {
	b, _, _ := newTestBreaker(testBreakerConfig())

	staleGen := b.Acquire(1)
	// 状态翻转一轮：失败到熔断，让 generation 前进。
	for i := 0; i < 3; i++ {
		b.Report(VerdictUnhealthy, b.Acquire(1))
	}
	before := b.Snapshot()
	b.Report(VerdictHealthy, staleGen) // 迟到的上一代结果
	after := b.Snapshot()

	if after.State != before.State {
		t.Errorf("stale report changed state from %s to %s", before.State, after.State)
	}
}

func TestBreakerDisabledAlwaysAllows(t *testing.T) {
	cfg := testBreakerConfig()
	cfg.Enabled = false
	b, _, trips := newTestBreaker(cfg)

	for i := 0; i < 10; i++ {
		b.Report(VerdictUnhealthy, b.Acquire(1))
	}
	if got := b.Allowance(7); got != 7 {
		t.Errorf("allowance = %d, want 7 when the breaker is disabled", got)
	}
	if *trips != 0 {
		t.Errorf("onTrip called %d times, want 0", *trips)
	}
}

// breakerVerdict 是"业务失败"和"下游不可用"的分界线。
// 这张表是它的规格说明：4xx 说明下游活着，不该触发熔断。
func TestBreakerVerdictSeparatesBusinessFailureFromUnavailability(t *testing.T) {
	tests := []struct {
		name string
		res  delivery.Result
		want Verdict
	}{
		{
			name: "2xx 成功",
			res:  delivery.Result{Outcome: model.OutcomeSuccess, StatusCode: 200},
			want: VerdictHealthy,
		},
		{
			name: "400 永久失败但下游可达",
			res:  delivery.Result{Outcome: model.OutcomePermanent, StatusCode: 400, Err: errTest},
			want: VerdictHealthy,
		},
		{
			name: "401 同样说明下游活着",
			res:  delivery.Result{Outcome: model.OutcomePermanent, StatusCode: 401, Err: errTest},
			want: VerdictHealthy,
		},
		{
			name: "503 下游自称有问题",
			res:  delivery.Result{Outcome: model.OutcomeRetryable, StatusCode: 503, Err: errTest},
			want: VerdictUnhealthy,
		},
		{
			name: "429 被限流也算不可用",
			res:  delivery.Result{Outcome: model.OutcomeRetryable, StatusCode: 429, Err: errTest},
			want: VerdictUnhealthy,
		},
		{
			name: "传输层失败",
			res:  delivery.Result{Outcome: model.OutcomeRetryable, Err: errTest, Transport: true},
			want: VerdictUnhealthy,
		},
		{
			name: "渲染失败（请求没发出）",
			res:  delivery.Result{Outcome: model.OutcomePermanent, Err: delivery.ErrPermanentRender},
			want: VerdictIgnore,
		},
		{
			name: "本进程关闭导致的取消",
			res:  delivery.Result{Outcome: model.OutcomeRetryable, Err: errShutdown, Transport: true},
			want: VerdictIgnore,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := breakerVerdict(tc.res); got != tc.want {
				t.Errorf("breakerVerdict = %v, want %v", got, tc.want)
			}
		})
	}
}
