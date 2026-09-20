package dispatcher

import (
	"testing"
	"time"

	"rc_zhangjun/internal/config"
)

// noJitter 让退避曲线完全可预测，便于断言精确值。
func noJitter() float64 { return 0.5 } // 0.5 => factor 1 + jitter*(2*0.5-1) = 1

func TestNextBackoffGrowsExponentiallyThenCaps(t *testing.T) {
	b := config.Backoff{Base: time.Second, Max: 10 * time.Second, Jitter: 0}

	want := []time.Duration{
		1 * time.Second,  // attempt 1
		2 * time.Second,  // attempt 2
		4 * time.Second,  // attempt 3
		8 * time.Second,  // attempt 4
		10 * time.Second, // attempt 5 被 Max 夹住
		10 * time.Second, // attempt 6 仍然是 Max
	}
	for i, expected := range want {
		attempt := i + 1
		if got := nextBackoff(b, attempt, nil); got != expected {
			t.Errorf("attempt %d: got %s, want %s", attempt, got, expected)
		}
	}
}

// 极大的 attempt 曾经会让 math.Pow 溢出成 +Inf，进而算出负的 duration
// （time.Duration 是 int64，+Inf 转换后是未定义值）。这个用例锁住那个边界。
func TestNextBackoffHandlesHugeAttemptWithoutOverflow(t *testing.T) {
	b := config.Backoff{Base: time.Second, Max: time.Hour, Jitter: 0}
	for _, attempt := range []int{0, 1, 64, 1000, 1 << 20} {
		got := nextBackoff(b, attempt, nil)
		if got < 0 {
			t.Errorf("attempt %d produced a negative delay: %s", attempt, got)
		}
		if got > b.Max {
			t.Errorf("attempt %d exceeded Max: got %s, want <= %s", attempt, got, b.Max)
		}
	}
}

func TestNextBackoffJitterStaysWithinConfiguredBand(t *testing.T) {
	b := config.Backoff{Base: 10 * time.Second, Max: time.Hour, Jitter: 0.3}

	// jitter 源返回 0 => factor = 1 - 0.3 = 0.7（下界）
	if got, want := nextBackoff(b, 1, func() float64 { return 0 }), 7*time.Second; got != want {
		t.Errorf("lower bound: got %s, want %s", got, want)
	}
	// jitter 源返回 1 => factor = 1 + 0.3 = 1.3（上界）
	if got, want := nextBackoff(b, 1, func() float64 { return 1 }), 13*time.Second; got != want {
		t.Errorf("upper bound: got %s, want %s", got, want)
	}
	// 中值不改变基准值
	if got, want := nextBackoff(b, 1, noJitter), 10*time.Second; got != want {
		t.Errorf("midpoint: got %s, want %s", got, want)
	}
}

func TestResolveDelayPrefersRetryAfter(t *testing.T) {
	b := config.Backoff{Base: time.Second, Max: time.Minute, Jitter: 0}

	// 下游明确说了等 5 秒，就该听它的，而不是用我们自己的 1 秒退避。
	if got, want := resolveDelay(b, 1, 5*time.Second, noJitter), 5*time.Second; got != want {
		t.Errorf("with Retry-After: got %s, want %s", got, want)
	}
	// 没有 Retry-After 时回退到退避曲线。
	if got, want := resolveDelay(b, 3, 0, noJitter), 4*time.Second; got != want {
		t.Errorf("without Retry-After: got %s, want %s", got, want)
	}
}

// 一个配错的 Retry-After（比如某供应商回了 86400）不能把通知冻住一整天。
func TestResolveDelayClampsAbsurdRetryAfterToMax(t *testing.T) {
	b := config.Backoff{Base: time.Second, Max: 2 * time.Minute, Jitter: 0}
	if got, want := resolveDelay(b, 1, 24*time.Hour, noJitter), 2*time.Minute; got != want {
		t.Errorf("got %s, want %s (clamped to backoff.max)", got, want)
	}
}
