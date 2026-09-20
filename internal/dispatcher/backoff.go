package dispatcher

import (
	"math"
	"time"

	"rc_zhangjun/internal/config"
)

// jitterSource 返回 [0,1) 区间的随机数。
// 抽成函数参数而不是接收 *rand.Rand，是为了让退避曲线在测试里完全可预测
// （传入固定值即可断言精确的重试时刻），同时生产环境可以直接用
// 并发安全的 rand.Float64 而不必自己加锁。
type jitterSource func() float64

// nextBackoff 计算第 attempt 次失败后应该等待多久再重试（attempt 从 1 开始）。
//
// 公式：min(base * 2^(attempt-1), max)，再叠加 ±jitter 比例的随机抖动。
//
// 抖动不是可选项。没有抖动，一次下游全站故障会让成千上万条通知
// 在同一毫秒集体到期重试，把刚刚恢复的下游第二次打挂（惊群）。
// 抖动把这些重试摊平到一个时间窗口里，代价只是重试时刻不再精确可预测。
func nextBackoff(b config.Backoff, attempt int, jitter jitterSource) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := float64(b.Base)
	// 先夹住指数，避免 attempt 很大时 math.Pow 溢出成 +Inf。
	exp := math.Min(float64(attempt-1), 62)
	d := base * math.Pow(2, exp)
	if maxD := float64(b.Max); d > maxD || math.IsInf(d, 1) {
		d = maxD
	}
	if b.Jitter > 0 && jitter != nil {
		// 对称抖动：d * (1 ± jitter)。
		factor := 1 + b.Jitter*(2*jitter()-1)
		d *= factor
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// resolveDelay 决定实际的重试延迟。
//
// 下游通过 Retry-After 明确要求的等待时间优先级最高——它比我们的退避曲线
// 更了解自己什么时候能恢复。但仍然夹在 backoff.max 以内，
// 防止一个配错的 Retry-After（比如 86400）把通知冻住一整天。
func resolveDelay(b config.Backoff, attempt int, retryAfter time.Duration, jitter jitterSource) time.Duration {
	if retryAfter > 0 {
		if retryAfter > b.Max {
			return b.Max
		}
		return retryAfter
	}
	return nextBackoff(b, attempt, jitter)
}
