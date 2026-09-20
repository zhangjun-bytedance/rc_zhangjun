package e2e

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/model"
)

// 最基础的路径：提交 → 送达。业务系统拿到 202 之后就不再关心任何事。
func TestDeliversSuccessfully(t *testing.T) {
	vendor := newFakeVendor(t)
	h := newHarness(t, map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 5, 4),
	})

	id := h.submit("crm", `{"contact":"c-1","status":"active"}`, "")
	n := h.waitForStatus(id, model.StatusSucceeded, 5*time.Second)

	if n.Attempt != 1 {
		t.Errorf("attempt = %d, want 1 for a healthy vendor", n.Attempt)
	}
	reqs := vendor.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("vendor received %d requests, want 1", len(reqs))
	}
	// payload 必须原样透传，通知服务不理解也不改写业务内容。
	if reqs[0].Body != `{"contact":"c-1","status":"active"}` {
		t.Errorf("vendor body = %q, want the payload verbatim", reqs[0].Body)
	}
	// 下游要靠这个 ID 去重，它必须和我们返回给业务方的 ID 一致。
	if reqs[0].NotificationID != id {
		t.Errorf("vendor saw id %q, want %q", reqs[0].NotificationID, id)
	}
}

// 这是整个系统存在的理由：外部系统短暂故障时，业务系统完全不需要感知，
// 通知会在队列里自动退避重试，直到对方恢复。
func TestRetriesUntilVendorRecovers(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(failFirstN(3)) // 前 3 次 503，第 4 次成功

	h := newHarness(t, map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 8, 4),
	})

	id := h.submit("crm", `{"contact":"c-1"}`, "")
	n := h.waitForStatus(id, model.StatusSucceeded, 10*time.Second)

	if n.Attempt != 4 {
		t.Errorf("attempt = %d, want 4 (3 failures then success)", n.Attempt)
	}
	if got := vendor.requestCount(); got != 4 {
		t.Errorf("vendor received %d requests, want 4", got)
	}

	// 尝试历史必须完整可查——这是排障时唯一能还原现场的东西。
	attempts := h.attempts(id)
	if len(attempts) != 4 {
		t.Fatalf("recorded %d attempts, want 4", len(attempts))
	}
	for i, a := range attempts[:3] {
		if a.Outcome != model.OutcomeRetryable || a.StatusCode != 503 {
			t.Errorf("attempt %d = %s/%d, want retryable/503", i+1, a.Outcome, a.StatusCode)
		}
	}
	if last := attempts[3]; last.Outcome != model.OutcomeSuccess || last.StatusCode != 200 {
		t.Errorf("final attempt = %s/%d, want success/200", last.Outcome, last.StatusCode)
	}
}

// 退避必须真的在退避：连续重试之间的间隔要随次数增长，
// 否则"重试"会退化成对着一个已经挂掉的下游猛敲。
func TestRetryIntervalsGrowExponentially(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(failFirstN(3))

	ep := fastEndpoint("crm", vendor.srv.URL, 8, 4)
	ep.Backoff = config.Backoff{Base: 100 * time.Millisecond, Max: 2 * time.Second, Jitter: 0}
	h := newHarness(t, map[string]config.Endpoint{"crm": ep})

	id := h.submit("crm", `{}`, "")
	h.waitForStatus(id, model.StatusSucceeded, 10*time.Second)

	reqs := vendor.snapshot()
	if len(reqs) != 4 {
		t.Fatalf("vendor received %d requests, want 4", len(reqs))
	}
	gaps := make([]time.Duration, 0, 3)
	for i := 1; i < len(reqs); i++ {
		gaps = append(gaps, reqs[i].At.Sub(reqs[i-1].At))
	}

	// 期望约 100ms / 200ms / 400ms。只断言"至少达到下界"和"单调增长"，
	// 不断言精确值——调度轮询和机器负载会带来正向偏差，
	// 把上界卡死只会得到一个在 CI 上随机失败的测试。
	wantMin := []time.Duration{90 * time.Millisecond, 180 * time.Millisecond, 360 * time.Millisecond}
	for i, gap := range gaps {
		if gap < wantMin[i] {
			t.Errorf("gap %d = %s, want at least %s", i+1, gap, wantMin[i])
		}
	}
	if !(gaps[1] > gaps[0] && gaps[2] > gaps[1]) {
		t.Errorf("gaps are not growing: %v", gaps)
	}
}

// 4xx 必须立刻进死信，而不是把重试预算烧完。
// 一个 400 重试 8 次的系统，只是把"需要人介入"这件事推迟了几分钟。
func TestPermanentFailureGoesToDeadLetterImmediately(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(alwaysStatus(http.StatusBadRequest))

	h := newHarness(t, map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 8, 4),
	})

	id := h.submit("crm", `{"bad":"payload"}`, "")
	n := h.waitForStatus(id, model.StatusDead, 5*time.Second)

	if n.Attempt != 1 {
		t.Errorf("attempt = %d, want 1 (a 4xx must not consume the retry budget)", n.Attempt)
	}
	if n.LastStatusCode != 400 {
		t.Errorf("last_status_code = %d, want 400", n.LastStatusCode)
	}
	// 确认它真的不再被重试。
	consistently(t, 300*time.Millisecond, "a dead notification must not be retried", func() bool {
		return vendor.requestCount() == 1
	})
}

// 401 也按永久失败处理：凭据错了，重试到天亮还是同一个 401。
func TestUnauthorizedIsTreatedAsPermanent(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(alwaysStatus(http.StatusUnauthorized))

	h := newHarness(t, map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 8, 4),
	})

	id := h.submit("crm", `{}`, "")
	n := h.waitForStatus(id, model.StatusDead, 5*time.Second)
	if n.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", n.Attempt)
	}
}

// 下游长期不可用时的最终归宿：耗尽预算 → 死信 → 人工重投。
// 这条路径必须走通，否则"可靠投递"在下游长期故障时就只剩下无限重试。
func TestExhaustedBudgetThenManualRequeueSucceeds(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(alwaysStatus(http.StatusServiceUnavailable))

	h := newHarness(t, map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 3, 4),
	})

	id := h.submit("crm", `{"contact":"c-9"}`, "")
	n := h.waitForStatus(id, model.StatusDead, 10*time.Second)

	if n.Attempt != 3 {
		t.Errorf("attempt = %d, want exactly max_attempts (3)", n.Attempt)
	}
	if got := vendor.requestCount(); got != 3 {
		t.Errorf("vendor received %d requests, want 3", got)
	}

	// 下游修好了，运维手动重投。
	vendor.setStatusFn(alwaysStatus(http.StatusOK))
	rec := h.adminPost("/v1/notifications/"+id+"/retry", `{"extra_attempts":2}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("requeue failed: status=%d body=%s", rec.Code, rec.Body)
	}

	after := h.waitForStatus(id, model.StatusSucceeded, 10*time.Second)
	// 历史被保留：这条通知一共试了 4 次，前 3 次是自动重试，第 4 次是人工救回来的。
	if after.Attempt != 4 {
		t.Errorf("attempt = %d, want 4 (history preserved across the requeue)", after.Attempt)
	}
	if len(h.attempts(id)) != 4 {
		t.Errorf("attempt history = %d records, want 4", len(h.attempts(id)))
	}
}

// 429 时必须听下游的 Retry-After，而不是按自己的退避曲线继续敲。
func TestHonorsRetryAfterOnRateLimit(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(func(n int) (int, string) {
		if n == 1 {
			return http.StatusTooManyRequests, "1" // 要求等 1 秒
		}
		return http.StatusOK, ""
	})

	ep := fastEndpoint("crm", vendor.srv.URL, 5, 4)
	// 自己的退避只有 20ms，如果忽略 Retry-After 就会在 20ms 后就重试。
	ep.Backoff = config.Backoff{Base: 20 * time.Millisecond, Max: 5 * time.Second, Jitter: 0}
	h := newHarness(t, map[string]config.Endpoint{"crm": ep})

	id := h.submit("crm", `{}`, "")
	h.waitForStatus(id, model.StatusSucceeded, 10*time.Second)

	reqs := vendor.snapshot()
	if len(reqs) != 2 {
		t.Fatalf("vendor received %d requests, want 2", len(reqs))
	}
	if gap := reqs[1].At.Sub(reqs[0].At); gap < 900*time.Millisecond {
		t.Errorf("retry gap = %s, want >= ~1s as requested by Retry-After", gap)
	}
}

// 队列必须跨进程重启存活。这是"可靠送达"最硬的一条要求：
// 发布、重启、崩溃都不该让已经接收的通知消失。
func TestQueueSurvivesRestart(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(alwaysStatus(http.StatusServiceUnavailable))
	dbPath := filepath.Join(t.TempDir(), "restart.db")

	endpoints := map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 20, 4),
	}

	// 第一个"进程"：提交通知，确认它开始重试，然后关停。
	first := newHarnessAt(t, dbPath, endpoints)
	id := first.submit("crm", `{"contact":"survivor"}`, "order-restart")
	eventually(t, 5*time.Second, "the first instance should attempt delivery", func() bool {
		return vendor.requestCount() >= 1
	})
	first.stop()

	attemptsBeforeRestart := vendor.requestCount()

	// 下游恢复，第二个"进程"在同一个数据库上启动。
	vendor.setStatusFn(alwaysStatus(http.StatusOK))
	second := newHarnessAt(t, dbPath, endpoints)

	n := second.waitForStatus(id, model.StatusSucceeded, 10*time.Second)
	if n.Attempt <= 0 {
		t.Errorf("attempt = %d, want the pre-restart history preserved", n.Attempt)
	}
	if vendor.requestCount() <= attemptsBeforeRestart {
		t.Error("the restarted instance did not pick up the pending notification")
	}
	// 幂等键也要跨重启保留，否则重启后重复提交会产生第二条通知。
	if n.IdempotencyKey != "order-restart" {
		t.Errorf("idempotency_key = %q, want it preserved across restart", n.IdempotencyKey)
	}
}

// 故障隔离：一个卡死的供应商不能拖慢其他供应商的通知。
// 这是"按 endpoint 分别领取 + 每 endpoint 独立并发上限"这个设计的验收标准。
func TestSlowVendorDoesNotBlockOtherEndpoints(t *testing.T) {
	slowVendor := newFakeVendor(t)
	fastVendor := newFakeVendor(t)

	// 慢供应商：每个请求都耗到超时。
	slowVendor.setDelay(2 * time.Second)

	slowEP := fastEndpoint("slow", slowVendor.srv.URL, 20, 2)
	slowEP.Timeout = 1500 * time.Millisecond
	fastEP := fastEndpoint("fast", fastVendor.srv.URL, 5, 4)

	h := newHarness(t, map[string]config.Endpoint{"slow": slowEP, "fast": fastEP})

	// 先把慢 endpoint 的并发槽位全部占满并持续堆积。
	for i := 0; i < 10; i++ {
		h.submit("slow", `{}`, "")
	}
	eventually(t, 3*time.Second, "the slow endpoint should saturate its own slots", func() bool {
		return slowVendor.requestCount() >= 2
	})

	// 此时提交一条给健康的 endpoint，它必须立刻被投递出去。
	start := time.Now()
	fastID := h.submit("fast", `{"urgent":true}`, "")
	h.waitForStatus(fastID, model.StatusSucceeded, 3*time.Second)
	elapsed := time.Since(start)

	// 慢供应商单个请求就要 1.5s，如果存在队头阻塞，这里必然远超 1s。
	if elapsed > time.Second {
		t.Errorf("delivery to the healthy endpoint took %s; a saturated endpoint is blocking others", elapsed)
	}
}

// 并发上限是本系统唯一的过载保护手段，必须真的生效——
// 否则一次积压重放会用几百个并发把下游打死。
func TestPerEndpointConcurrencyLimitIsEnforced(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setDelay(100 * time.Millisecond) // 让请求重叠，才能观察到并发峰值

	const limit = 3
	h := newHarness(t, map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 5, limit),
	})

	const total = 20
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		ids = append(ids, h.submit("crm", fmt.Sprintf(`{"i":%d}`, i), ""))
	}
	for _, id := range ids {
		h.waitForStatus(id, model.StatusSucceeded, 20*time.Second)
	}

	if peak := vendor.peakConcurrency(); peak > limit {
		t.Errorf("peak concurrency = %d, want <= %d", peak, limit)
	}
	if peak := vendor.peakConcurrency(); peak < 2 {
		t.Errorf("peak concurrency = %d; the test did not actually exercise concurrency", peak)
	}
}

// 熔断的核心价值：下游挂掉时停止无意义的敲打，并且不消耗重试预算。
//
// 如果熔断期间照常取任务、照常判失败，下游宕机几分钟就能把所有在途通知
// 的重试预算烧完、全部推进死信——而这些通知本来只要等下游恢复就能成功。
func TestCircuitBreakerStopsHammeringAndPreservesRetryBudget(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(alwaysStatus(http.StatusServiceUnavailable))

	ep := fastEndpoint("crm", vendor.srv.URL, 20, 4)
	ep.Breaker = config.Breaker{
		Enabled:          true,
		FailureThreshold: 3,
		Cooldown:         1500 * time.Millisecond,
		HalfOpenProbes:   1,
	}
	h := newHarness(t, map[string]config.Endpoint{"crm": ep})

	id := h.submit("crm", `{}`, "")

	// 连续失败触发熔断。
	eventually(t, 5*time.Second, "the breaker should open after repeated failures", func() bool {
		return h.disp.BreakerSnapshots()["crm"].State == "open"
	})

	countAtTrip := vendor.requestCount()
	attemptAtTrip := h.get(id).Attempt

	// 熔断期间：不再向下游发请求，重试预算也不再被消耗。
	consistently(t, 700*time.Millisecond, "an open breaker must stop all traffic to the vendor", func() bool {
		return vendor.requestCount() == countAtTrip
	})
	if got := h.get(id).Attempt; got != attemptAtTrip {
		t.Errorf("attempt went from %d to %d while the breaker was open; "+
			"an open breaker must not consume retry budget", attemptAtTrip, got)
	}
	if h.get(id).Status == model.StatusDead {
		t.Error("the notification was killed while the breaker was open")
	}

	// 下游恢复：半开探测成功后熔断关闭，积压的通知被正常送达。
	vendor.setStatusFn(alwaysStatus(http.StatusOK))
	h.waitForStatus(id, model.StatusSucceeded, 10*time.Second)

	eventually(t, 3*time.Second, "the breaker should close after a successful probe", func() bool {
		return h.disp.BreakerSnapshots()["crm"].State == "closed"
	})
}

// 4xx 不能触发熔断：那是某个业务方的脏数据，不是下游故障。
// 用业务失败去熔断，会让一个业务方的错误连带影响所有其他业务方。
func TestBusinessFailuresDoNotTripTheBreaker(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(alwaysStatus(http.StatusBadRequest))

	ep := fastEndpoint("crm", vendor.srv.URL, 5, 4)
	ep.Breaker = config.Breaker{
		Enabled: true, FailureThreshold: 2,
		Cooldown: time.Second, HalfOpenProbes: 1,
	}
	h := newHarness(t, map[string]config.Endpoint{"crm": ep})

	// 提交 5 条注定被拒的通知，全部应该进死信。
	for i := 0; i < 5; i++ {
		id := h.submit("crm", fmt.Sprintf(`{"i":%d}`, i), "")
		h.waitForStatus(id, model.StatusDead, 5*time.Second)
	}

	if got := h.disp.BreakerSnapshots()["crm"].State; got != "closed" {
		t.Errorf("breaker state = %s, want closed (4xx means the vendor is alive)", got)
	}

	// 通道仍然畅通：另一个业务方的正常通知不受影响。
	vendor.setStatusFn(alwaysStatus(http.StatusOK))
	id := h.submit("crm", `{"good":true}`, "")
	h.waitForStatus(id, model.StatusSucceeded, 5*time.Second)
}

// body_template 场景的端到端验证：不同供应商要求不同的 body 格式。
func TestBodyTemplateTransformsPayloadPerVendor(t *testing.T) {
	vendor := newFakeVendor(t)
	ep := fastEndpoint("inventory", vendor.srv.URL, 5, 4)
	ep.BodyTemplate = `{"sku":"{{ .sku }}","delta":{{ .quantity }},"ref":"{{ .order_id }}"}`

	h := newHarness(t, map[string]config.Endpoint{"inventory": ep})

	id := h.submit("inventory", `{"sku":"SKU-1","quantity":-2,"order_id":"o-7"}`, "")
	h.waitForStatus(id, model.StatusSucceeded, 5*time.Second)

	reqs := vendor.snapshot()
	want := `{"sku":"SKU-1","delta":-2,"ref":"o-7"}`
	if reqs[0].Body != want {
		t.Errorf("vendor body = %q, want %q", reqs[0].Body, want)
	}
}

// 入口幂等的端到端验证：业务系统因为网络抖动重复提交，
// 下游只会收到一次通知。这是本系统对"重复"这件事能做出的最强保证。
func TestDuplicateSubmissionDeliversOnlyOnce(t *testing.T) {
	vendor := newFakeVendor(t)
	h := newHarness(t, map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 5, 4),
	})

	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, h.submit("crm", `{"contact":"c-1"}`, "order-dup-1"))
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("submissions returned different ids: %s vs %s", id, ids[0])
		}
	}
	h.waitForStatus(ids[0], model.StatusSucceeded, 5*time.Second)

	consistently(t, 300*time.Millisecond, "only one delivery should be made", func() bool {
		return vendor.requestCount() == 1
	})
}

// 指标必须反映真实发生的事，否则告警是瞎的。
func TestMetricsReflectDeliveryOutcomes(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setStatusFn(failFirstN(2))

	h := newHarness(t, map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 8, 4),
	})

	id := h.submit("crm", `{}`, "")
	h.waitForStatus(id, model.StatusSucceeded, 10*time.Second)

	text := h.metricsText()
	mustContain(t, text, `notify_ingress_total{endpoint="crm",result="accepted"} 1`)
	mustContain(t, text, `notify_delivery_attempts_total{endpoint="crm",outcome="retryable"} 2`)
	mustContain(t, text, `notify_delivery_attempts_total{endpoint="crm",outcome="success"} 1`)
	mustContain(t, text, `notify_terminal_total{endpoint="crm",state="succeeded"} 1`)
	mustContain(t, text, `notify_queue_depth{status="succeeded"} 1`)
	mustContain(t, text, `notify_delivery_duration_seconds_count{endpoint="crm"} 3`)
}

// 优雅关闭：收到关闭信号时，已经落库的通知不能丢，
// 在途投递要么跑完，要么被重新排期等下次启动继续。
func TestGracefulShutdownLosesNothing(t *testing.T) {
	vendor := newFakeVendor(t)
	vendor.setDelay(200 * time.Millisecond)
	dbPath := filepath.Join(t.TempDir(), "shutdown.db")
	endpoints := map[string]config.Endpoint{
		"crm": fastEndpoint("crm", vendor.srv.URL, 20, 4),
	}

	first := newHarnessAt(t, dbPath, endpoints)
	var ids []string
	for i := 0; i < 12; i++ {
		ids = append(ids, first.submit("crm", fmt.Sprintf(`{"i":%d}`, i), fmt.Sprintf("key-%d", i)))
	}
	eventually(t, 3*time.Second, "deliveries should start", func() bool {
		return vendor.requestCount() >= 1
	})
	first.stop() // 触发优雅关闭

	// 重启后，所有通知最终都必须到达成功态——一条都不能少。
	second := newHarnessAt(t, dbPath, endpoints)
	for _, id := range ids {
		second.waitForStatus(id, model.StatusSucceeded, 20*time.Second)
	}
}
