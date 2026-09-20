// Package e2e 是端到端集成测试：真实的 HTTP 入口 + 真实的 SQLite 队列 +
// 真实的调度器 + 真实的 HTTP 投递，只有外部供应商是可控的假实现。
//
// 这一层测试的价值在于，它验证的是需求里真正被承诺的那几件事：
// 下游挂掉时通知不丢、恢复后能自动送达、永久失败能快速进死信、
// 一个供应商的故障不会影响其他供应商、进程重启后队列还在。
// 这些行为跨越了所有分层，只能在这里被验证。
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"rc_zhangjun/internal/api"
	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/delivery"
	"rc_zhangjun/internal/dispatcher"
	"rc_zhangjun/internal/metrics"
	"rc_zhangjun/internal/model"
	"rc_zhangjun/internal/store"
)

// ---------- 可控的假供应商 ----------

// vendorRequest 记录一次收到的请求。
type vendorRequest struct {
	NotificationID string
	Attempt        string
	Body           string
	At             time.Time
}

// fakeVendor 是一个行为可编程的外部供应商。
type fakeVendor struct {
	srv *httptest.Server

	mu sync.Mutex
	// statusFn 决定第 n 次请求（从 1 开始）返回什么状态码和 Retry-After。
	statusFn    func(n int) (status int, retryAfter string)
	delay       time.Duration
	requests    []vendorRequest
	inFlight    int
	maxInFlight int
}

func newFakeVendor(t *testing.T) *fakeVendor {
	t.Helper()
	v := &fakeVendor{
		statusFn: func(int) (int, string) { return http.StatusOK, "" },
	}
	v.srv = httptest.NewServer(http.HandlerFunc(v.handle))
	t.Cleanup(v.srv.Close)
	return v
}

func (v *fakeVendor) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))

	v.mu.Lock()
	v.inFlight++
	if v.inFlight > v.maxInFlight {
		v.maxInFlight = v.inFlight
	}
	n := len(v.requests) + 1
	v.requests = append(v.requests, vendorRequest{
		NotificationID: r.Header.Get(delivery.HeaderNotificationID),
		Attempt:        r.Header.Get(delivery.HeaderAttempt),
		Body:           string(body),
		At:             time.Now(),
	})
	statusFn, delay := v.statusFn, v.delay
	v.mu.Unlock()

	defer func() {
		v.mu.Lock()
		v.inFlight--
		v.mu.Unlock()
	}()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}

	status, retryAfter := statusFn(n)
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.WriteHeader(status)
	io.WriteString(w, `{"ok":true}`)
}

// setStatusFn 切换供应商行为，用于模拟"故障中"和"已恢复"。
func (v *fakeVendor) setStatusFn(fn func(n int) (int, string)) {
	v.mu.Lock()
	v.statusFn = fn
	v.mu.Unlock()
}

func (v *fakeVendor) setDelay(d time.Duration) {
	v.mu.Lock()
	v.delay = d
	v.mu.Unlock()
}

func (v *fakeVendor) snapshot() []vendorRequest {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]vendorRequest, len(v.requests))
	copy(out, v.requests)
	return out
}

func (v *fakeVendor) requestCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.requests)
}

func (v *fakeVendor) peakConcurrency() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.maxInFlight
}

// alwaysStatus 返回一个固定状态码的行为函数。
func alwaysStatus(code int) func(int) (int, string) {
	return func(int) (int, string) { return code, "" }
}

// failFirstN 前 n 次失败（503），之后成功——模拟"下游故障后恢复"。
func failFirstN(n int) func(int) (int, string) {
	return func(i int) (int, string) {
		if i <= n {
			return http.StatusServiceUnavailable, ""
		}
		return http.StatusOK, ""
	}
}

// ---------- 测试装置 ----------

// harness 把整个服务在测试进程内拉起来。
type harness struct {
	t      *testing.T
	store  *store.SQLite
	api    http.Handler
	disp   *dispatcher.Dispatcher
	dbPath string

	cancel context.CancelFunc
	done   chan struct{}
}

// fastEndpoint 构造一个时间尺度被压缩的 endpoint 配置。
//
// 退避从 20ms 起步而不是生产配置的秒级：测试要验证的是重试逻辑的正确性，
// 不是等待的绝对时长。时间尺度压缩后整套用例能在几秒内跑完，
// 这直接决定了这些测试会不会被人在本地反复运行。
func fastEndpoint(name, url string, maxAttempts, concurrency int) config.Endpoint {
	return config.Endpoint{
		Name:        name,
		URL:         url,
		Method:      "POST",
		ContentType: "application/json",
		Timeout:     500 * time.Millisecond,
		MaxAttempts: maxAttempts,
		Concurrency: concurrency,
		Backoff: config.Backoff{
			Base:   20 * time.Millisecond,
			Max:    200 * time.Millisecond,
			Jitter: 0, // 测试里关掉抖动，让时序可预测
		},
		Breaker: config.Breaker{
			Enabled:          true,
			FailureThreshold: 100, // 默认基本不熔断，需要时按用例单独调小
			Cooldown:         200 * time.Millisecond,
			HalfOpenProbes:   1,
		},
		RetryStatusCodes: config.DefaultRetryStatusCodes,
	}
}

func newHarness(t *testing.T, endpoints map[string]config.Endpoint) *harness {
	t.Helper()
	return newHarnessAt(t, filepath.Join(t.TempDir(), "e2e.db"), endpoints)
}

// newHarnessAt 在指定数据库路径上启动服务，便于测试"重启后恢复"。
func newHarnessAt(t *testing.T, dbPath string, endpoints map[string]config.Endpoint) *harness {
	t.Helper()

	st, err := store.Open(store.Options{Path: dbPath, Synchronous: "OFF"})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	exec, err := delivery.NewExecutor(endpoints)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	// 测试里只保留 warn 以上的日志，既能看到死信/熔断这类关键事件，
	// 又不会被逐条投递日志淹没。
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
	mx := metrics.New()

	disp := dispatcher.New(dispatcher.Options{
		Config: config.Dispatcher{
			InstanceID:     "test-instance",
			PollInterval:   10 * time.Millisecond,
			BatchSize:      20,
			LeaseDuration:  2 * time.Second,
			ReaperInterval: 50 * time.Millisecond,
		},
		Endpoints: endpoints,
		Store:     st,
		Executor:  exec,
		Metrics:   mx,
		Logger:    logger,
		Owner:     "test-instance",
	})

	handler := api.New(api.Options{
		Config:     config.Server{MaxBodyBytes: 1 << 20},
		Endpoints:  endpoints,
		Store:      st,
		Dispatcher: disp,
		Metrics:    mx,
		Logger:     logger,
	}).Handler()

	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{
		t: t, store: st, api: handler, disp: disp,
		dbPath: dbPath, cancel: cancel, done: make(chan struct{}),
	}
	go func() {
		defer close(h.done)
		disp.Run(ctx, 2*time.Second)
	}()
	t.Cleanup(h.stop)
	return h
}

// stop 优雅关闭这一"实例"。可重复调用。
func (h *harness) stop() {
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		h.t.Error("dispatcher did not shut down within 10s")
	}
	h.store.Close()
}

// submit 通过真实 HTTP 入口提交一条通知，返回通知 ID。
func (h *harness) submit(endpoint string, payload string, idemKey string) string {
	h.t.Helper()
	body := map[string]any{"endpoint": endpoint, "payload": json.RawMessage(payload)}
	if idemKey != "" {
		body["idempotency_key"] = idemKey
	}
	raw, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.api.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted && rec.Code != http.StatusOK {
		h.t.Fatalf("submit failed: status=%d body=%s", rec.Code, rec.Body)
	}
	var resp struct{ ID string }
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		h.t.Fatalf("decode submit response: %v", err)
	}
	return resp.ID
}

// adminPost 调用管理接口。
func (h *harness) adminPost(path, body string) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.api.ServeHTTP(rec, req)
	return rec
}

// get 读取一条通知的当前状态。
func (h *harness) get(id string) *model.Notification {
	h.t.Helper()
	n, _, err := h.store.Get(context.Background(), id)
	if err != nil {
		h.t.Fatalf("get %s: %v", id, err)
	}
	return n
}

// attempts 读取一条通知的投递尝试历史。
func (h *harness) attempts(id string) []*model.Attempt {
	h.t.Helper()
	_, as, err := h.store.Get(context.Background(), id)
	if err != nil {
		h.t.Fatalf("get attempts %s: %v", id, err)
	}
	return as
}

// metricsText 抓取一次 /metrics。
func (h *harness) metricsText() string {
	h.t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.api.ServeHTTP(rec, req)
	return rec.Body.String()
}

// ---------- 断言辅助 ----------

// waitForStatus 等待通知到达期望状态。
//
// 轮询而不是固定 sleep：固定 sleep 要么让测试变慢，要么在 CI 的慢机器上变脆。
func (h *harness) waitForStatus(id string, want model.Status, timeout time.Duration) *model.Notification {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var last *model.Notification
	for time.Now().Before(deadline) {
		last = h.get(id)
		if last.Status == want {
			return last
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("notification %s did not reach %s within %s (current: status=%s attempt=%d/%d last_error=%q)",
		id, want, timeout, last.Status, last.Attempt, last.MaxAttempts, last.LastError)
	return nil
}

// eventually 等待条件成立。
func eventually(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition never became true within %s: %s", timeout, desc)
}

// consistently 验证条件在一段时间内持续成立（用于断言"没有发生某件事"）。
func consistently(t *testing.T, d time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("condition stopped holding: %s", desc)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected to find %q in:\n%s", needle, haystack)
	}
}

var _ = fmt.Sprintf // 保留 fmt 供子测试命名使用
