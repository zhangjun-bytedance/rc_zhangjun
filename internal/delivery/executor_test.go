package delivery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/model"
)

func testEndpoint(url string) config.Endpoint {
	return config.Endpoint{
		Name:             "test",
		URL:              url,
		Method:           "POST",
		ContentType:      "application/json",
		Timeout:          2 * time.Second,
		MaxAttempts:      5,
		Concurrency:      4,
		RetryStatusCodes: config.DefaultRetryStatusCodes,
	}
}

func testNotification() *model.Notification {
	return &model.Notification{
		ID:             "01TESTNOTIFICATION00000000",
		Endpoint:       "test",
		IdempotencyKey: "order-42",
		Payload:        json.RawMessage(`{"sku":"ABC","quantity":3,"order_id":"o-42"}`),
		CreatedAt:      time.Unix(1700000000, 0).UTC(),
	}
}

// 这张表就是「什么算失败、失败了该不该重试」的规格说明。
// 它是本系统最需要被明确下来的判断，所以用穷举表格而不是零散用例来锁定。
func TestClassifyStatus(t *testing.T) {
	ep := testEndpoint("http://example.invalid")

	tests := []struct {
		code int
		want model.Outcome
		why  string
	}{
		{200, model.OutcomeSuccess, "标准成功"},
		{201, model.OutcomeSuccess, "2xx 均视为成功"},
		{204, model.OutcomeSuccess, "无内容也是成功"},
		{299, model.OutcomeSuccess, "2xx 区间上界"},

		{301, model.OutcomePermanent, "通知投递不跟随重定向到配置外的地址"},
		{302, model.OutcomePermanent, "同上"},

		{400, model.OutcomePermanent, "请求本身格式错，重试不会变好"},
		{401, model.OutcomePermanent, "凭据错，重试十次也不会突然有权限"},
		{403, model.OutcomePermanent, "无权限，需要人改配置"},
		{404, model.OutcomePermanent, "地址不存在，重试不会让它存在"},
		{409, model.OutcomePermanent, "业务冲突，通常意味着对方已处理过"},
		{422, model.OutcomePermanent, "语义校验失败，属于调用方问题"},

		{408, model.OutcomeRetryable, "对方自己说超时了"},
		{423, model.OutcomeRetryable, "资源被锁，稍后可能解锁"},
		{425, model.OutcomeRetryable, "太早，稍后再来"},
		{429, model.OutcomeRetryable, "被限流，退避后重试"},

		{500, model.OutcomeRetryable, "下游内部错误"},
		{502, model.OutcomeRetryable, "网关错误"},
		{503, model.OutcomeRetryable, "下游不可用"},
		{504, model.OutcomeRetryable, "网关超时"},
		{599, model.OutcomeRetryable, "所有 5xx 一律可重试"},
	}
	for _, tc := range tests {
		if got := classifyStatus(ep, tc.code); got != tc.want {
			t.Errorf("classifyStatus(%d) = %s, want %s (%s)", tc.code, got, tc.want, tc.why)
		}
	}
}

// 有些老旧供应商成功时返回 201/202，甚至把成功塞在非 2xx 里，
// 所以成功码必须可配，而不是硬编码 2xx。
func TestClassifyStatusHonorsCustomSuccessCodes(t *testing.T) {
	ep := testEndpoint("http://example.invalid")
	ep.SuccessStatusCodes = []int{201, 202}

	if got := classifyStatus(ep, 201); got != model.OutcomeSuccess {
		t.Errorf("201 = %s, want success", got)
	}
	// 显式声明了成功码之后，200 就不再算成功了——这是配置该有的精确含义。
	if got := classifyStatus(ep, 200); got != model.OutcomePermanent {
		t.Errorf("200 = %s, want permanent once success_status_codes is set", got)
	}
}

func TestClassifyStatusHonorsCustomRetryCodes(t *testing.T) {
	ep := testEndpoint("http://example.invalid")
	// 某些供应商用 409 表示"请稍后重试"。
	ep.RetryStatusCodes = []int{409}

	if got := classifyStatus(ep, 409); got != model.OutcomeRetryable {
		t.Errorf("409 = %s, want retryable when configured", got)
	}
	if got := classifyStatus(ep, 429); got != model.OutcomePermanent {
		t.Errorf("429 = %s, want permanent once retry_status_codes overrides the default", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		header string
		want   time.Duration
	}{
		{"", 0},
		{"5", 5 * time.Second},
		{"  30 ", 30 * time.Second},
		{"0", 0},
		{"-5", 0},
		{"not-a-number", 0},
		{"Sun, 01 Mar 2026 12:00:10 GMT", 10 * time.Second},
		{"Sun, 01 Mar 2026 11:59:50 GMT", 0}, // 已经过去的时间点
	}
	for _, tc := range tests {
		if got := parseRetryAfter(tc.header, now); got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", tc.header, got, tc.want)
		}
	}
}

func TestDeliverInjectsIdentityHeaders(t *testing.T) {
	var got http.Header
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ep := testEndpoint(srv.URL)
	ep.Headers = map[string]string{"X-Vendor-Token": "secret"}
	exec, err := NewExecutor(map[string]config.Endpoint{"test": ep})
	if err != nil {
		t.Fatal(err)
	}

	n := testNotification()
	n.Headers = map[string]string{"X-Request-Id": "req-1"}

	res := exec.Deliver(context.Background(), n, 3)
	if res.Outcome != model.OutcomeSuccess {
		t.Fatalf("outcome = %s (err=%v), want success", res.Outcome, res.Err)
	}

	// 下游要靠这几个头做去重，它们是 at-least-once 能落地的前提。
	if v := got.Get(HeaderNotificationID); v != n.ID {
		t.Errorf("%s = %q, want %q", HeaderNotificationID, v, n.ID)
	}
	if v := got.Get(HeaderAttempt); v != "3" {
		t.Errorf("%s = %q, want \"3\"", HeaderAttempt, v)
	}
	if v := got.Get(HeaderIdempotencyKey); v != "order-42" {
		t.Errorf("%s = %q, want \"order-42\"", HeaderIdempotencyKey, v)
	}
	if v := got.Get("X-Vendor-Token"); v != "secret" {
		t.Errorf("endpoint static header missing: %q", v)
	}
	if v := got.Get("X-Request-Id"); v != "req-1" {
		t.Errorf("caller header should pass through: %q", v)
	}
	// 默认透传：payload 原样送出，不做任何改写。
	if gotBody != string(n.Payload) {
		t.Errorf("body = %q, want the payload verbatim %q", gotBody, n.Payload)
	}
}

// 允许调用方覆盖 Authorization 或 Host，就等于把"凭据集中管理"这个
// 设计前提整个废掉，所以这些头必须在渲染阶段被丢弃。
func TestRenderHeadersDropsDangerousCallerHeaders(t *testing.T) {
	ep := testEndpoint("http://example.invalid")
	ep.Headers = map[string]string{"Authorization": "Bearer real-token"}

	n := testNotification()
	n.Headers = map[string]string{
		"Authorization":     "Bearer stolen",
		"Host":              "evil.example.com",
		"X-Notification-Id": "forged-id",
		"Transfer-Encoding": "chunked",
		"X-Legit":           "kept",
		"User-Agent":        "not-x-prefixed",
	}

	h := renderHeaders(ep, n, 1, 10)

	if got := h.Get("Authorization"); got != "Bearer real-token" {
		t.Errorf("Authorization = %q, want the endpoint's own credential", got)
	}
	if got := h.Get("Host"); got != "" {
		t.Errorf("Host = %q, want empty", got)
	}
	if got := h.Get(HeaderNotificationID); got != n.ID {
		t.Errorf("%s = %q, want the real id (callers must not forge it)", HeaderNotificationID, got)
	}
	if got := h.Get("Transfer-Encoding"); got != "" {
		t.Errorf("Transfer-Encoding = %q, want empty (hop-by-hop)", got)
	}
	if got := h.Get("X-Legit"); got != "kept" {
		t.Errorf("X-Legit = %q, want kept", got)
	}
	// 默认只放行 X- 前缀，非 X- 的头需要在 allow_caller_headers 里显式声明。
	if got := h.Get("User-Agent"); got != "" {
		t.Errorf("User-Agent = %q, want empty by default", got)
	}
}

func TestRenderHeadersAllowlistEnablesNonXHeaders(t *testing.T) {
	ep := testEndpoint("http://example.invalid")
	ep.AllowCallerHeaders = []string{"User-Agent"}

	n := testNotification()
	n.Headers = map[string]string{"User-Agent": "biz-service/1.0", "X-Other": "dropped"}

	h := renderHeaders(ep, n, 1, 10)
	if got := h.Get("User-Agent"); got != "biz-service/1.0" {
		t.Errorf("User-Agent = %q, want allowlisted value", got)
	}
	// 一旦显式配了白名单，就以白名单为准，X- 前缀不再自动放行。
	if got := h.Get("X-Other"); got != "" {
		t.Errorf("X-Other = %q, want empty once an allowlist is configured", got)
	}
}

func TestDeliverRendersBodyTemplate(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ep := testEndpoint(srv.URL)
	ep.BodyTemplate = `{"sku":"{{ .sku }}","delta":{{ .quantity }},"ref":"{{ .order_id }}"}`
	exec, err := NewExecutor(map[string]config.Endpoint{"test": ep})
	if err != nil {
		t.Fatal(err)
	}

	res := exec.Deliver(context.Background(), testNotification(), 1)
	if res.Outcome != model.OutcomeSuccess {
		t.Fatalf("outcome = %s (err=%v)", res.Outcome, res.Err)
	}
	want := `{"sku":"ABC","delta":3,"ref":"o-42"}`
	if gotBody != want {
		t.Errorf("body = %q, want %q", gotBody, want)
	}
}

// 模板语法错误必须在进程启动时就暴露，而不是等到凌晨三点第一条通知来。
func TestNewExecutorRejectsBrokenTemplateAtStartup(t *testing.T) {
	ep := testEndpoint("http://example.invalid")
	ep.BodyTemplate = `{{ .unclosed `
	if _, err := NewExecutor(map[string]config.Endpoint{"test": ep}); err == nil {
		t.Fatal("expected NewExecutor to fail on an invalid template")
	}
}

func TestDeliverTimeoutIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	ep := testEndpoint(srv.URL)
	ep.Timeout = 100 * time.Millisecond
	exec, err := NewExecutor(map[string]config.Endpoint{"test": ep})
	if err != nil {
		t.Fatal(err)
	}

	res := exec.Deliver(context.Background(), testNotification(), 1)
	if res.Outcome != model.OutcomeRetryable {
		t.Errorf("outcome = %s, want retryable", res.Outcome)
	}
	if !res.Transport {
		t.Error("a timeout should be flagged as a transport failure")
	}
	// 超时不能被当成"下游返回了状态码"，否则熔断判定会走错分支。
	if res.StatusCode != 0 {
		t.Errorf("status = %d, want 0 for a transport failure", res.StatusCode)
	}
}

func TestDeliverCapturesRetryAfterOnRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	exec, err := NewExecutor(map[string]config.Endpoint{"test": testEndpoint(srv.URL)})
	if err != nil {
		t.Fatal(err)
	}

	res := exec.Deliver(context.Background(), testNotification(), 1)
	if res.Outcome != model.OutcomeRetryable {
		t.Fatalf("outcome = %s, want retryable", res.Outcome)
	}
	if res.RetryAfter != 7*time.Second {
		t.Errorf("RetryAfter = %s, want 7s", res.RetryAfter)
	}
}

// 响应体只留开头一小段：排障需要看到对方的错误信息，
// 但一个持续返回巨大响应体的供应商不该把我们的磁盘写满。
func TestDeliverTruncatesResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, strings.Repeat("x", 10*maxResponseSnippet))
	}))
	defer srv.Close()

	exec, err := NewExecutor(map[string]config.Endpoint{"test": testEndpoint(srv.URL)})
	if err != nil {
		t.Fatal(err)
	}

	res := exec.Deliver(context.Background(), testNotification(), 1)
	if len(res.ResponseBody) > maxResponseSnippet {
		t.Errorf("response snippet is %d bytes, want <= %d", len(res.ResponseBody), maxResponseSnippet)
	}
}

func TestDeliverUnknownEndpointIsPermanent(t *testing.T) {
	exec, err := NewExecutor(map[string]config.Endpoint{})
	if err != nil {
		t.Fatal(err)
	}
	res := exec.Deliver(context.Background(), testNotification(), 1)
	if res.Outcome != model.OutcomePermanent {
		t.Errorf("outcome = %s, want permanent for an unconfigured endpoint", res.Outcome)
	}
}

// payload 不是合法 JSON 而 endpoint 配了模板时，属于调用方的错误，
// 必须判永久失败——重试一万次它也不会变成合法 JSON。
func TestDeliverInvalidPayloadWithTemplateIsPermanent(t *testing.T) {
	ep := testEndpoint("http://example.invalid")
	ep.BodyTemplate = `{{ .sku }}`
	exec, err := NewExecutor(map[string]config.Endpoint{"test": ep})
	if err != nil {
		t.Fatal(err)
	}

	n := testNotification()
	n.Payload = json.RawMessage(`{not json`)

	res := exec.Deliver(context.Background(), n, 1)
	if res.Outcome != model.OutcomePermanent {
		t.Errorf("outcome = %s, want permanent", res.Outcome)
	}
	if res.Transport {
		t.Error("a render failure must not be flagged as a transport failure")
	}
}
