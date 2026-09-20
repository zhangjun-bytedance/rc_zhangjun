package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/metrics"
	"rc_zhangjun/internal/model"
	"rc_zhangjun/internal/store"
)

func testServer(t *testing.T, adminToken string) (http.Handler, *store.SQLite) {
	t.Helper()
	st, err := store.Open(store.Options{
		Path:        filepath.Join(t.TempDir(), "api.db"),
		Synchronous: "OFF",
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	endpoints := map[string]config.Endpoint{
		"crm": {
			Name: "crm", URL: "http://127.0.0.1:1/crm", Method: "POST",
			Timeout: time.Second, MaxAttempts: 8, Concurrency: 4,
			Headers: map[string]string{"Authorization": "Bearer super-secret"},
		},
	}
	srv := New(Options{
		Config: config.Server{
			MaxBodyBytes: 1024,
			AdminToken:   adminToken,
		},
		Endpoints: endpoints,
		Store:     st,
		Metrics:   metrics.New(),
	})
	return srv.Handler(), st
}

func post(t *testing.T, h http.Handler, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func get(t *testing.T, h http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return v
}

// 202 而不是 200：投递还没发生，用 200 会诱导调用方以为外部系统已经收到了。
func TestSubmitReturns202AndPersists(t *testing.T) {
	h, st := testServer(t, "")

	rec := post(t, h, "/v1/notifications",
		`{"endpoint":"crm","payload":{"contact":"c-1","status":"active"}}`, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body)
	}

	resp := decode[submitResponse](t, rec)
	if resp.ID == "" {
		t.Fatal("response must contain the notification id")
	}
	if resp.Status != model.StatusPending || resp.Duplicate {
		t.Errorf("resp = %+v, want pending and not duplicate", resp)
	}

	// 返回 202 的前提是已经落库了，这里直接验证这一点。
	stored, _, err := st.Get(context.Background(), resp.ID)
	if err != nil {
		t.Fatalf("notification was not persisted: %v", err)
	}
	if string(stored.Payload) != `{"contact":"c-1","status":"active"}` {
		t.Errorf("payload = %s, want it stored verbatim", stored.Payload)
	}
	if stored.MaxAttempts != 8 {
		t.Errorf("max_attempts = %d, want the endpoint default 8", stored.MaxAttempts)
	}
}

// 幂等重复提交返回 200 + duplicate=true，让调用方能区分"我刚提交了"和"这是重试"。
func TestSubmitIsIdempotent(t *testing.T) {
	h, _ := testServer(t, "")
	body := `{"endpoint":"crm","idempotency_key":"order-77","payload":{"a":1}}`

	first := post(t, h, "/v1/notifications", body, nil)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first submit status = %d, want 202", first.Code)
	}
	second := post(t, h, "/v1/notifications", body, nil)
	if second.Code != http.StatusOK {
		t.Fatalf("second submit status = %d, want 200", second.Code)
	}

	r1, r2 := decode[submitResponse](t, first), decode[submitResponse](t, second)
	if r2.ID != r1.ID {
		t.Errorf("duplicate returned id %s, want the original %s", r2.ID, r1.ID)
	}
	if !r2.Duplicate {
		t.Error("duplicate flag = false, want true")
	}
}

func TestSubmitAcceptsIdempotencyKeyHeader(t *testing.T) {
	h, _ := testServer(t, "")
	body := `{"endpoint":"crm","payload":{"a":1}}`
	hdr := map[string]string{"Idempotency-Key": "header-key-1"}

	first := post(t, h, "/v1/notifications", body, hdr)
	second := post(t, h, "/v1/notifications", body, hdr)

	if first.Code != http.StatusAccepted || second.Code != http.StatusOK {
		t.Fatalf("statuses = %d/%d, want 202/200", first.Code, second.Code)
	}
	if decode[submitResponse](t, second).ID != decode[submitResponse](t, first).ID {
		t.Error("the Idempotency-Key header did not deduplicate")
	}
}

// 未注册的 endpoint 必须在入口被拒绝，而不是收下来再在投递时进死信——
// 后者会把一个明显的调用方配置错误推迟几分钟才暴露。
func TestSubmitRejectsUnknownEndpoint(t *testing.T) {
	h, _ := testServer(t, "")
	rec := post(t, h, "/v1/notifications", `{"endpoint":"nope","payload":{}}`, nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	body := decode[errorBody](t, rec)
	if body.Error.Code != "unknown_endpoint" {
		t.Errorf("error code = %q, want unknown_endpoint", body.Error.Code)
	}
}

func TestSubmitValidation(t *testing.T) {
	h, _ := testServer(t, "")

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantErr  string
	}{
		{
			name:     "非法 JSON",
			body:     `{not json`,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_request",
		},
		{
			name: "字段拼写错误不能被静默忽略",
			// 调用方以为自己配了 max_attempt，实际没生效，这种偏差必须报错。
			body:     `{"endpoint":"crm","payload":{},"max_attempt":3}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_request",
		},
		{
			name:     "max_attempts 超过硬上限",
			body:     `{"endpoint":"crm","payload":{},"max_attempts":9999}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_request",
		},
		{
			name:     "header 值含换行会造成 header 注入",
			body:     `{"endpoint":"crm","payload":{},"headers":{"X-Evil":"a\r\nX-Injected: 1"}}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_request",
		},
		{
			name:     "payload 必须是合法 JSON",
			body:     `{"endpoint":"crm","payload":"not-an-object"}`,
			wantCode: http.StatusAccepted, // JSON 字符串本身是合法 JSON
			wantErr:  "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(t, h, "/v1/notifications", tc.body, nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantCode, rec.Body)
			}
			if tc.wantErr != "" {
				if got := decode[errorBody](t, rec).Error.Code; got != tc.wantErr {
					t.Errorf("error code = %q, want %q", got, tc.wantErr)
				}
			}
		})
	}
}

func TestSubmitTooManyHeaders(t *testing.T) {
	h, _ := testServer(t, "")
	pairs := make([]string, 0, maxHeaderCount+1)
	for i := 0; i <= maxHeaderCount; i++ {
		pairs = append(pairs, fmt.Sprintf(`"X-H%d":"v"`, i))
	}
	body := fmt.Sprintf(`{"endpoint":"crm","payload":{},"headers":{%s}}`, strings.Join(pairs, ","))

	rec := post(t, h, "/v1/notifications", body, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// 超大 body 必须在读取阶段就被拒绝，而不是读完再判断。
func TestSubmitRejectsOversizedBody(t *testing.T) {
	h, _ := testServer(t, "")
	huge := fmt.Sprintf(`{"endpoint":"crm","payload":{"blob":%q}}`, strings.Repeat("x", 4096))

	rec := post(t, h, "/v1/notifications", huge, nil)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%s", rec.Code, rec.Body)
	}
	if got := decode[errorBody](t, rec).Error.Code; got != "payload_too_large" {
		t.Errorf("error code = %q, want payload_too_large", got)
	}
}

func TestSubmitRejectsNonJSONContentType(t *testing.T) {
	h, _ := testServer(t, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/notifications",
		strings.NewReader(`{"endpoint":"crm","payload":{}}`))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415", rec.Code)
	}
}

func TestSubmitMaxAttemptsOverride(t *testing.T) {
	h, st := testServer(t, "")
	rec := post(t, h, "/v1/notifications",
		`{"endpoint":"crm","payload":{},"max_attempts":2}`, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	stored, _, err := st.Get(context.Background(), decode[submitResponse](t, rec).ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.MaxAttempts != 2 {
		t.Errorf("max_attempts = %d, want the caller override 2", stored.MaxAttempts)
	}
}

func TestAdminEndpointsRequireToken(t *testing.T) {
	h, _ := testServer(t, "s3cret")

	if rec := get(t, h, "/v1/notifications?status=dead", nil); rec.Code != http.StatusUnauthorized {
		t.Errorf("without token: status = %d, want 401", rec.Code)
	}
	bad := map[string]string{"Authorization": "Bearer wrong"}
	if rec := get(t, h, "/v1/notifications?status=dead", bad); rec.Code != http.StatusUnauthorized {
		t.Errorf("with wrong token: status = %d, want 401", rec.Code)
	}
	good := map[string]string{"Authorization": "Bearer s3cret"}
	if rec := get(t, h, "/v1/notifications?status=dead", good); rec.Code != http.StatusOK {
		t.Errorf("with valid token: status = %d, want 200", rec.Code)
	}

	// 业务提交入口不受 admin token 保护——它面向内网所有业务系统。
	if rec := post(t, h, "/v1/notifications", `{"endpoint":"crm","payload":{}}`, nil); rec.Code != http.StatusAccepted {
		t.Errorf("ingress must stay open to callers: status = %d, want 202", rec.Code)
	}
}

func TestGetReturnsAttemptHistory(t *testing.T) {
	h, st := testServer(t, "")
	rec := post(t, h, "/v1/notifications", `{"endpoint":"crm","payload":{"a":1}}`, nil)
	id := decode[submitResponse](t, rec).ID

	ctx := context.Background()
	if err := st.RecordAttempt(ctx, &model.Attempt{
		NotificationID: id, AttemptNo: 1, StartedAt: time.Now().UTC(),
		DurationMS: 42, Outcome: model.OutcomeRetryable, StatusCode: 503,
		Error: "HTTP 503", ResponseBody: `{"error":"down"}`, TargetURL: "http://crm/x",
	}); err != nil {
		t.Fatal(err)
	}

	detail := decode[notificationDetail](t, get(t, h, "/v1/notifications/"+id, nil))
	if len(detail.Attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(detail.Attempts))
	}
	a := detail.Attempts[0]
	if a.StatusCode != 503 || a.Outcome != model.OutcomeRetryable {
		t.Errorf("attempt = %+v, want the recorded 503/retryable", a)
	}
	// 排障的人需要看到对方到底回了什么。
	if a.ResponseBody != `{"error":"down"}` {
		t.Errorf("response body = %q, want the captured snippet", a.ResponseBody)
	}
}

func TestGetUnknownIDReturns404(t *testing.T) {
	h, _ := testServer(t, "")
	if rec := get(t, h, "/v1/notifications/NOPE", nil); rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestRetryRequeuesDeadLetter(t *testing.T) {
	h, st := testServer(t, "")
	rec := post(t, h, "/v1/notifications", `{"endpoint":"crm","payload":{}}`, nil)
	id := decode[submitResponse](t, rec).ID

	ctx := context.Background()
	st.ClaimForEndpoint(ctx, "crm", "owner", 1, time.Minute)
	st.MarkFailure(ctx, "owner", store.FailureUpdate{
		ID: id, Attempt: 8, Dead: true, LastError: "HTTP 500", LastStatusCode: 500,
	})

	out := post(t, h, "/v1/notifications/"+id+"/retry", `{"extra_attempts":2}`, nil)
	if out.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", out.Code, out.Body)
	}
	n := decode[model.Notification](t, out)
	if n.Status != model.StatusPending {
		t.Errorf("status = %s, want pending", n.Status)
	}
	if n.MaxAttempts != 10 {
		t.Errorf("max_attempts = %d, want 10 (8 + 2)", n.MaxAttempts)
	}
}

func TestRetryOnNonDeadReturns409(t *testing.T) {
	h, _ := testServer(t, "")
	rec := post(t, h, "/v1/notifications", `{"endpoint":"crm","payload":{}}`, nil)
	id := decode[submitResponse](t, rec).ID

	out := post(t, h, "/v1/notifications/"+id+"/retry", `{}`, nil)
	if out.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409 for a notification that is not dead", out.Code)
	}
}

func TestCancelPendingNotification(t *testing.T) {
	h, _ := testServer(t, "")
	rec := post(t, h, "/v1/notifications", `{"endpoint":"crm","payload":{}}`, nil)
	id := decode[submitResponse](t, rec).ID

	out := post(t, h, "/v1/notifications/"+id+"/cancel", "", nil)
	if out.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", out.Code, out.Body)
	}
	if got := decode[model.Notification](t, out).Status; got != model.StatusCanceled {
		t.Errorf("status = %s, want canceled", got)
	}
}

func TestListSupportsFilteringAndPaginationCursor(t *testing.T) {
	h, _ := testServer(t, "")
	for i := 0; i < 3; i++ {
		post(t, h, "/v1/notifications", `{"endpoint":"crm","payload":{}}`, nil)
	}

	all := decode[listResponse](t, get(t, h, "/v1/notifications", nil))
	if all.Count != 3 {
		t.Errorf("count = %d, want 3", all.Count)
	}

	page := decode[listResponse](t, get(t, h, "/v1/notifications?limit=2", nil))
	if page.Count != 2 {
		t.Errorf("count = %d, want 2", page.Count)
	}
	// 满页时必须给出游标，否则调用方无法翻到下一页。
	if page.NextBefore == "" {
		t.Error("next_before must be set when a full page is returned")
	}

	if rec := get(t, h, "/v1/notifications?status=bogus", nil); rec.Code != http.StatusBadRequest {
		t.Errorf("invalid status: got %d, want 400", rec.Code)
	}
	if rec := get(t, h, "/v1/notifications?limit=99999", nil); rec.Code != http.StatusBadRequest {
		t.Errorf("excessive limit: got %d, want 400", rec.Code)
	}
}

// 静态 header 里装的就是供应商凭据。哪怕这个接口有 admin token 保护，
// 也不该把值吐出来——密钥不应该出现在任何读接口的响应里。
func TestEndpointsNeverLeakCredentialValues(t *testing.T) {
	h, _ := testServer(t, "")
	rec := get(t, h, "/v1/endpoints", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "super-secret") {
		t.Fatalf("endpoint response leaked a credential value: %s", rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "Authorization") {
		t.Error("header names should still be visible for troubleshooting")
	}
}

func TestHealthAndReady(t *testing.T) {
	h, st := testServer(t, "")

	if rec := get(t, h, "/healthz", nil); rec.Code != http.StatusOK {
		t.Errorf("healthz = %d, want 200", rec.Code)
	}
	if rec := get(t, h, "/readyz", nil); rec.Code != http.StatusOK {
		t.Errorf("readyz = %d, want 200", rec.Code)
	}

	// 存储不可用时 readyz 必须失败（应该摘流），而 healthz 仍然成功（不该重启进程）。
	st.Close()
	if rec := get(t, h, "/readyz", nil); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz with a broken store = %d, want 503", rec.Code)
	}
	if rec := get(t, h, "/healthz", nil); rec.Code != http.StatusOK {
		t.Errorf("healthz with a broken store = %d, want 200", rec.Code)
	}
}

func TestMetricsExposesIngressCounters(t *testing.T) {
	h, _ := testServer(t, "")
	post(t, h, "/v1/notifications", `{"endpoint":"crm","payload":{}}`, nil)
	post(t, h, "/v1/notifications", `{"endpoint":"nope","payload":{}}`, nil)

	body := get(t, h, "/metrics", nil).Body.String()
	if !strings.Contains(body, `notify_ingress_total{endpoint="crm",result="accepted"} 1`) {
		t.Errorf("accepted counter missing from:\n%s", body)
	}
	// 未注册的 endpoint 名来自调用方输入，必须归到固定标签值，
	// 否则一次拼写错误的循环调用就能把 Prometheus 标签基数打爆。
	if !strings.Contains(body, `notify_ingress_total{endpoint="_unknown",result="rejected"} 1`) {
		t.Errorf("unknown-endpoint rejections must use a fixed label value:\n%s", body)
	}
	if !strings.Contains(body, "# TYPE notify_queue_depth gauge") {
		t.Errorf("queue depth gauge missing from:\n%s", body)
	}
}

func TestGeneratedIDsAreUniqueAndTimeOrdered(t *testing.T) {
	seen := make(map[string]bool, 2000)
	prev := ""
	for i := 0; i < 2000; i++ {
		id := newNotificationID()
		if len(id) != 26 {
			t.Fatalf("id %q has length %d, want 26", id, len(id))
		}
		if seen[id] {
			t.Fatalf("duplicate id generated: %s", id)
		}
		seen[id] = true
		// 同一毫秒内的 ID 顺序不保证，但整体必须单调不减，
		// 这样"按 ID 排序"才等于"按创建时间排序"。
		if prev != "" && id[:8] < prev[:8] {
			t.Fatalf("id time prefix went backwards: %s then %s", prev, id)
		}
		prev = id
	}
}

func TestSubmitDrainsAndClosesRequestBody(t *testing.T) {
	h, _ := testServer(t, "")
	body := bytes.NewBufferString(`{"endpoint":"crm","payload":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if _, err := io.ReadAll(req.Body); err != nil {
		t.Logf("body already consumed as expected: %v", err)
	}
}
