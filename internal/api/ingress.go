package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"rc_zhangjun/internal/metrics"
	"rc_zhangjun/internal/model"
)

// 入口侧的硬性限制。
//
// 这些上限不是为了"更严谨"，而是因为共享服务必须假设调用方会犯错：
// 一个循环里拼出 5000 个 header 的业务代码是真实存在的，
// 没有上限时它会把整张队列表和所有下游请求一起搞坏。
const (
	maxHeaderCount     = 20
	maxHeaderKeyLen    = 128
	maxHeaderValueLen  = 1024
	maxIdempotencyKey  = 256
	maxAttemptsCeiling = 50
	// unknownEndpointLabel 用于指标标签。
	// 绝不能把调用方传来的任意字符串直接当标签值——那是 Prometheus 标签基数爆炸
	// 最经典的事故成因：一次拼写错误的循环调用就能生成上万条时间序列。
	unknownEndpointLabel = "_unknown"
)

// submitRequest 是业务系统提交通知的请求体。
type submitRequest struct {
	// Endpoint 是配置中注册的目标名。
	Endpoint string `json:"endpoint"`
	// IdempotencyKey 用于入口去重，也可通过 Idempotency-Key 头传入。
	IdempotencyKey string `json:"idempotency_key"`
	// Payload 是要投递给外部系统的内容，默认原样透传。
	Payload json.RawMessage `json:"payload"`
	// Headers 是附加请求头（受 endpoint 的放行策略限制）。
	Headers map[string]string `json:"headers"`
	// MaxAttempts 覆盖该条通知的重试预算上限，可选。
	MaxAttempts int `json:"max_attempts"`
}

// submitResponse 是提交结果。
type submitResponse struct {
	ID       string       `json:"id"`
	Status   model.Status `json:"status"`
	Endpoint string       `json:"endpoint"`
	// Duplicate 为 true 表示幂等键命中了已存在的通知，本次没有新建任务。
	Duplicate bool      `json:"duplicate"`
	CreatedAt time.Time `json:"created_at"`
}

// handleSubmit 接收通知提交请求。
//
// 语义：请求成功返回即表示「已持久化，本服务会负责送达」。
// 返回 202 而不是 200，是因为投递还没发生——用 200 会诱导调用方
// 以为外部系统已经收到了，而这是本系统明确不承诺的事。
func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		s.countIngress(unknownEndpointLabel, "rejected")
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type",
			"Content-Type must be application/json")
		return
	}

	// 限制请求体大小。超限时返回 413 而不是读完再判断，
	// 避免一个恶意/失控的调用方用超大 body 把内存吃光。
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		s.countIngress(unknownEndpointLabel, "rejected")
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
				fmt.Sprintf("request body exceeds %d bytes", s.cfg.MaxBodyBytes))
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_request", "failed to read request body")
		return
	}

	var req submitRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields() // 字段拼错必须报错：静默忽略会让调用方以为配置生效了
	if err := dec.Decode(&req); err != nil {
		s.countIngress(unknownEndpointLabel, "rejected")
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+err.Error())
		return
	}

	// Idempotency-Key 头是业界更通用的写法，body 字段优先（显式覆盖隐式）。
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}

	ep, ok := s.endpoints[req.Endpoint]
	if !ok {
		s.countIngress(unknownEndpointLabel, "rejected")
		writeError(w, http.StatusBadRequest, "unknown_endpoint",
			fmt.Sprintf("endpoint %q is not registered; register it in the service config first", req.Endpoint))
		return
	}

	if err := validateSubmit(&req); err != nil {
		s.countIngress(req.Endpoint, "rejected")
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	maxAttempts := ep.MaxAttempts
	if req.MaxAttempts > 0 {
		maxAttempts = req.MaxAttempts
	}

	now := s.now()
	n := &model.Notification{
		ID:             s.newID(),
		Endpoint:       req.Endpoint,
		IdempotencyKey: req.IdempotencyKey,
		Payload:        req.Payload,
		Headers:        req.Headers,
		Status:         model.StatusPending,
		MaxAttempts:    maxAttempts,
		NextAttemptAt:  now, // 立即可投递
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if len(n.Payload) == 0 {
		n.Payload = json.RawMessage(`{}`)
	}

	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	stored, created, err := s.store.Enqueue(ctx, n)
	if err != nil {
		s.countIngress(req.Endpoint, "error")
		s.log.Error("enqueue failed", "endpoint", req.Endpoint, "error", err)
		// 落库失败必须让调用方知道，绝不能返回 202。
		// 返回 202 就等于承诺了一条我们其实没有接住的通知——
		// 业务方会据此认为通知已经在路上，这比直接失败危险得多。
		writeError(w, http.StatusServiceUnavailable, "enqueue_failed",
			"could not persist the notification, please retry")
		return
	}

	if !created {
		s.countIngress(req.Endpoint, "duplicate")
		// 幂等命中返回 200 而不是 202：202 的含义是"我刚接收了一个新任务"，
		// 这里并没有新任务产生。状态码的差异让调用方能区分重试和首次提交。
		writeJSON(w, http.StatusOK, submitResponse{
			ID: stored.ID, Status: stored.Status, Endpoint: stored.Endpoint,
			Duplicate: true, CreatedAt: stored.CreatedAt,
		})
		return
	}

	s.countIngress(req.Endpoint, "accepted")
	// 唤醒调度器：不靠轮询，新任务能在毫秒级被领取。
	if s.dispatcher != nil {
		s.dispatcher.Wake()
	}
	writeJSON(w, http.StatusAccepted, submitResponse{
		ID: stored.ID, Status: stored.Status, Endpoint: stored.Endpoint,
		Duplicate: false, CreatedAt: stored.CreatedAt,
	})
}

func validateSubmit(req *submitRequest) error {
	if len(req.IdempotencyKey) > maxIdempotencyKey {
		return fmt.Errorf("idempotency_key must be at most %d characters", maxIdempotencyKey)
	}
	if req.MaxAttempts < 0 {
		return errors.New("max_attempts must not be negative")
	}
	if req.MaxAttempts > maxAttemptsCeiling {
		// 给出硬上限而不是默默截断：调用方以为自己配了 1000 次重试、
		// 实际只有 50 次，这种偏差应该在提交时就暴露出来。
		return fmt.Errorf("max_attempts must be at most %d", maxAttemptsCeiling)
	}
	if len(req.Headers) > maxHeaderCount {
		return fmt.Errorf("at most %d headers are allowed", maxHeaderCount)
	}
	for k, v := range req.Headers {
		if k == "" || len(k) > maxHeaderKeyLen {
			return fmt.Errorf("header names must be 1..%d characters", maxHeaderKeyLen)
		}
		if len(v) > maxHeaderValueLen {
			return fmt.Errorf("header %q exceeds %d characters", k, maxHeaderValueLen)
		}
		// 换行会导致 header 注入，必须在入口拦掉。
		if strings.ContainsAny(k, "\r\n") || strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("header %q must not contain CR or LF", k)
		}
	}
	if len(req.Payload) > 0 && !json.Valid(req.Payload) {
		return errors.New("payload must be valid JSON")
	}
	return nil
}

func (s *Server) countIngress(endpoint, result string) {
	s.metrics.Inc(metrics.MetricIngressTotal,
		metrics.Label{Name: "endpoint", Value: endpoint},
		metrics.Label{Name: "result", Value: result})
}

// contextWithTimeout 在请求上下文之上加一层超时。
func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}
