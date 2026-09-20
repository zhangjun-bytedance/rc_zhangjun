package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/dispatcher"
	"rc_zhangjun/internal/model"
	"rc_zhangjun/internal/store"
)

// defaultRequeueBudget 是死信重投时追加的重试次数。
const defaultRequeueBudget = 3

// notificationDetail 是单条通知的完整视图（含尝试历史）。
type notificationDetail struct {
	*model.Notification
	Attempts []*model.Attempt `json:"attempts"`
}

// handleGet 返回一条通知的当前状态和全部投递尝试记录。
//
// 这是排障的主入口：业务方来问"我那条通知到底发出去了没"，
// 这个接口要一次说清楚——现在什么状态、试了几次、每次对方回了什么。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	n, attempts, err := s.store.Get(ctx, r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, notificationDetail{Notification: n, Attempts: attempts})
}

// listResponse 是列表接口的响应。
type listResponse struct {
	Items []*model.Notification `json:"items"`
	Count int                   `json:"count"`
	// NextBefore 是下一页的游标（传回 before 参数）。为空表示没有更多数据。
	NextBefore string `json:"next_before,omitempty"`
}

// handleList 按状态/endpoint 列出通知，主要用途是死信巡检。
//
// 用 created_at 游标翻页而不是 OFFSET：死信表会持续增长，
// OFFSET 翻到后面会越来越慢，而且期间有新数据插入时会漏记录。
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.ListFilter{
		Endpoint: q.Get("endpoint"),
		Limit:    100,
	}
	if v := q.Get("status"); v != "" {
		status := model.Status(v)
		if !status.Valid() {
			writeError(w, http.StatusBadRequest, "invalid_request",
				fmt.Sprintf("unknown status %q", v))
			return
		}
		f.Status = status
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 500 {
			writeError(w, http.StatusBadRequest, "invalid_request", "limit must be within 1..500")
			return
		}
		f.Limit = n
	}
	if v := q.Get("before"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request",
				"before must be an RFC3339 timestamp")
			return
		}
		f.CreatedBefore = t
	}

	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	items, err := s.store.List(ctx, f)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	resp := listResponse{Items: items, Count: len(items)}
	if len(items) == f.Limit {
		resp.NextBefore = items[len(items)-1].CreatedAt.Format(time.RFC3339Nano)
	}
	writeJSON(w, http.StatusOK, resp)
}

// retryRequest 是死信重投的请求体。
type retryRequest struct {
	// ExtraAttempts 是追加的重试预算，默认 3。
	ExtraAttempts int `json:"extra_attempts"`
}

// handleRetry 把死信重新放回队列。
//
// 只允许重投 dead 状态：重投一条还在正常重试中的通知没有意义（它本来就会重试），
// 重投一条已成功的通知则会造成一次业务上可见的重复投递。
// 这个限制由 store 层的状态机保证，这里只负责把冲突翻译成 409。
func (s *Server) handleRetry(w http.ResponseWriter, r *http.Request) {
	var req retryRequest
	if r.Body != nil {
		body, _ := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
				return
			}
		}
	}
	extra := req.ExtraAttempts
	if extra <= 0 {
		extra = defaultRequeueBudget
	}
	if extra > maxAttemptsCeiling {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("extra_attempts must be at most %d", maxAttemptsCeiling))
		return
	}

	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	n, err := s.store.Requeue(ctx, r.PathValue("id"), extra)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.log.Info("dead letter requeued", "notification_id", n.ID,
		"endpoint", n.Endpoint, "extra_attempts", extra)
	if s.dispatcher != nil {
		s.dispatcher.Wake()
	}
	writeJSON(w, http.StatusOK, n)
}

// handleCancel 取消一条尚未投递的通知。
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	n, err := s.store.Cancel(ctx, r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.log.Info("notification canceled", "notification_id", n.ID, "endpoint", n.Endpoint)
	writeJSON(w, http.StatusOK, n)
}

// endpointView 是 endpoint 的对外视图。
type endpointView struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Method      string `json:"method"`
	TimeoutMS   int64  `json:"timeout_ms"`
	MaxAttempts int    `json:"max_attempts"`
	Concurrency int    `json:"concurrency"`
	// HeaderNames 只返回 header 名，不返回值。
	// 静态 header 里装的就是供应商凭据，把它从任何接口里吐出来
	// 等于把密钥管理这件事白做了——哪怕这个接口已经有 admin token 保护。
	HeaderNames []string            `json:"header_names"`
	Breaker     dispatcher.Snapshot `json:"breaker"`
}

// handleEndpoints 列出已注册的 endpoint 及其熔断器状态。
//
// 排障时第一个要回答的问题往往是"是不是被熔断了"，
// 这个接口让人不用去翻 Prometheus 就能直接看到。
func (s *Server) handleEndpoints(w http.ResponseWriter, _ *http.Request) {
	var snapshots map[string]dispatcher.Snapshot
	if s.dispatcher != nil {
		snapshots = s.dispatcher.BreakerSnapshots()
	}
	views := make([]endpointView, 0, len(s.endpoints))
	for name, ep := range s.endpoints {
		views = append(views, endpointView{
			Name:        name,
			URL:         ep.URL,
			Method:      ep.Method,
			TimeoutMS:   ep.Timeout.Milliseconds(),
			MaxAttempts: ep.MaxAttempts,
			Concurrency: ep.Concurrency,
			HeaderNames: headerNames(ep),
			Breaker:     snapshots[name],
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	writeJSON(w, http.StatusOK, map[string]any{"endpoints": views, "count": len(views)})
}

func headerNames(ep config.Endpoint) []string {
	names := make([]string, 0, len(ep.Headers))
	for k := range ep.Headers {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
