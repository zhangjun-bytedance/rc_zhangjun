// Package api 提供业务系统的提交入口和运维用的管理接口。
//
// 入口的唯一职责：把通知安全地落到持久化队列里，然后立刻返回 202。
// 它绝不在请求线程里尝试投递——那会把外部供应商的延迟和故障
// 直接传导给业务系统的主流程，而这正是本系统要消除的耦合。
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/dispatcher"
	"rc_zhangjun/internal/metrics"
	"rc_zhangjun/internal/store"
)

// Server 承载 HTTP 接口。
type Server struct {
	cfg        config.Server
	endpoints  map[string]config.Endpoint
	store      store.Store
	dispatcher *dispatcher.Dispatcher
	metrics    *metrics.Metrics
	log        *slog.Logger
	// now 可注入，便于测试。
	now func() time.Time
	// newID 可注入，便于测试。
	newID func() string
}

// Options 是构造 Server 的依赖。
type Options struct {
	Config     config.Server
	Endpoints  map[string]config.Endpoint
	Store      store.Store
	Dispatcher *dispatcher.Dispatcher
	Metrics    *metrics.Metrics
	Logger     *slog.Logger
	NewID      func() string
	Now        func() time.Time
}

// New 创建 Server。
func New(o Options) *Server {
	s := &Server{
		cfg:        o.Config,
		endpoints:  o.Endpoints,
		store:      o.Store,
		dispatcher: o.Dispatcher,
		metrics:    o.Metrics,
		log:        o.Logger,
		now:        o.Now,
		newID:      o.NewID,
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.now == nil {
		s.now = func() time.Time { return time.Now().UTC() }
	}
	if s.newID == nil {
		s.newID = newNotificationID
	}
	s.registerQueueDepthGauge()
	return s
}

// registerQueueDepthGauge 注册队列深度指标。
//
// 用抓取时回调查一次数据库，而不是在每个状态变更点维护内存计数器：
// 真值本来就在库里，内存计数器一旦和实际状态漂移，运维看到的就是假数据，
// 而这个指标恰恰是判断"是不是堆积了 / 死信是不是在涨"的依据。
func (s *Server) registerQueueDepthGauge() {
	s.metrics.SetGaugeFunc(metrics.MetricQueueDepth, func() []metrics.GaugeSample {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		counts, err := s.store.CountByStatus(ctx)
		if err != nil {
			s.log.Warn("queue depth gauge failed", "error", err)
			return nil
		}
		samples := make([]metrics.GaugeSample, 0, len(counts))
		for status, n := range counts {
			samples = append(samples, metrics.GaugeSample{
				Labels: []metrics.Label{{Name: "status", Value: string(status)}},
				Value:  float64(n),
			})
		}
		return samples
	})
}

// Handler 返回配置好路由的 http.Handler。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 业务入口：不鉴权 admin token，因为它面向内网所有业务系统。
	// 真实生产环境应该挂在服务网格 / 网关上做 mTLS 或 JWT，
	// 在服务内部重复实现一套身份体系是重复劳动（见 README 系统边界）。
	mux.HandleFunc("POST /v1/notifications", s.handleSubmit)

	// 管理接口：受 admin token 保护。
	mux.Handle("GET /v1/notifications", s.requireAdmin(http.HandlerFunc(s.handleList)))
	mux.Handle("GET /v1/notifications/{id}", s.requireAdmin(http.HandlerFunc(s.handleGet)))
	mux.Handle("POST /v1/notifications/{id}/retry", s.requireAdmin(http.HandlerFunc(s.handleRetry)))
	mux.Handle("POST /v1/notifications/{id}/cancel", s.requireAdmin(http.HandlerFunc(s.handleCancel)))
	mux.Handle("GET /v1/endpoints", s.requireAdmin(http.HandlerFunc(s.handleEndpoints)))

	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	return s.withRecovery(mux)
}

// withRecovery 兜住 handler 里的 panic。
//
// 一个 panic 不该让整个进程退出：进程里还有几十条在途投递和一个调度循环，
// 它们的状态比这一个请求重要得多。
func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic in http handler",
					"method", r.Method, "path", r.URL.Path, "panic", rec)
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// requireAdmin 用固定 token 保护管理接口。
func (s *Server) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.AdminToken == "" {
			// 未配置 token 时放行，但每次都记一条告警日志，
			// 避免"本地开发省事"的配置被无声地带到生产。
			s.log.Warn("admin endpoint accessed without a configured admin_token",
				"path", r.URL.Path)
			next.ServeHTTP(w, r)
			return
		}
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtleCompare(provided, s.cfg.AdminToken) {
			next.ServeHTTP(w, r)
			return
		}
		writeError(w, http.StatusUnauthorized, "unauthorized", "valid admin token required")
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz 探测依赖是否就绪。
//
// 和 healthz 分开是有意义的：healthz 只回答"进程还活着吗"（挂了就重启），
// readyz 回答"能接收流量吗"（数据库不可用时应该摘流，而不是重启进程）。
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := s.metrics.WritePrometheus(w); err != nil {
		s.log.Error("write metrics failed", "error", err)
	}
}

// ---------- 响应工具 ----------

// errorBody 是统一的错误响应结构。
// code 是给程序判断的稳定标识，message 是给人看的说明。
type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var body errorBody
	body.Error.Code = code
	body.Error.Message = message
	writeJSON(w, status, body)
}

// writeStoreError 把 store 层错误映射成合适的 HTTP 状态码。
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "notification not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

// subtleCompare 做长度无关的常量时间比较，避免通过响应时间侧信道猜 token。
func subtleCompare(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
