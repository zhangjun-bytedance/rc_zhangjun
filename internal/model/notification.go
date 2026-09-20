// Package model 定义通知投递的领域模型与状态机。
//
// 状态机（只有这 5 个状态，刻意保持最小）：
//
//	                 ┌──────────────── 重试（退避后）────────────────┐
//	                 ▼                                              │
//	[pending] ──claim──> [in_flight] ──2xx──────────────> [succeeded]│
//	     ▲                    │                                     │
//	     │                    ├──可重试失败 / lease 过期 ────────────┘
//	     │                    │
//	     │                    └──不可重试失败 / 重试预算耗尽──> [dead]
//	     │                                                      │
//	     └──────────────── 人工重投（admin API）─────────────────┘
//
//	[pending] ──人工取消──> [canceled]
//
// succeeded / canceled 是终态；dead 是"可人工干预的终态"。
package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Status 是通知任务的投递状态。
type Status string

const (
	// StatusPending 等待投递（含"等待下一次重试"）。next_attempt_at 决定何时可被领取。
	StatusPending Status = "pending"
	// StatusInFlight 已被某个实例领取，正在投递。由 lease 保护，lease 过期会被回收成 pending。
	StatusInFlight Status = "in_flight"
	// StatusSucceeded 目标系统已返回成功状态码。终态。
	StatusSucceeded Status = "succeeded"
	// StatusDead 重试预算耗尽或遇到不可重试错误，进入死信。可通过 admin API 重投。
	StatusDead Status = "dead"
	// StatusCanceled 人工取消，不再投递。终态。
	StatusCanceled Status = "canceled"
)

// Valid 判断状态值是否合法（用于校验外部输入，例如 admin 列表接口的过滤参数）。
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusInFlight, StatusSucceeded, StatusDead, StatusCanceled:
		return true
	}
	return false
}

// Terminal 表示该状态下调度器不会再自动投递。
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusDead || s == StatusCanceled
}

// ErrInvalidTransition 表示试图执行状态机不允许的状态迁移。
var ErrInvalidTransition = errors.New("invalid status transition")

// allowedTransitions 把状态机显式编码成数据，而不是散落在各处的 if 判断。
// store 层的每次状态写入都会先过这张表，避免并发下出现"succeeded 又被改回 pending"这类脏迁移。
var allowedTransitions = map[Status]map[Status]bool{
	StatusPending:   {StatusInFlight: true, StatusCanceled: true, StatusDead: true},
	StatusInFlight:  {StatusSucceeded: true, StatusPending: true, StatusDead: true},
	StatusSucceeded: {},
	StatusDead:      {StatusPending: true}, // 仅人工重投
	StatusCanceled:  {},
}

// CanTransition 报告 from -> to 是否为合法迁移。
func CanTransition(from, to Status) bool {
	return allowedTransitions[from][to]
}

// CheckTransition 在非法迁移时返回带上下文的错误。
func CheckTransition(from, to Status) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}

// Notification 是一条待投递（或已投递）的外部通知。
//
// 它同时承担三个角色：业务系统提交的请求记录、持久化队列中的一个任务、
// 以及排障时的现状快照。刻意不拆成多张表——MVP 阶段单表足够，
// 拆表带来的 join 成本和一致性成本大于收益。
type Notification struct {
	ID string `json:"id"`
	// Endpoint 是目标供应商在配置文件中注册的逻辑名（不是 URL）。
	// 调用方不能直接指定 URL，原因见 README「为什么用注册制」。
	Endpoint string `json:"endpoint"`
	// IdempotencyKey 由业务系统提供，用于入口去重。空字符串表示不参与去重。
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// Payload 是业务系统提交的原始 JSON。默认原样透传给下游。
	Payload json.RawMessage `json:"payload"`
	// Headers 是调用方希望附加的 header（如链路追踪 ID）。
	// 会被 endpoint 配置里的静态 header 覆盖，且受敏感 header 黑名单限制。
	Headers map[string]string `json:"headers,omitempty"`

	Status      Status `json:"status"`
	Attempt     int    `json:"attempt"`      // 已完成的投递尝试次数
	MaxAttempts int    `json:"max_attempts"` // 重试预算上限

	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LeaseOwner    string     `json:"lease_owner,omitempty"`
	LeaseExpires  *time.Time `json:"lease_expires_at,omitempty"`

	LastError      string     `json:"last_error,omitempty"`
	LastStatusCode int        `json:"last_status_code,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}

// ExhaustedBudget 报告重试预算是否已经用尽。
func (n *Notification) ExhaustedBudget() bool {
	return n.Attempt >= n.MaxAttempts
}

// Outcome 是单次投递尝试的判定结果。
//
// 把"失败"细分成可重试和永久失败是本系统最重要的一个判断：
// 盲目重试 4xx 只会浪费重试预算并延迟人工介入。
type Outcome string

const (
	// OutcomeSuccess 目标系统确认收到（默认 2xx）。
	OutcomeSuccess Outcome = "success"
	// OutcomeRetryable 目标系统暂时不可用（5xx / 429 / 超时 / 网络错误）。
	OutcomeRetryable Outcome = "retryable"
	// OutcomePermanent 请求本身有问题（多数 4xx），重试不会改变结果。
	OutcomePermanent Outcome = "permanent"
)

// Attempt 是一次投递尝试的审计记录。
//
// 存在的意义是排障：出问题时需要回答"第几次、什么时候、打到哪、对方回了什么"。
// 只保留响应体前若干字节，避免把死信风暴变成磁盘事故。
type Attempt struct {
	ID             int64     `json:"id"`
	NotificationID string    `json:"notification_id"`
	AttemptNo      int       `json:"attempt_no"`
	StartedAt      time.Time `json:"started_at"`
	DurationMS     int64     `json:"duration_ms"`
	Outcome        Outcome   `json:"outcome"`
	StatusCode     int       `json:"status_code,omitempty"`
	Error          string    `json:"error,omitempty"`
	ResponseBody   string    `json:"response_body,omitempty"`
	TargetURL      string    `json:"target_url,omitempty"`
}
