// Package store 定义通知任务的持久化契约。
//
// 这一层是整个系统"不丢消息"承诺的落点：HTTP 入口在返回 202 之前必须
// 已经把任务写入存储，之后的所有故障（进程崩溃、机器重启、下游长期不可用）
// 都靠这张表恢复。
//
// 之所以先定义接口再给 SQLite 实现，不是为了"可插拔"这种虚的好处，
// 而是因为第一版明确会演进到 PostgreSQL（见 README「演进路径」）。
// 接口划在这里，是为了让未来替换存储时 dispatcher 和 api 层零改动。
package store

import (
	"context"
	"errors"
	"time"

	"rc_zhangjun/internal/model"
)

var (
	// ErrNotFound 目标通知不存在。
	ErrNotFound = errors.New("notification not found")
	// ErrConflict 状态机不允许当前操作（例如重投一个已经成功的通知）。
	ErrConflict = errors.New("notification state conflict")
)

// ListFilter 是 admin 查询接口的过滤条件。
type ListFilter struct {
	Status   model.Status
	Endpoint string
	Limit    int
	// CreatedBefore 用于游标翻页；零值表示不限。
	CreatedBefore time.Time
}

// FailureUpdate 描述一次失败尝试后要写回的状态。
type FailureUpdate struct {
	ID             string
	Attempt        int
	LastError      string
	LastStatusCode int
	// NextAttemptAt 为下一次可领取时间；仅当 Dead 为 false 时有意义。
	NextAttemptAt time.Time
	// Dead 为 true 表示直接进死信（不可重试错误或预算耗尽）。
	Dead bool
}

// Store 是通知任务的持久化队列。
//
// 所有方法都必须是并发安全的，并且状态写入必须做乐观校验
// （带上期望的原状态/lease 持有者），否则 lease 回收与正常完成会互相覆盖。
type Store interface {
	// Enqueue 幂等地写入一条通知。
	// created 为 false 表示 IdempotencyKey 命中已有记录，返回的是已存在的那条。
	Enqueue(ctx context.Context, n *model.Notification) (stored *model.Notification, created bool, err error)

	// ClaimForEndpoint 为单个 endpoint 领取最多 limit 条到期任务，并打上 lease。
	//
	// 按 endpoint 分别领取（而不是一次捞全表）是刻意的设计：
	// 它让某个供应商的堆积无法占满 worker 池，天然避免队头阻塞。
	ClaimForEndpoint(ctx context.Context, endpoint, owner string, limit int, leaseFor time.Duration) ([]*model.Notification, error)

	// MarkSucceeded 在 owner 仍持有 lease 的前提下把任务标记为成功。
	MarkSucceeded(ctx context.Context, id, owner string, attempt, statusCode int) error

	// MarkFailure 在 owner 仍持有 lease 的前提下写回失败结果（重新排期或进死信）。
	MarkFailure(ctx context.Context, owner string, u FailureUpdate) error

	// RecoverExpiredLeases 把 lease 已过期的 in_flight 任务放回 pending。
	// 这是进程崩溃后的恢复路径，也是 at-least-once 语义中重复投递的主要来源。
	RecoverExpiredLeases(ctx context.Context, now time.Time) (int, error)

	// Get 返回通知本体及其投递尝试历史（按尝试序号升序）。
	Get(ctx context.Context, id string) (*model.Notification, []*model.Attempt, error)

	// List 按条件列出通知，用于死信巡检。
	List(ctx context.Context, f ListFilter) ([]*model.Notification, error)

	// Requeue 把死信重新放回 pending，并追加 extraAttempts 的重试预算。
	Requeue(ctx context.Context, id string, extraAttempts int) (*model.Notification, error)

	// Cancel 取消一条尚未投递成功的通知。仅 pending 可取消——
	// in_flight 的请求可能已经打到下游，取消它会给出错误的语义承诺。
	Cancel(ctx context.Context, id string) (*model.Notification, error)

	// RecordAttempt 追加一条投递尝试审计记录。
	RecordAttempt(ctx context.Context, a *model.Attempt) error

	// CountByStatus 返回各状态的任务数，供 /metrics 的队列深度指标使用。
	CountByStatus(ctx context.Context) (map[model.Status]int64, error)

	// Ping 探测存储可用性，供 /readyz 使用。
	Ping(ctx context.Context) error

	Close() error
}
