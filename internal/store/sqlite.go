package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"rc_zhangjun/internal/model"

	_ "modernc.org/sqlite" // 纯 Go SQLite 驱动，无需 CGO
)

//go:embed schema.sql
var schemaSQL string

// SQLite 是 Store 的 SQLite 实现。
//
// 关键取舍：连接池限制为 1 条连接。
// SQLite 同一时刻只允许一个写者，用 database/sql 的多连接去抢写锁只会换来
// SQLITE_BUSY 和难以复现的抖动。把并发收敛到单连接，让所有写串行化，
// 换来的是零"database is locked"错误和完全可预测的行为。
// 代价是写吞吐上限（单机约数千 TPS），这正是演进到 PostgreSQL 的触发条件。
type SQLite struct {
	db *sql.DB
}

// Options 是 SQLite 存储的打开参数。
type Options struct {
	// Path 是数据库文件路径。":memory:" 表示内存库（仅测试用）。
	Path string
	// Synchronous 对应 SQLite 的 synchronous pragma，默认 FULL。
	// FULL 保证断电也不丢已提交事务——既然入口已经对业务方承诺了 202，
	// 就不该在这里省这点 fsync。需要更高吞吐时可降为 NORMAL。
	Synchronous string
	// BusyTimeout 是遇到锁时的等待上限，默认 5s。
	BusyTimeout time.Duration
}

// Open 打开（必要时创建）SQLite 数据库并初始化表结构。
func Open(opts Options) (*SQLite, error) {
	if opts.Path == "" {
		return nil, errors.New("store: path is required")
	}
	if opts.Synchronous == "" {
		opts.Synchronous = "FULL"
	}
	if opts.BusyTimeout <= 0 {
		opts.BusyTimeout = 5 * time.Second
	}

	memory := opts.Path == ":memory:" || strings.HasPrefix(opts.Path, "file::memory:")
	if !memory {
		if dir := filepath.Dir(opts.Path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("store: create data dir: %w", err)
			}
		}
	}

	q := url.Values{}
	// WAL 让读不阻塞写，这里主要是为了让 /metrics 的队列深度查询不影响投递主路径。
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", fmt.Sprintf("synchronous(%s)", opts.Synchronous))
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", opts.BusyTimeout.Milliseconds()))
	q.Add("_pragma", "foreign_keys(1)")
	// 所有事务都以写锁开启：本服务的事务全是短事务，避免读升级为写时抢锁失败。
	q.Set("_txlock", "immediate")

	dsn := opts.Path
	if memory {
		// 内存库必须共享 cache，否则单连接池之外的连接看不到表；同时禁用 WAL。
		dsn = "file::memory:?cache=shared&_pragma=busy_timeout(5000)&_txlock=immediate"
	} else {
		dsn = "file:" + opts.Path + "?" + q.Encode()
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}
	return &SQLite{db: db}, nil
}

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// ---------- 时间编解码 ----------

func ms(t time.Time) int64 { return t.UnixMilli() }

func fromMS(v int64) time.Time { return time.UnixMilli(v).UTC() }

func nullMS(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UnixMilli()
}

func ptrFromMS(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := fromMS(v.Int64)
	return &t
}

// ---------- 写入 ----------

const insertSQL = `
INSERT INTO notifications
  (id, endpoint, idempotency_key, payload, headers, status, attempt, max_attempts,
   next_attempt_at, last_error, last_status_code, created_at, updated_at)
VALUES (?,?,?,?,?,?,?,?,?,'',0,?,?)`

func (s *SQLite) Enqueue(ctx context.Context, n *model.Notification) (*model.Notification, bool, error) {
	headers, err := json.Marshal(n.Headers)
	if err != nil {
		return nil, false, fmt.Errorf("store: marshal headers: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	// 幂等键命中时直接返回已有记录。
	// 先查再插而不是依赖 INSERT 冲突：单进程内连接池只有 1 条连接 + 写事务，
	// 这里不存在竞态；跨进程的竞态仍由唯一索引兜底（下面的插入失败分支）。
	if n.IdempotencyKey != "" {
		existing, err := s.getInTx(ctx, tx, "", n.Endpoint, n.IdempotencyKey)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, false, err
		}
		if existing != nil {
			return existing, false, tx.Commit()
		}
	}

	var idemArg any
	if n.IdempotencyKey != "" {
		idemArg = n.IdempotencyKey
	}
	_, err = tx.ExecContext(ctx, insertSQL,
		n.ID, n.Endpoint, idemArg, []byte(n.Payload), string(headers),
		string(model.StatusPending), 0, n.MaxAttempts,
		ms(n.NextAttemptAt), ms(n.CreatedAt), ms(n.UpdatedAt))
	if err != nil {
		if isUniqueViolation(err) && n.IdempotencyKey != "" {
			// 另一个进程刚刚插入了同一个幂等键。
			existing, gerr := s.getInTx(ctx, tx, "", n.Endpoint, n.IdempotencyKey)
			if gerr != nil {
				return nil, false, gerr
			}
			return existing, false, tx.Commit()
		}
		return nil, false, fmt.Errorf("store: insert notification: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}

	stored := *n
	stored.Status = model.StatusPending
	return &stored, true, nil
}

func isUniqueViolation(err error) bool {
	// modernc 驱动不暴露稳定的错误码常量，这里按消息匹配。
	// 唯一约束是本表唯一的 UNIQUE 索引，误判风险可接受。
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unique constraint failed")
}

// ---------- 领取 ----------

const selectCols = `
  id, endpoint, idempotency_key, payload, headers, status, attempt, max_attempts,
  next_attempt_at, lease_owner, lease_expires_at, last_error, last_status_code,
  created_at, updated_at, completed_at`

func (s *SQLite) ClaimForEndpoint(ctx context.Context, endpoint, owner string, limit int, leaseFor time.Duration) ([]*model.Notification, error) {
	if limit <= 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	leaseExpires := now.Add(leaseFor)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT`+selectCols+`
		  FROM notifications
		 WHERE status = ? AND endpoint = ? AND next_attempt_at <= ?
		 ORDER BY next_attempt_at ASC, created_at ASC
		 LIMIT ?`,
		string(model.StatusPending), endpoint, ms(now), limit)
	if err != nil {
		return nil, fmt.Errorf("store: claim select: %w", err)
	}
	var claimed []*model.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		claimed = append(claimed, n)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(claimed) == 0 {
		return nil, tx.Commit()
	}

	ids := make([]any, 0, len(claimed)+3)
	ids = append(ids, owner, ms(leaseExpires), ms(now))
	placeholders := make([]string, len(claimed))
	for i, n := range claimed {
		placeholders[i] = "?"
		ids = append(ids, n.ID)
	}
	// 再次带上 status 条件，保证这条 UPDATE 本身就是一次合法的 pending -> in_flight 迁移。
	res, err := tx.ExecContext(ctx, `
		UPDATE notifications
		   SET status = 'in_flight', lease_owner = ?, lease_expires_at = ?, updated_at = ?
		 WHERE status = 'pending' AND id IN (`+strings.Join(placeholders, ",")+`)`, ids...)
	if err != nil {
		return nil, fmt.Errorf("store: claim update: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected != int64(len(claimed)) {
		// 单连接串行写下不应发生；出现即说明有并发假设被打破，宁可放弃这一批。
		return nil, fmt.Errorf("store: claim raced, expected %d rows, updated %d", len(claimed), affected)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	for _, n := range claimed {
		n.Status = model.StatusInFlight
		n.LeaseOwner = owner
		exp := leaseExpires
		n.LeaseExpires = &exp
		n.UpdatedAt = now
	}
	return claimed, nil
}

// ---------- 完成 / 失败 ----------

func (s *SQLite) MarkSucceeded(ctx context.Context, id, owner string, attempt, statusCode int) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		   SET status = 'succeeded', attempt = ?, last_status_code = ?, last_error = '',
		       lease_owner = NULL, lease_expires_at = NULL,
		       completed_at = ?, updated_at = ?
		 WHERE id = ? AND status = 'in_flight' AND lease_owner = ?`,
		attempt, statusCode, ms(now), ms(now), id, owner)
	if err != nil {
		return fmt.Errorf("store: mark succeeded: %w", err)
	}
	return s.assertOwned(ctx, res, id)
}

func (s *SQLite) MarkFailure(ctx context.Context, owner string, u FailureUpdate) error {
	now := time.Now().UTC()
	var (
		res sql.Result
		err error
	)
	if u.Dead {
		res, err = s.db.ExecContext(ctx, `
			UPDATE notifications
			   SET status = 'dead', attempt = ?, last_error = ?, last_status_code = ?,
			       lease_owner = NULL, lease_expires_at = NULL,
			       completed_at = ?, updated_at = ?
			 WHERE id = ? AND status = 'in_flight' AND lease_owner = ?`,
			u.Attempt, truncate(u.LastError, 2048), u.LastStatusCode, ms(now), ms(now), u.ID, owner)
	} else {
		res, err = s.db.ExecContext(ctx, `
			UPDATE notifications
			   SET status = 'pending', attempt = ?, last_error = ?, last_status_code = ?,
			       next_attempt_at = ?, lease_owner = NULL, lease_expires_at = NULL,
			       updated_at = ?
			 WHERE id = ? AND status = 'in_flight' AND lease_owner = ?`,
			u.Attempt, truncate(u.LastError, 2048), u.LastStatusCode,
			ms(u.NextAttemptAt), ms(now), u.ID, owner)
	}
	if err != nil {
		return fmt.Errorf("store: mark failure: %w", err)
	}
	return s.assertOwned(ctx, res, u.ID)
}

// assertOwned 把"0 行受影响"翻译成有意义的错误：
// 要么记录不存在，要么 lease 已被回收（该任务已被别人接管，本次结果必须丢弃）。
func (s *SQLite) assertOwned(ctx context.Context, res sql.Result, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM notifications WHERE id = ?`, id).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return fmt.Errorf("%w: lease no longer held for %s", ErrConflict, id)
}

func (s *SQLite) RecoverExpiredLeases(ctx context.Context, now time.Time) (int, error) {
	// lease 过期意味着上一个持有者已经失联（进程崩溃、被 kill、或投递耗时远超 lease）。
	//
	// 这里连带把 attempt +1：崩溃也要消耗重试预算。
	// 否则一条能稳定打挂进程的"毒任务"会被无限次回收重放，
	// 把单条脏数据放大成全系统不可用。预算耗尽则直接进死信等人工处理。
	res, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		   SET attempt = attempt + 1,
		       status = CASE WHEN attempt + 1 >= max_attempts THEN 'dead' ELSE 'pending' END,
		       completed_at = CASE WHEN attempt + 1 >= max_attempts THEN ? ELSE NULL END,
		       last_error = 'lease expired: previous delivery attempt did not report a result',
		       next_attempt_at = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
		 WHERE status = 'in_flight' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?`,
		ms(now), ms(now), ms(now), ms(now))
	if err != nil {
		return 0, fmt.Errorf("store: recover leases: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ---------- 查询 ----------

func (s *SQLite) Get(ctx context.Context, id string) (*model.Notification, []*model.Attempt, error) {
	n, err := s.getInTx(ctx, s.db, id, "", "")
	if err != nil {
		return nil, nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, notification_id, attempt_no, started_at, duration_ms, outcome,
		       status_code, error, response_body, target_url
		  FROM delivery_attempts WHERE notification_id = ? ORDER BY attempt_no ASC, id ASC`, id)
	if err != nil {
		return nil, nil, fmt.Errorf("store: list attempts: %w", err)
	}
	defer rows.Close()
	var attempts []*model.Attempt
	for rows.Next() {
		var (
			a       model.Attempt
			started int64
			outcome string
		)
		if err := rows.Scan(&a.ID, &a.NotificationID, &a.AttemptNo, &started, &a.DurationMS,
			&outcome, &a.StatusCode, &a.Error, &a.ResponseBody, &a.TargetURL); err != nil {
			return nil, nil, err
		}
		a.StartedAt = fromMS(started)
		a.Outcome = model.Outcome(outcome)
		attempts = append(attempts, &a)
	}
	return n, attempts, rows.Err()
}

// querier 让同一段读逻辑既能跑在 *sql.DB 上也能跑在事务里。
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// getInTx 按主键或 (endpoint, idempotency_key) 读取单条记录。
func (s *SQLite) getInTx(ctx context.Context, q querier, id, endpoint, idemKey string) (*model.Notification, error) {
	var row *sql.Row
	if id != "" {
		row = q.QueryRowContext(ctx, `SELECT`+selectCols+` FROM notifications WHERE id = ?`, id)
	} else {
		row = q.QueryRowContext(ctx, `SELECT`+selectCols+`
			FROM notifications WHERE endpoint = ? AND idempotency_key = ?`, endpoint, idemKey)
	}
	n, err := scanNotification(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return n, err
}

// scanner 抽象 *sql.Row 与 *sql.Rows 的共同能力。
type scanner interface{ Scan(dest ...any) error }

func scanNotification(sc scanner) (*model.Notification, error) {
	var (
		n            model.Notification
		idem         sql.NullString
		payload      []byte
		headersJSON  string
		status       string
		nextAttempt  int64
		leaseOwner   sql.NullString
		leaseExpires sql.NullInt64
		createdAt    int64
		updatedAt    int64
		completedAt  sql.NullInt64
	)
	if err := sc.Scan(&n.ID, &n.Endpoint, &idem, &payload, &headersJSON, &status,
		&n.Attempt, &n.MaxAttempts, &nextAttempt, &leaseOwner, &leaseExpires,
		&n.LastError, &n.LastStatusCode, &createdAt, &updatedAt, &completedAt); err != nil {
		return nil, err
	}
	n.IdempotencyKey = idem.String
	n.Payload = json.RawMessage(payload)
	if headersJSON != "" && headersJSON != "null" {
		if err := json.Unmarshal([]byte(headersJSON), &n.Headers); err != nil {
			return nil, fmt.Errorf("store: unmarshal headers for %s: %w", n.ID, err)
		}
	}
	n.Status = model.Status(status)
	n.NextAttemptAt = fromMS(nextAttempt)
	n.LeaseOwner = leaseOwner.String
	n.LeaseExpires = ptrFromMS(leaseExpires)
	n.CreatedAt = fromMS(createdAt)
	n.UpdatedAt = fromMS(updatedAt)
	n.CompletedAt = ptrFromMS(completedAt)
	return &n, nil
}

func (s *SQLite) List(ctx context.Context, f ListFilter) ([]*model.Notification, error) {
	q := `SELECT` + selectCols + ` FROM notifications WHERE 1=1`
	var args []any
	if f.Status != "" {
		q += ` AND status = ?`
		args = append(args, string(f.Status))
	}
	if f.Endpoint != "" {
		q += ` AND endpoint = ?`
		args = append(args, f.Endpoint)
	}
	if !f.CreatedBefore.IsZero() {
		q += ` AND created_at < ?`
		args = append(args, ms(f.CreatedBefore))
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list: %w", err)
	}
	defer rows.Close()
	var out []*model.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *SQLite) Requeue(ctx context.Context, id string, extraAttempts int) (*model.Notification, error) {
	if extraAttempts <= 0 {
		extraAttempts = 1
	}
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		   SET status = 'pending', max_attempts = attempt + ?, next_attempt_at = ?,
		       completed_at = NULL, last_error = '', updated_at = ?
		 WHERE id = ? AND status = 'dead'`,
		extraAttempts, ms(now), ms(now), id)
	if err != nil {
		return nil, fmt.Errorf("store: requeue: %w", err)
	}
	if err := s.assertMutated(ctx, res, id, model.StatusDead); err != nil {
		return nil, err
	}
	return s.getInTx(ctx, s.db, id, "", "")
}

func (s *SQLite) Cancel(ctx context.Context, id string) (*model.Notification, error) {
	now := time.Now().UTC()
	// 只允许取消 pending：in_flight 的请求可能已经落到下游，
	// 这时返回"已取消"是在给业务方一个我们兜不住的承诺。
	res, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		   SET status = 'canceled', lease_owner = NULL, lease_expires_at = NULL,
		       completed_at = ?, updated_at = ?
		 WHERE id = ? AND status = 'pending'`,
		ms(now), ms(now), id)
	if err != nil {
		return nil, fmt.Errorf("store: cancel: %w", err)
	}
	if err := s.assertMutated(ctx, res, id, model.StatusPending); err != nil {
		return nil, err
	}
	return s.getInTx(ctx, s.db, id, "", "")
}

// assertMutated 把 0 行受影响区分为"不存在"与"当前状态不允许该操作"。
func (s *SQLite) assertMutated(ctx context.Context, res sql.Result, id string, want model.Status) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	cur, err := s.getInTx(ctx, s.db, id, "", "")
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: %s is %s, expected %s", ErrConflict, id, cur.Status, want)
}

func (s *SQLite) RecordAttempt(ctx context.Context, a *model.Attempt) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO delivery_attempts
		  (notification_id, attempt_no, started_at, duration_ms, outcome, status_code, error, response_body, target_url)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		a.NotificationID, a.AttemptNo, ms(a.StartedAt), a.DurationMS, string(a.Outcome),
		a.StatusCode, truncate(a.Error, 1024), truncate(a.ResponseBody, 1024), a.TargetURL)
	if err != nil {
		return fmt.Errorf("store: record attempt: %w", err)
	}
	return nil
}

func (s *SQLite) CountByStatus(ctx context.Context) (map[model.Status]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM notifications GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("store: count by status: %w", err)
	}
	defer rows.Close()
	out := map[model.Status]int64{
		model.StatusPending:   0,
		model.StatusInFlight:  0,
		model.StatusSucceeded: 0,
		model.StatusDead:      0,
		model.StatusCanceled:  0,
	}
	for rows.Next() {
		var (
			status string
			count  int64
		)
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		out[model.Status(status)] = count
	}
	return out, rows.Err()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}
