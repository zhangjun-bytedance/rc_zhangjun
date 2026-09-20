-- 通知任务表。既是业务请求记录，也是持久化队列。
--
-- 时间统一存 unix 毫秒整数而不是 TEXT/DATETIME：
-- 排序和范围查询走整数索引，且避免了 SQLite 时间函数与 Go time.Time 之间的时区歧义。
CREATE TABLE IF NOT EXISTS notifications (
    id                TEXT    PRIMARY KEY,
    endpoint          TEXT    NOT NULL,
    idempotency_key   TEXT,
    payload           BLOB    NOT NULL,
    headers           TEXT    NOT NULL DEFAULT '{}',

    status            TEXT    NOT NULL,
    attempt           INTEGER NOT NULL DEFAULT 0,
    max_attempts      INTEGER NOT NULL,

    next_attempt_at   INTEGER NOT NULL,
    lease_owner       TEXT,
    lease_expires_at  INTEGER,

    last_error        TEXT    NOT NULL DEFAULT '',
    last_status_code  INTEGER NOT NULL DEFAULT 0,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    completed_at      INTEGER
);

-- 入口幂等：同一 endpoint 下 idempotency_key 唯一。
-- 用部分索引（WHERE ... IS NOT NULL）让"不带幂等键"的提交不互相冲突。
-- 带上 endpoint 是因为幂等键由各业务系统自行生成，跨供应商撞键不应互相影响。
CREATE UNIQUE INDEX IF NOT EXISTS idx_notifications_idem
    ON notifications (endpoint, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- 调度主路径的索引：按 endpoint 领取到期的 pending 任务。
-- 列顺序对应 WHERE status=? AND endpoint=? AND next_attempt_at<=? ORDER BY next_attempt_at。
CREATE INDEX IF NOT EXISTS idx_notifications_claim
    ON notifications (status, endpoint, next_attempt_at);

-- lease 回收路径的索引。
CREATE INDEX IF NOT EXISTS idx_notifications_lease
    ON notifications (status, lease_expires_at);

-- 死信巡检 / admin 列表。
CREATE INDEX IF NOT EXISTS idx_notifications_list
    ON notifications (status, created_at DESC);

-- 投递尝试审计表。排障时回答"第几次、打到哪、对方回了什么"。
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    notification_id  TEXT    NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    attempt_no       INTEGER NOT NULL,
    started_at       INTEGER NOT NULL,
    duration_ms      INTEGER NOT NULL,
    outcome          TEXT    NOT NULL,
    status_code      INTEGER NOT NULL DEFAULT 0,
    error            TEXT    NOT NULL DEFAULT '',
    response_body    TEXT    NOT NULL DEFAULT '',
    target_url       TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_attempts_notification
    ON delivery_attempts (notification_id, attempt_no);
