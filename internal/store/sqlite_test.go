package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"rc_zhangjun/internal/model"
)

// newTestStore 每个用例用独立的临时数据库文件。
//
// 刻意用真实文件而不是 :memory:，因为本层要验证的恰恰是持久化行为
// （lease 回收、崩溃后恢复），内存库测不出重新打开后的状态。
func newTestStore(t *testing.T) *SQLite {
	t.Helper()
	st, err := Open(Options{
		Path: filepath.Join(t.TempDir(), "test.db"),
		// 测试里用 OFF 换速度：这里验证的是 SQL 逻辑，不是掉电持久性。
		Synchronous: "OFF",
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func newNotification(id, endpoint, idemKey string) *model.Notification {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return &model.Notification{
		ID:             id,
		Endpoint:       endpoint,
		IdempotencyKey: idemKey,
		Payload:        json.RawMessage(`{"hello":"world"}`),
		Headers:        map[string]string{"X-Request-Id": "req-" + id},
		Status:         model.StatusPending,
		MaxAttempts:    3,
		NextAttemptAt:  now,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func TestEnqueueRoundTrip(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	in := newNotification("n1", "crm", "key-1")

	stored, created, err := st.Enqueue(ctx, in)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true for a fresh notification")
	}
	if stored.Status != model.StatusPending {
		t.Errorf("status = %s, want pending", stored.Status)
	}

	got, attempts, err := st.Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(attempts) != 0 {
		t.Errorf("attempts = %d, want 0", len(attempts))
	}
	if string(got.Payload) != `{"hello":"world"}` {
		t.Errorf("payload = %s, want it stored verbatim", got.Payload)
	}
	if got.Headers["X-Request-Id"] != "req-n1" {
		t.Errorf("headers round-trip failed: %v", got.Headers)
	}
	// 时间必须能原样取回（毫秒精度），否则调度和排期都会错。
	if !got.NextAttemptAt.Equal(in.NextAttemptAt) {
		t.Errorf("next_attempt_at = %s, want %s", got.NextAttemptAt, in.NextAttemptAt)
	}
}

// 入口幂等是业务方最直接的收益：网络抖动导致的重复提交不会变成重复通知。
func TestEnqueueIsIdempotentPerEndpointAndKey(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	first, created, err := st.Enqueue(ctx, newNotification("n1", "crm", "order-1"))
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}

	// 同 endpoint + 同幂等键：返回已有记录，不新建。
	second, created, err := st.Enqueue(ctx, newNotification("n2", "crm", "order-1"))
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if created {
		t.Error("created = true, want false for a duplicate idempotency key")
	}
	if second.ID != first.ID {
		t.Errorf("returned id = %s, want the original %s", second.ID, first.ID)
	}

	// 幂等键由各业务系统自行生成，跨 endpoint 撞键不该互相影响。
	third, created, err := st.Enqueue(ctx, newNotification("n3", "adnetwork", "order-1"))
	if err != nil || !created {
		t.Fatalf("cross-endpoint enqueue: created=%v err=%v", created, err)
	}
	if third.ID != "n3" {
		t.Errorf("id = %s, want n3", third.ID)
	}
}

// 不带幂等键的提交必须互不干扰，否则部分索引写错会让它们全部撞在 NULL 上。
func TestEnqueueWithoutIdempotencyKeyNeverDeduplicates(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, created, err := st.Enqueue(ctx, newNotification(fmt.Sprintf("n%d", i), "crm", ""))
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		if !created {
			t.Fatalf("enqueue %d: created = false, want true", i)
		}
	}
	counts, err := st.CountByStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.StatusPending] != 3 {
		t.Errorf("pending = %d, want 3", counts[model.StatusPending])
	}
}

// 按 endpoint 分别领取是故障隔离的基础：crm 堆积不能挡住 adnetwork。
func TestClaimIsScopedToEndpoint(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		mustEnqueue(t, st, newNotification(fmt.Sprintf("crm%d", i), "crm", ""))
	}
	mustEnqueue(t, st, newNotification("ad0", "adnetwork", ""))

	claimed, err := st.ClaimForEndpoint(ctx, "adnetwork", "owner-a", 10, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "ad0" {
		t.Fatalf("claimed %d tasks, want only ad0", len(claimed))
	}
	if claimed[0].Status != model.StatusInFlight {
		t.Errorf("status = %s, want in_flight", claimed[0].Status)
	}
	if claimed[0].LeaseOwner != "owner-a" {
		t.Errorf("lease_owner = %q, want owner-a", claimed[0].LeaseOwner)
	}
	if claimed[0].LeaseExpires == nil {
		t.Error("lease_expires_at must be set on claim")
	}
}

func TestClaimRespectsLimitAndSkipsFutureTasks(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		mustEnqueue(t, st, newNotification(fmt.Sprintf("n%d", i), "crm", ""))
	}
	// 一条排在未来（模拟退避中）。
	future := newNotification("later", "crm", "")
	future.NextAttemptAt = time.Now().UTC().Add(time.Hour)
	mustEnqueue(t, st, future)

	claimed, err := st.ClaimForEndpoint(ctx, "crm", "owner-a", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2 (limit)", len(claimed))
	}

	// 把剩下的到期任务都领走，确认退避中的那条不会被提前领取。
	rest, err := st.ClaimForEndpoint(ctx, "crm", "owner-a", 100, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 3 {
		t.Fatalf("claimed %d, want 3 (the backing-off task must stay queued)", len(rest))
	}
	for _, n := range rest {
		if n.ID == "later" {
			t.Error("a task scheduled in the future was claimed early")
		}
	}
}

// 已被领取的任务不能被第二次领取，否则同一条通知会被并发投递两次。
func TestClaimedTasksAreNotClaimedTwice(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))

	first, err := st.ClaimForEndpoint(ctx, "crm", "owner-a", 10, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim: %d tasks, err=%v", len(first), err)
	}
	second, err := st.ClaimForEndpoint(ctx, "crm", "owner-b", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Errorf("second claim got %d tasks, want 0", len(second))
	}
}

func TestMarkSucceeded(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))
	claimed, _ := st.ClaimForEndpoint(ctx, "crm", "owner-a", 1, time.Minute)

	if err := st.MarkSucceeded(ctx, "n1", "owner-a", 1, 200); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}
	_ = claimed

	got, _, err := st.Get(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.StatusSucceeded {
		t.Errorf("status = %s, want succeeded", got.Status)
	}
	if got.Attempt != 1 || got.LastStatusCode != 200 {
		t.Errorf("attempt=%d status_code=%d, want 1/200", got.Attempt, got.LastStatusCode)
	}
	if got.CompletedAt == nil {
		t.Error("completed_at must be set")
	}
	if got.LeaseOwner != "" {
		t.Errorf("lease_owner = %q, want cleared", got.LeaseOwner)
	}
}

// 这是并发安全的核心保证：lease 已被回收（任务交给别人了）时，
// 原持有者的写回必须失败，否则会出现"两个 worker 同时改一条任务"的脏状态。
func TestWritebackRequiresHoldingTheLease(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))
	st.ClaimForEndpoint(ctx, "crm", "owner-a", 1, time.Minute)

	err := st.MarkSucceeded(ctx, "n1", "owner-b", 1, 200)
	if !errors.Is(err, ErrConflict) {
		t.Errorf("err = %v, want ErrConflict when another owner holds the lease", err)
	}

	err = st.MarkFailure(ctx, "owner-b", FailureUpdate{
		ID: "n1", Attempt: 1, NextAttemptAt: time.Now().Add(time.Second),
	})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("MarkFailure err = %v, want ErrConflict", err)
	}

	// 真正的持有者仍然能正常写回。
	if err := st.MarkSucceeded(ctx, "n1", "owner-a", 1, 200); err != nil {
		t.Errorf("the real lease holder should succeed, got %v", err)
	}
}

func TestMarkFailureReschedulesForRetry(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))
	st.ClaimForEndpoint(ctx, "crm", "owner-a", 1, time.Minute)

	next := time.Now().UTC().Add(30 * time.Second).Truncate(time.Millisecond)
	err := st.MarkFailure(ctx, "owner-a", FailureUpdate{
		ID: "n1", Attempt: 1, LastError: "HTTP 503",
		LastStatusCode: 503, NextAttemptAt: next,
	})
	if err != nil {
		t.Fatalf("mark failure: %v", err)
	}

	got, _, _ := st.Get(ctx, "n1")
	if got.Status != model.StatusPending {
		t.Errorf("status = %s, want pending (queued for retry)", got.Status)
	}
	if !got.NextAttemptAt.Equal(next) {
		t.Errorf("next_attempt_at = %s, want %s", got.NextAttemptAt, next)
	}
	if got.CompletedAt != nil {
		t.Error("completed_at must stay nil for a task that will be retried")
	}

	// 排到未来之后，立刻领取应该拿不到它。
	claimed, _ := st.ClaimForEndpoint(ctx, "crm", "owner-a", 10, time.Minute)
	if len(claimed) != 0 {
		t.Errorf("claimed %d tasks, want 0 while backing off", len(claimed))
	}
}

func TestMarkFailureDead(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))
	st.ClaimForEndpoint(ctx, "crm", "owner-a", 1, time.Minute)

	err := st.MarkFailure(ctx, "owner-a", FailureUpdate{
		ID: "n1", Attempt: 3, LastError: "HTTP 400", LastStatusCode: 400, Dead: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _, _ := st.Get(ctx, "n1")
	if got.Status != model.StatusDead {
		t.Errorf("status = %s, want dead", got.Status)
	}
	if got.CompletedAt == nil {
		t.Error("completed_at must be set for a dead notification")
	}
}

// 进程崩溃后的恢复路径。这是 at-least-once 承诺的兜底机制，
// 也是重复投递的主要来源，所以行为必须被精确锁定。
func TestRecoverExpiredLeasesRequeuesAndConsumesBudget(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))

	// lease 只给 1ms，等一下就过期，模拟持有者失联。
	if _, err := st.ClaimForEndpoint(ctx, "crm", "owner-gone", 1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	n, err := st.RecoverExpiredLeases(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered %d, want 1", n)
	}

	got, _, _ := st.Get(ctx, "n1")
	if got.Status != model.StatusPending {
		t.Errorf("status = %s, want pending", got.Status)
	}
	// 崩溃也要消耗预算：否则一条能稳定打挂进程的"毒任务"会被无限回收重放。
	if got.Attempt != 1 {
		t.Errorf("attempt = %d, want 1 (a crash must consume retry budget)", got.Attempt)
	}
	if got.LeaseOwner != "" {
		t.Errorf("lease_owner = %q, want cleared", got.LeaseOwner)
	}

	// 失联的原持有者事后写回必须被拒绝。
	if err := st.MarkSucceeded(ctx, "n1", "owner-gone", 1, 200); !errors.Is(err, ErrConflict) {
		t.Errorf("stale writeback err = %v, want ErrConflict", err)
	}
}

// 反复崩溃的任务必须在预算耗尽时进死信，而不是被永久回收重放。
func TestRecoverExpiredLeasesMovesExhaustedTaskToDead(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	n := newNotification("n1", "crm", "")
	n.MaxAttempts = 1
	mustEnqueue(t, st, n)

	st.ClaimForEndpoint(ctx, "crm", "owner-gone", 1, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if _, err := st.RecoverExpiredLeases(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	got, _, _ := st.Get(ctx, "n1")
	if got.Status != model.StatusDead {
		t.Errorf("status = %s, want dead once the budget is exhausted", got.Status)
	}
	if got.CompletedAt == nil {
		t.Error("completed_at must be set")
	}
}

// 未过期的 lease 不能被回收，否则正在投递的请求会被并发重投。
func TestRecoverExpiredLeasesLeavesLiveLeasesAlone(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))
	st.ClaimForEndpoint(ctx, "crm", "owner-a", 1, time.Hour)

	n, err := st.RecoverExpiredLeases(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("recovered %d, want 0 for a live lease", n)
	}
}

func TestRequeueOnlyWorksOnDeadLetters(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))

	// pending 状态不允许重投——它本来就会被投递。
	if _, err := st.Requeue(ctx, "n1", 3); !errors.Is(err, ErrConflict) {
		t.Errorf("requeue of a pending task: err = %v, want ErrConflict", err)
	}

	st.ClaimForEndpoint(ctx, "crm", "owner-a", 1, time.Minute)
	st.MarkFailure(ctx, "owner-a", FailureUpdate{ID: "n1", Attempt: 3, Dead: true, LastError: "HTTP 500"})

	got, err := st.Requeue(ctx, "n1", 2)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if got.Status != model.StatusPending {
		t.Errorf("status = %s, want pending", got.Status)
	}
	// 追加预算而不是清零 attempt：保留"这条通知一共试过几次"的事实，
	// 排障时才能看出它是第一次失败还是已经被人工救过三轮了。
	if got.MaxAttempts != 5 {
		t.Errorf("max_attempts = %d, want 5 (attempt 3 + 2 extra)", got.MaxAttempts)
	}
	if got.Attempt != 3 {
		t.Errorf("attempt = %d, want the history preserved at 3", got.Attempt)
	}
	if got.CompletedAt != nil {
		t.Error("completed_at must be cleared on requeue")
	}
}

func TestRequeueOfSucceededIsRejected(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))
	st.ClaimForEndpoint(ctx, "crm", "owner-a", 1, time.Minute)
	st.MarkSucceeded(ctx, "n1", "owner-a", 1, 200)

	// 重投一条已成功的通知会造成业务上可见的重复，必须拒绝。
	if _, err := st.Requeue(ctx, "n1", 3); !errors.Is(err, ErrConflict) {
		t.Errorf("err = %v, want ErrConflict", err)
	}
}

func TestCancelOnlyWorksOnPending(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))

	got, err := st.Cancel(ctx, "n1")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got.Status != model.StatusCanceled {
		t.Errorf("status = %s, want canceled", got.Status)
	}

	// 已取消的任务不会再被领取。
	claimed, _ := st.ClaimForEndpoint(ctx, "crm", "owner-a", 10, time.Minute)
	if len(claimed) != 0 {
		t.Errorf("claimed %d canceled tasks, want 0", len(claimed))
	}
}

// in_flight 的请求可能已经落到下游，这时返回"已取消"是在给业务方
// 一个我们兜不住的承诺，所以必须拒绝。
func TestCancelRejectsInFlight(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))
	st.ClaimForEndpoint(ctx, "crm", "owner-a", 1, time.Minute)

	if _, err := st.Cancel(ctx, "n1"); !errors.Is(err, ErrConflict) {
		t.Errorf("err = %v, want ErrConflict for an in-flight notification", err)
	}
}

func TestGetNotFound(t *testing.T) {
	st := newTestStore(t)
	if _, _, err := st.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestRecordAttemptAndHistoryOrdering(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))

	for i := 3; i >= 1; i-- { // 乱序写入，验证读取时按 attempt_no 排序
		err := st.RecordAttempt(ctx, &model.Attempt{
			NotificationID: "n1", AttemptNo: i, StartedAt: time.Now().UTC(),
			DurationMS: int64(i * 10), Outcome: model.OutcomeRetryable,
			StatusCode: 503, Error: "HTTP 503", TargetURL: "http://x/y",
		})
		if err != nil {
			t.Fatalf("record attempt %d: %v", i, err)
		}
	}

	_, attempts, err := st.Get(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 3 {
		t.Fatalf("attempts = %d, want 3", len(attempts))
	}
	for i, a := range attempts {
		if a.AttemptNo != i+1 {
			t.Errorf("attempts[%d].AttemptNo = %d, want %d", i, a.AttemptNo, i+1)
		}
	}
}

// 超长错误信息和响应体必须被截断，否则一次死信风暴就能把磁盘写满。
func TestRecordAttemptTruncatesLongFields(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	mustEnqueue(t, st, newNotification("n1", "crm", ""))

	huge := make([]byte, 100_000)
	for i := range huge {
		huge[i] = 'x'
	}
	err := st.RecordAttempt(ctx, &model.Attempt{
		NotificationID: "n1", AttemptNo: 1, StartedAt: time.Now().UTC(),
		Outcome: model.OutcomeRetryable, Error: string(huge), ResponseBody: string(huge),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, attempts, _ := st.Get(ctx, "n1")
	if len(attempts[0].Error) > 2048 {
		t.Errorf("error field is %d bytes, want truncated", len(attempts[0].Error))
	}
	if len(attempts[0].ResponseBody) > 2048 {
		t.Errorf("response body is %d bytes, want truncated", len(attempts[0].ResponseBody))
	}
}

func TestListFiltersAndOrdering(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		n := newNotification(fmt.Sprintf("crm%d", i), "crm", "")
		n.CreatedAt = time.Now().UTC().Add(time.Duration(i) * time.Second)
		mustEnqueue(t, st, n)
	}
	mustEnqueue(t, st, newNotification("ad0", "adnetwork", ""))

	byEndpoint, err := st.List(ctx, ListFilter{Endpoint: "crm"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byEndpoint) != 3 {
		t.Errorf("endpoint filter returned %d, want 3", len(byEndpoint))
	}
	// 最新的排最前，方便运维一眼看到刚出问题的那批。
	if byEndpoint[0].ID != "crm2" {
		t.Errorf("first item = %s, want crm2 (newest first)", byEndpoint[0].ID)
	}

	byStatus, err := st.List(ctx, ListFilter{Status: model.StatusDead})
	if err != nil {
		t.Fatal(err)
	}
	if len(byStatus) != 0 {
		t.Errorf("dead filter returned %d, want 0", len(byStatus))
	}

	limited, err := st.List(ctx, ListFilter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 {
		t.Errorf("limit=2 returned %d", len(limited))
	}
}

func TestCountByStatusAlwaysReportsAllStates(t *testing.T) {
	st := newTestStore(t)
	counts, err := st.CountByStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 空库时也要为每个状态输出 0，否则 Prometheus 里这些时间序列会缺失，
	// 告警规则在"从未出现过死信"和"死信指标挂了"之间无法区分。
	for _, s := range []model.Status{
		model.StatusPending, model.StatusInFlight, model.StatusSucceeded,
		model.StatusDead, model.StatusCanceled,
	} {
		if _, ok := counts[s]; !ok {
			t.Errorf("status %s missing from CountByStatus", s)
		}
	}
}

// 队列必须跨进程重启存活——这是"可靠送达"最基本的要求。
func TestDataSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	st, err := Open(Options{Path: path, Synchronous: "FULL"})
	if err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, st, newNotification("n1", "crm", "key-1"))
	st.Close()

	reopened, err := Open(Options{Path: path, Synchronous: "FULL"})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, _, err := reopened.Get(context.Background(), "n1")
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if got.Status != model.StatusPending || got.IdempotencyKey != "key-1" {
		t.Errorf("recovered notification = %+v, want the original pending record", got)
	}
}

func mustEnqueue(t *testing.T, st *SQLite, n *model.Notification) {
	t.Helper()
	if _, _, err := st.Enqueue(context.Background(), n); err != nil {
		t.Fatalf("enqueue %s: %v", n.ID, err)
	}
}
