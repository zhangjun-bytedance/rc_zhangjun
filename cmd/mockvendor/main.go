// Command mockvendor 模拟一个外部供应商的 HTTP API，用于演示和手工验证。
//
// 它能被运行时切换故障模式，这样就可以现场演示本系统真正要解决的问题：
// 下游挂掉 → 通知在队列里退避重试 → 下游恢复 → 通知自动送达，
// 且业务系统对这整个过程无感。
//
// 用法：
//
//	go run ./cmd/mockvendor -addr :9101 -name crm
//	curl -X POST 'localhost:9101/_control?mode=down'    # 开始返回 503
//	curl -X POST 'localhost:9101/_control?mode=up'      # 恢复正常
//	curl 'localhost:9101/_received'                     # 查看收到的通知（含去重统计）
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

// mode 是故障模式。
type mode string

const (
	modeUp      mode = "up"      // 正常返回 200
	modeDown    mode = "down"    // 返回 503（可重试）
	modeReject  mode = "reject"  // 返回 400（永久失败）
	modeSlow    mode = "slow"    // 超过投递超时后才返回，制造超时
	modeFlaky   mode = "flaky"   // 隔次失败
	modeLimited mode = "limited" // 返回 429 + Retry-After
)

type received struct {
	NotificationID string    `json:"notification_id"`
	Attempt        string    `json:"attempt"`
	At             time.Time `json:"at"`
	Body           string    `json:"body"`
}

type vendor struct {
	name  string
	delay time.Duration

	mu sync.Mutex
	// mode 当前故障模式。
	mode mode
	// log 是所有成功接收的通知（按到达顺序）。
	log []received
	// seen 记录每个 notification_id 被成功接收的次数。
	//
	// 这是整个 mock 里最有价值的一个字段：它让「至少一次」语义
	// 造成的重复投递变成可观测的事实，而不是一句口头承诺。
	seen    map[string]int
	counter int
}

func main() {
	var (
		addr     = flag.String("addr", ":9101", "listen address")
		name     = flag.String("name", "vendor", "vendor name, used in logs")
		initMode = flag.String("mode", string(modeUp), "initial mode: up, down, reject, slow, flaky, limited")
		delay    = flag.Duration("slow-delay", 8*time.Second, "response delay in slow mode")
	)
	flag.Parse()

	v := &vendor{
		name:  *name,
		mode:  mode(*initMode),
		delay: *delay,
		seen:  make(map[string]int),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_control", v.handleControl)
	mux.HandleFunc("/_received", v.handleReceived)
	mux.HandleFunc("/_reset", v.handleReset)
	mux.HandleFunc("/", v.handleNotify)

	log.Printf("mock vendor %q listening on %s (mode=%s)", v.name, *addr, v.mode)
	srv := &http.Server{Addr: *addr, Handler: mux, ReadTimeout: 30 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "mockvendor: %v\n", err)
		os.Exit(1)
	}
}

func (v *vendor) handleNotify(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	id := r.Header.Get("X-Notification-Id")
	attempt := r.Header.Get("X-Notification-Attempt")

	v.mu.Lock()
	cur := v.mode
	v.counter++
	n := v.counter
	v.mu.Unlock()

	switch cur {
	case modeDown:
		log.Printf("[%s] REJECT 503 id=%s attempt=%s", v.name, id, attempt)
		http.Error(w, `{"error":"service unavailable"}`, http.StatusServiceUnavailable)
		return
	case modeReject:
		log.Printf("[%s] REJECT 400 id=%s attempt=%s", v.name, id, attempt)
		http.Error(w, `{"error":"invalid payload"}`, http.StatusBadRequest)
		return
	case modeLimited:
		log.Printf("[%s] REJECT 429 id=%s attempt=%s", v.name, id, attempt)
		w.Header().Set("Retry-After", "2")
		http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
		return
	case modeSlow:
		log.Printf("[%s] SLOW  id=%s attempt=%s (sleeping %s)", v.name, id, attempt, v.delay)
		select {
		case <-time.After(v.delay):
		case <-r.Context().Done():
			// 调用方已经超时断开，没必要继续占着 goroutine。
			return
		}
	case modeFlaky:
		if n%2 == 1 {
			log.Printf("[%s] REJECT 502 id=%s attempt=%s (flaky)", v.name, id, attempt)
			http.Error(w, `{"error":"bad gateway"}`, http.StatusBadGateway)
			return
		}
	}

	v.mu.Lock()
	v.seen[id]++
	dupCount := v.seen[id]
	v.log = append(v.log, received{
		NotificationID: id, Attempt: attempt, At: time.Now(), Body: string(body),
	})
	v.mu.Unlock()

	if dupCount > 1 {
		// 这条日志正是 at-least-once 的证据：同一个 ID 被成功接收了不止一次。
		log.Printf("[%s] ACCEPT 200 id=%s attempt=%s (DUPLICATE, seen %d times)",
			v.name, id, attempt, dupCount)
	} else {
		log.Printf("[%s] ACCEPT 200 id=%s attempt=%s", v.name, id, attempt)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"ok":true,"received":%d}`, dupCount)
}

func (v *vendor) handleControl(w http.ResponseWriter, r *http.Request) {
	m := mode(r.URL.Query().Get("mode"))
	switch m {
	case modeUp, modeDown, modeReject, modeSlow, modeFlaky, modeLimited:
	default:
		http.Error(w, "mode must be one of: up, down, reject, slow, flaky, limited", http.StatusBadRequest)
		return
	}
	v.mu.Lock()
	v.mode = m
	v.mu.Unlock()
	log.Printf("[%s] mode switched to %s", v.name, m)
	writeJSON(w, map[string]string{"mode": string(m)})
}

func (v *vendor) handleReceived(w http.ResponseWriter, _ *http.Request) {
	v.mu.Lock()
	defer v.mu.Unlock()
	duplicates := 0
	for _, c := range v.seen {
		if c > 1 {
			duplicates += c - 1
		}
	}
	writeJSON(w, map[string]any{
		"mode":             v.mode,
		"total_requests":   v.counter,
		"accepted":         len(v.log),
		"unique":           len(v.seen),
		"duplicate_extras": duplicates,
		"received":         v.log,
	})
}

func (v *vendor) handleReset(w http.ResponseWriter, _ *http.Request) {
	v.mu.Lock()
	v.log = nil
	v.seen = make(map[string]int)
	v.counter = 0
	v.mu.Unlock()
	writeJSON(w, map[string]string{"status": "reset"})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
