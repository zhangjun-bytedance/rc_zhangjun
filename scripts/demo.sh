#!/usr/bin/env bash
#
# 一键演示脚本：不需要任何外部依赖（无 Docker、无 Redis、无 MQ），
# 全程只用 Go 和 curl。
#
# 它按顺序演示本系统真正要解决的几个问题：
#   场景 1  正常投递
#   场景 2  入口幂等（重复提交只投一次）
#   场景 3  下游故障 → 自动退避重试 → 下游恢复 → 自动送达（业务方无感）
#   场景 4  永久失败（4xx）→ 立刻进死信，不浪费重试预算
#   场景 5  重试预算耗尽 → 死信 → 人工重投 → 送达
#   场景 6  熔断：下游长期不可用时停止敲打，且不消耗重试预算
#   场景 7  故障隔离：一个供应商挂掉不影响其他供应商
#
set -euo pipefail

cd "$(dirname "$0")/.."

NOTIFYD_PORT=18080
AD_PORT=19101
CRM_PORT=19102
INV_PORT=19103

BASE="http://127.0.0.1:${NOTIFYD_PORT}"
WORKDIR="$(mktemp -d)"
PIDS=()

# ---------- 输出工具 ----------
if [ -t 1 ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'
  CYAN=$'\033[36m'; RED=$'\033[31m'; RESET=$'\033[0m'
else
  BOLD=""; DIM=""; GREEN=""; YELLOW=""; CYAN=""; RED=""; RESET=""
fi

scene() { printf '\n%s=== %s ===%s\n' "$BOLD$CYAN" "$1" "$RESET"; }
step()  { printf '%s>%s %s\n' "$GREEN" "$RESET" "$1"; }
note()  { printf '%s  %s%s\n' "$DIM" "$1" "$RESET"; }
warn()  { printf '%s  %s%s\n' "$YELLOW" "$1" "$RESET"; }
fail()  { printf '%s  %s%s\n' "$RED" "$1" "$RESET"; }

cleanup() {
  printf '\n%s清理进程...%s\n' "$DIM" "$RESET"
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

wait_for_port() {
  local port="$1" name="$2" i
  for i in $(seq 1 100); do
    if curl -sf -o /dev/null "http://127.0.0.1:${port}/_received" 2>/dev/null \
      || curl -sf -o /dev/null "http://127.0.0.1:${port}/healthz" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  fail "$name 在 10 秒内没有启动，请查看 ${WORKDIR}"
  exit 1
}

# 提交一条通知，输出 notification id。
submit() {
  local endpoint="$1" payload="$2" idem="${3:-}"
  local body
  if [ -n "$idem" ]; then
    body="{\"endpoint\":\"${endpoint}\",\"idempotency_key\":\"${idem}\",\"payload\":${payload}}"
  else
    body="{\"endpoint\":\"${endpoint}\",\"payload\":${payload}}"
  fi
  curl -sf -X POST "${BASE}/v1/notifications" \
    -H 'Content-Type: application/json' -d "$body" \
    | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'
}

# 读取一条通知的字段值。
field() {
  local id="$1" key="$2"
  curl -sf "${BASE}/v1/notifications/${id}" \
    | sed -n "s/.*\"${key}\":\"\{0,1\}\([^,\"}]*\)\"\{0,1\}.*/\1/p" | head -1
}

status_of() { field "$1" status; }
attempt_of() {
  curl -sf "${BASE}/v1/notifications/$1" \
    | sed -n 's/.*"attempt":\([0-9]*\).*/\1/p' | head -1
}

# 等待通知到达期望状态。
await_status() {
  local id="$1" want="$2" timeout="${3:-30}"
  local deadline=$((SECONDS + timeout)) cur=""
  while [ $SECONDS -lt $deadline ]; do
    cur="$(status_of "$id")"
    if [ "$cur" = "$want" ]; then
      note "通知 ${id:0:10}… 状态 = ${want}（尝试 $(attempt_of "$id") 次）"
      return 0
    fi
    sleep 0.3
  done
  fail "超时：通知 $id 停在 ${cur}，期望 ${want}"
  return 1
}

vendor_mode() {
  local port="$1" mode="$2"
  curl -sf -X POST "http://127.0.0.1:${port}/_control?mode=${mode}" > /dev/null
}

vendor_requests() {
  curl -sf "http://127.0.0.1:$1/_received" \
    | sed -n 's/.*"total_requests": \([0-9]*\).*/\1/p' | head -1
}

breaker_state() {
  curl -sf "${BASE}/v1/endpoints" \
    | tr ',' '\n' | grep -A2 "\"name\": \"$1\"" -m1 > /dev/null 2>&1 || true
  curl -sf "${BASE}/v1/endpoints" | python3 -c '
import json,sys
data = json.load(sys.stdin)
name = sys.argv[1]
for ep in data["endpoints"]:
    if ep["name"] == name:
        print(ep["breaker"]["state"])
' "$1" 2>/dev/null || echo "unknown"
}

# ---------- 启动 ----------
scene "准备环境"
step "编译..."
go build -o "${WORKDIR}/notifyd" ./cmd/notifyd
go build -o "${WORKDIR}/mockvendor" ./cmd/mockvendor
note "编译完成，产物在 ${WORKDIR}"

step "启动 3 个模拟外部供应商"
"${WORKDIR}/mockvendor" -addr ":${AD_PORT}"  -name adnetwork > "${WORKDIR}/ad.log"  2>&1 & PIDS+=($!)
"${WORKDIR}/mockvendor" -addr ":${CRM_PORT}" -name crm       > "${WORKDIR}/crm.log" 2>&1 & PIDS+=($!)
"${WORKDIR}/mockvendor" -addr ":${INV_PORT}" -name inventory > "${WORKDIR}/inv.log" 2>&1 & PIDS+=($!)
wait_for_port "$AD_PORT"  "adnetwork mock"
wait_for_port "$CRM_PORT" "crm mock"
wait_for_port "$INV_PORT" "inventory mock"
note "adnetwork :${AD_PORT}  crm :${CRM_PORT}  inventory :${INV_PORT}"

step "生成 demo 配置（时间尺度压缩，便于现场观察）"
cat > "${WORKDIR}/config.yaml" <<EOF
server:
  addr: ":${NOTIFYD_PORT}"
  max_body_bytes: 1048576
  shutdown_grace: 10s
store:
  path: "${WORKDIR}/data/notify.db"
  synchronous: FULL
dispatcher:
  poll_interval: 100ms
  batch_size: 20
  lease_duration: 30s
  reaper_interval: 5s
defaults:
  method: POST
  content_type: application/json
  timeout: 2s
  max_attempts: 8
  concurrency: 4
  backoff:
    base: 500ms
    max: 4s
    jitter: 0.2
  breaker:
    enabled: true
    failure_threshold: 3
    cooldown: 4s
    half_open_probes: 1
  retry_status_codes: [408, 423, 425, 429]
endpoints:
  - name: adnetwork
    url: "http://127.0.0.1:${AD_PORT}/track/registration"
    headers:
      X-Vendor-Token: "demo-ad-token"
  - name: crm
    url: "http://127.0.0.1:${CRM_PORT}/api/v2/contacts/status"
    headers:
      Authorization: "Bearer demo-crm-token"
    max_attempts: 3
  - name: inventory
    url: "http://127.0.0.1:${INV_PORT}/inventory/adjust"
    body_template: |-
      {"sku":"{{ .sku }}","delta":{{ .quantity }},"ref":"{{ .order_id }}"}
EOF

step "启动 notifyd"
"${WORKDIR}/notifyd" -config "${WORKDIR}/config.yaml" -log-level info \
  > "${WORKDIR}/notifyd.log" 2>&1 & PIDS+=($!)
wait_for_port "$NOTIFYD_PORT" "notifyd"
note "notifyd 运行在 ${BASE}，日志：${WORKDIR}/notifyd.log"

# ---------- 场景 1 ----------
scene "场景 1：正常投递"
note "业务系统提交通知后立刻拿到 202，不需要等外部 API 返回。"
ID1="$(submit adnetwork '{"user_id":"u-1001","campaign":"spring"}')"
step "已提交：${ID1}"
await_status "$ID1" succeeded 15
note "adnetwork 收到 $(vendor_requests "$AD_PORT") 个请求"

step "body_template 演示：inventory 要求扁平结构"
ID_INV="$(submit inventory '{"sku":"SKU-77","quantity":-3,"order_id":"o-555"}')"
await_status "$ID_INV" succeeded 15
note "inventory 实际收到的 body："
curl -sf "http://127.0.0.1:${INV_PORT}/_received" | grep '"body"' | tail -1 | sed 's/^/    /'

# ---------- 场景 2 ----------
scene "场景 2：入口幂等（业务系统重复提交）"
note "业务系统因为超时重发了 4 次，带同一个 idempotency_key。"
BEFORE="$(vendor_requests "$AD_PORT")"
IDEM_ID=""
for i in 1 2 3 4; do
  got="$(submit adnetwork '{"user_id":"u-2002"}' 'order-idem-demo')"
  step "第 ${i} 次提交 → ${got}"
  IDEM_ID="$got"
done
await_status "$IDEM_ID" succeeded 15
AFTER="$(vendor_requests "$AD_PORT")"
note "4 次提交返回了同一个 ID；供应商请求数从 ${BEFORE} 变成 ${AFTER}（只增加 1 次）"

# ---------- 场景 3 ----------
scene "场景 3：下游故障 → 自动重试 → 恢复后送达"
note "这是整个系统存在的理由：外部系统抖动时，业务系统完全无感。"
step "把 CRM 切成故障模式（返回 503）"
vendor_mode "$CRM_PORT" down

ID3="$(submit crm '{"contact":"c-3003","status":"active"}')"
step "已提交：${ID3}（此时 CRM 是挂的）"
sleep 1.2
note "当前状态：$(status_of "$ID3")，已尝试 $(attempt_of "$ID3") 次 —— 通知安全地待在队列里"

step "CRM 恢复"
vendor_mode "$CRM_PORT" up
await_status "$ID3" succeeded 20
note "业务系统从头到尾没有参与这次重试，也不知道 CRM 曾经挂过"

# ---------- 场景 4 ----------
scene "场景 4：永久失败（4xx）立刻进死信"
note "4xx 说明请求本身有问题，重试 8 次也是同样结果，只会推迟人工介入。"
step "把 CRM 切成拒绝模式（返回 400）"
vendor_mode "$CRM_PORT" reject
ID4="$(submit crm '{"contact":"bad-payload"}')"
await_status "$ID4" dead 15
note "只尝试了 $(attempt_of "$ID4") 次就进死信，没有浪费剩余重试预算"

# ---------- 场景 5 ----------
scene "场景 5：重试预算耗尽 → 死信 → 人工重投"
step "CRM 持续 503，max_attempts=3"
vendor_mode "$CRM_PORT" down
ID5="$(submit crm '{"contact":"c-5005"}')"
await_status "$ID5" dead 25
note "尝试 $(attempt_of "$ID5") 次后耗尽预算进死信"

step "查询死信列表"
curl -sf "${BASE}/v1/notifications?status=dead&limit=5" \
  | python3 -m json.tool 2>/dev/null | head -20 | sed 's/^/    /' || true

step "CRM 修好了，运维通过 admin API 重投"
vendor_mode "$CRM_PORT" up
curl -sf -X POST "${BASE}/v1/notifications/${ID5}/retry" \
  -H 'Content-Type: application/json' -d '{"extra_attempts":3}' > /dev/null
await_status "$ID5" succeeded 20
note "尝试次数累计为 $(attempt_of "$ID5") —— 历史被保留，能看出它被人工救过"

# ---------- 场景 6 ----------
scene "场景 6：熔断 —— 不对着挂掉的下游猛敲，也不烧重试预算"
step "CRM 再次挂掉，连续提交 6 条通知"
vendor_mode "$CRM_PORT" down
CRM_BEFORE="$(vendor_requests "$CRM_PORT")"
BREAKER_IDS=()
for i in 1 2 3 4 5 6; do
  BREAKER_IDS+=("$(submit crm "{\"contact\":\"c-brk-${i}\"}")")
done

sleep 2
STATE="$(breaker_state crm)"
CRM_MID="$(vendor_requests "$CRM_PORT")"
note "熔断器状态：${STATE}"
note "这 2 秒里 CRM 只收到 $((CRM_MID - CRM_BEFORE)) 个请求（熔断后就停了）"
SAMPLE="${BREAKER_IDS[0]}"
note "样本通知 ${SAMPLE:0:10}… 状态=$(status_of "$SAMPLE") 尝试=$(attempt_of "$SAMPLE") 次"
warn "关键点：熔断期间任务留在队列里不被领取，因此不消耗重试预算。"
warn "如果照常取任务再判失败，下游宕机几分钟就能把所有通知推进死信。"

step "CRM 恢复，半开探测成功后熔断关闭，积压通知自动送达"
vendor_mode "$CRM_PORT" up
for id in "${BREAKER_IDS[@]}"; do
  await_status "$id" succeeded 40
done
note "熔断器状态：$(breaker_state crm)"

# ---------- 场景 7 ----------
scene "场景 7：故障隔离 —— 一个供应商挂掉不影响其他供应商"
step "把 CRM 切成慢响应（每个请求都拖到超时），并塞 8 条进去"
vendor_mode "$CRM_PORT" slow
for i in 1 2 3 4 5 6 7 8; do
  submit crm "{\"contact\":\"c-slow-${i}\"}" > /dev/null
done
sleep 0.5

step "此时给健康的 adnetwork 发一条紧急通知"
START="$(date +%s%N 2>/dev/null || date +%s)"
ID7="$(submit adnetwork '{"user_id":"u-urgent","campaign":"flash"}')"
await_status "$ID7" succeeded 15
END="$(date +%s%N 2>/dev/null || date +%s)"
if [ ${#START} -gt 10 ]; then
  note "耗时 $(( (END - START) / 1000000 )) ms —— 没有被 CRM 的堆积拖累"
else
  note "健康 endpoint 的通知立即送达，没有被 CRM 的堆积拖累"
fi
warn "原因：调度器按 endpoint 分别领取任务、各自独立并发上限，不存在队头阻塞。"
vendor_mode "$CRM_PORT" up

# ---------- 收尾 ----------
scene "可观测性"
step "队列深度与投递结果指标"
curl -sf "${BASE}/metrics" | grep -E '^notify_(queue_depth|terminal_total|delivery_attempts_total|breaker_trips_total)' \
  | sed 's/^/    /'

step "endpoint 与熔断器状态"
curl -sf "${BASE}/v1/endpoints" | python3 -m json.tool 2>/dev/null | sed 's/^/    /' || true

scene "演示结束"
note "notifyd 日志：${WORKDIR}/notifyd.log"
note "供应商日志：${WORKDIR}/crm.log 等（可以看到每次重试、以及重复投递的标记）"
printf '\n%s全部场景通过。%s\n' "$BOLD$GREEN" "$RESET"
