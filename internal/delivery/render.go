package delivery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"text/template"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/model"
)

// 注入给下游的标识头。
//
// 这几个头是 at-least-once 语义能够落地的关键：本系统保证"至少送达一次"，
// 但无法保证"恰好一次"（见 README）。把稳定的通知 ID 和尝试次数交给下游，
// 下游就能用它做去重，从而把系统整体行为收敛到"业务上只生效一次"。
const (
	HeaderNotificationID = "X-Notification-Id"
	HeaderAttempt        = "X-Notification-Attempt"
	HeaderEndpoint       = "X-Notification-Endpoint"
	HeaderIdempotencyKey = "X-Idempotency-Key"
	HeaderTimestamp      = "X-Notification-Timestamp"
)

// deniedCallerHeaders 是调用方永远不能透传的 header。
//
// 前半部分是 hop-by-hop 头，透传会破坏连接语义；
// 后半部分是能改变请求路由或身份的头——允许业务系统覆盖 Host 或 Authorization，
// 等于把「凭据集中管理」这个设计前提直接废掉。
var deniedCallerHeaders = map[string]bool{
	"host":                true,
	"authorization":       true,
	"proxy-authorization": true,
	"content-length":      true,
	"content-type":        true,
	"connection":          true,
	"keep-alive":          true,
	"transfer-encoding":   true,
	"te":                  true,
	"trailer":             true,
	"upgrade":             true,
	"expect":              true,
}

// renderedRequest 是一次投递的最终请求形态。
type renderedRequest struct {
	method string
	url    string
	header http.Header
	body   []byte
}

// renderBody 生成请求体。
//
// 默认是原样透传业务系统提交的 payload——通知系统不理解业务语义，
// 多做一层转换就多一个出错点。只有 endpoint 显式配了 body_template 才做渲染。
func renderBody(tpl *template.Template, payload json.RawMessage) ([]byte, error) {
	if tpl == nil {
		if len(payload) == 0 {
			return []byte("{}"), nil
		}
		return payload, nil
	}
	var data any
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &data); err != nil {
			// 模板需要结构化输入，payload 不是合法 JSON 是调用方的问题，
			// 属于永久失败，不该重试。
			return nil, fmt.Errorf("%w: payload is not valid JSON: %v", ErrPermanentRender, err)
		}
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("%w: body_template: %v", ErrPermanentRender, err)
	}
	return buf.Bytes(), nil
}

// renderHeaders 组装请求头。
//
// 顺序即优先级：调用方自定义头最先写入，endpoint 静态头覆盖它，
// 系统标识头最后写入且不可被覆盖。
func renderHeaders(ep config.Endpoint, n *model.Notification, attemptNo int, bodyLen int) http.Header {
	h := make(http.Header, len(ep.Headers)+len(n.Headers)+6)

	for k, v := range n.Headers {
		if !callerHeaderAllowed(ep, k) {
			continue
		}
		h.Set(k, v)
	}
	for k, v := range ep.Headers {
		h.Set(k, v)
	}

	if ep.ContentType != "" && bodyLen > 0 {
		h.Set("Content-Type", ep.ContentType)
	}
	h.Set(HeaderNotificationID, n.ID)
	h.Set(HeaderAttempt, strconv.Itoa(attemptNo))
	h.Set(HeaderEndpoint, n.Endpoint)
	h.Set(HeaderTimestamp, strconv.FormatInt(n.CreatedAt.UnixMilli(), 10))
	if n.IdempotencyKey != "" {
		h.Set(HeaderIdempotencyKey, n.IdempotencyKey)
	}
	return h
}

// callerHeaderAllowed 判断调用方传入的 header 是否允许透传。
//
// 默认策略是「只放行 X- 前缀的自定义头」：这覆盖了真实场景里绝大多数需求
// （链路追踪 ID、业务标记），同时把攻击面压到最小。需要更多就在
// endpoint 的 allow_caller_headers 里显式列出，让放行动作在 review 里可见。
func callerHeaderAllowed(ep config.Endpoint, key string) bool {
	lower := strings.ToLower(key)
	if deniedCallerHeaders[lower] {
		return false
	}
	if strings.HasPrefix(lower, "x-notification-") {
		return false // 系统保留前缀，不允许伪造
	}
	if len(ep.AllowCallerHeaders) == 0 {
		return strings.HasPrefix(lower, "x-")
	}
	for _, allowed := range ep.AllowCallerHeaders {
		if strings.EqualFold(allowed, key) {
			return true
		}
	}
	return false
}
