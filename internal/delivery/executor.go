// Package delivery 负责把一条通知真正发到外部供应商的 HTTP API。
//
// 这一层只做一件事并且只做一次：构造请求、发出去、把结果翻译成
// 「成功 / 可重试 / 永久失败」三种判定。它不知道重试排期、不知道熔断、
// 不碰数据库——这些属于 dispatcher。这样切分是为了让"什么算失败"
// 这个最需要被测试和被讨论的判断，能独立于调度逻辑被验证。
package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"text/template"
	"time"

	"rc_zhangjun/internal/config"
	"rc_zhangjun/internal/model"
)

// ErrPermanentRender 表示请求在发出之前就构造失败了（模板错误、payload 非法）。
// 这类错误重试永远不会好，必须直接判永久失败。
var ErrPermanentRender = errors.New("permanent render failure")

// maxResponseSnippet 是保留的响应体字节数上限。
//
// 业务系统不关心外部 API 的返回值，但排障的人关心。折中方案是只留开头一小段：
// 足够看清对方的错误信息，又不会在死信风暴时把磁盘写满。
const maxResponseSnippet = 1024

// Result 是一次投递尝试的结果。
type Result struct {
	Outcome    model.Outcome
	StatusCode int
	Err        error
	Duration   time.Duration
	// RetryAfter 是下游通过 Retry-After 头明确要求的等待时长，0 表示没有要求。
	RetryAfter   time.Duration
	ResponseBody string
	TargetURL    string
	// Transport 为 true 表示失败发生在传输层（连接不上/超时），而非收到了 HTTP 响应。
	Transport bool
}

// Executor 执行投递。并发安全，全局共享一个实例。
type Executor struct {
	client    *http.Client
	endpoints map[string]config.Endpoint
	templates map[string]*template.Template
}

// NewExecutor 创建投递执行器，并预编译所有 endpoint 的 body 模板。
//
// 模板在启动时编译而不是每次投递时编译：一是避免热路径上的重复开销，
// 二是让模板语法错误在进程启动时就暴露，而不是在凌晨三点的第一条通知上暴露。
func NewExecutor(endpoints map[string]config.Endpoint) (*Executor, error) {
	tpls := make(map[string]*template.Template, len(endpoints))
	for name, ep := range endpoints {
		if ep.BodyTemplate == "" {
			continue
		}
		t, err := template.New(name).Option("missingkey=error").Parse(ep.BodyTemplate)
		if err != nil {
			return nil, fmt.Errorf("delivery: endpoint %q body_template: %w", name, err)
		}
		tpls[name] = t
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	// 默认 MaxIdleConnsPerHost 是 2，对一个持续向少量固定域名发请求的服务来说太小，
	// 会导致大量连接反复建立并把 TIME_WAIT 堆满。这里按 endpoint 并发量放大。
	transport.MaxIdleConnsPerHost = 64
	transport.MaxIdleConns = 256
	transport.IdleConnTimeout = 90 * time.Second

	return &Executor{
		// 不设 client.Timeout：超时按 endpoint 配置用 context 控制，
		// 一个全局超时无法同时适配"快返回的广告系统"和"慢的 CRM"。
		client:    &http.Client{Transport: transport},
		endpoints: endpoints,
		templates: tpls,
	}, nil
}

// Deliver 执行第 attemptNo 次投递尝试（attemptNo 从 1 开始）。
func (e *Executor) Deliver(ctx context.Context, n *model.Notification, attemptNo int) Result {
	ep, ok := e.endpoints[n.Endpoint]
	if !ok {
		// endpoint 曾经存在、现在被从配置里删掉了。
		// 判永久失败进死信，而不是无限重试或静默丢弃——这需要人来决定怎么办。
		return Result{
			Outcome: model.OutcomePermanent,
			Err:     fmt.Errorf("%w: endpoint %q is not configured", ErrPermanentRender, n.Endpoint),
		}
	}

	body, err := renderBody(e.templates[n.Endpoint], n.Payload)
	if err != nil {
		return Result{Outcome: model.OutcomePermanent, Err: err, TargetURL: ep.URL}
	}
	header := renderHeaders(ep, n, attemptNo, len(body))

	reqCtx, cancel := context.WithTimeout(ctx, ep.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, ep.Method, ep.URL, bytes.NewReader(body))
	if err != nil {
		return Result{Outcome: model.OutcomePermanent, Err: fmt.Errorf("%w: build request: %v", ErrPermanentRender, err), TargetURL: ep.URL}
	}
	req.Header = header
	// 显式设置 ContentLength，让下游能看到完整长度而不是 chunked 编码。
	req.ContentLength = int64(len(body))

	start := time.Now()
	resp, err := e.client.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		return Result{
			Outcome:   classifyTransportError(err),
			Err:       err,
			Duration:  elapsed,
			TargetURL: ep.URL,
			Transport: true,
		}
	}
	defer resp.Body.Close()

	snippet := readSnippet(resp.Body)

	res := Result{
		StatusCode:   resp.StatusCode,
		Duration:     elapsed,
		ResponseBody: snippet,
		TargetURL:    ep.URL,
		Outcome:      classifyStatus(ep, resp.StatusCode),
	}
	if res.Outcome != model.OutcomeSuccess {
		res.Err = fmt.Errorf("target returned HTTP %d", resp.StatusCode)
	}
	if res.Outcome == model.OutcomeRetryable {
		res.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	}
	return res
}

// readSnippet 读取响应体开头一小段，并把剩余内容读完丢弃。
//
// 必须把 body 读到 EOF（而不是直接 Close），否则底层连接无法复用，
// 高频投递场景下会退化成每次请求都新建 TCP+TLS 连接。
func readSnippet(r io.Reader) string {
	buf := make([]byte, maxResponseSnippet)
	n, _ := io.ReadFull(io.LimitReader(r, maxResponseSnippet), buf)
	// 限量丢弃剩余内容：对方返回几个 GB 时不该由我们来陪葬。
	io.Copy(io.Discard, io.LimitReader(r, 1<<20))
	return strings.ToValidUTF8(string(buf[:n]), "")
}

// classifyStatus 把 HTTP 状态码翻译成投递判定。
//
// 这是整个系统最重要的一个判断。规则：
//   - 成功：配置的 success_status_codes，未配置则所有 2xx。
//   - 可重试：所有 5xx（下游自己说它坏了）+ 配置的 retry_status_codes（默认 408/423/425/429）。
//   - 其余一律永久失败：400/401/403/404/409/422 这些说的是"你的请求有问题"，
//     重试到天亮也是同样的结果，只会推迟人工介入的时间点。
//   - 3xx 也判永久失败：通知投递不应该跟随重定向到一个配置之外的地址。
func classifyStatus(ep config.Endpoint, code int) model.Outcome {
	if len(ep.SuccessStatusCodes) > 0 {
		if containsInt(ep.SuccessStatusCodes, code) {
			return model.OutcomeSuccess
		}
	} else if code >= 200 && code < 300 {
		return model.OutcomeSuccess
	}
	if containsInt(ep.RetryStatusCodes, code) {
		return model.OutcomeRetryable
	}
	if code >= 500 {
		return model.OutcomeRetryable
	}
	return model.OutcomePermanent
}

// classifyTransportError 判定传输层错误。
//
// 绝大多数传输层错误（连不上、超时、连接被重置、DNS 抖动）都是暂时的，判可重试。
// 唯一的例外是本进程主动取消（优雅关闭）——它同样要重试，
// 但不该被熔断器记成"下游有问题"，所以在 dispatcher 里单独识别。
func classifyTransportError(err error) model.Outcome {
	if errors.Is(err, ErrPermanentRender) {
		return model.OutcomePermanent
	}
	// 目标地址本身非法（unknown scheme、非法 host 等）在配置校验阶段已被拦住，
	// 能走到这里的传输错误都按可重试处理。
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		// 域名不存在通常是配置写错了，但也可能是 DNS 暂时污染。
		// 判可重试 + 靠熔断兜住流量，比直接扔进死信更稳妥。
		return model.OutcomeRetryable
	}
	return model.OutcomeRetryable
}

// IsShutdownCancel 报告错误是否由本进程主动取消引起（优雅关闭），
// 这类失败不应该计入熔断器的失败计数。
func IsShutdownCancel(err error) bool {
	return errors.Is(err, context.Canceled)
}

// parseRetryAfter 解析 Retry-After 头，支持「秒数」和「HTTP 日期」两种格式。
//
// 尊重下游明确给出的等待时间，是被限流时唯一正确的做法：
// 对方已经告诉你什么时候可以再来，还按自己的退避曲线猛敲就是在制造故障。
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

func containsInt(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
