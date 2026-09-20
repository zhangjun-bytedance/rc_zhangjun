// Package config 负责加载与校验服务配置。
//
// 核心设计：endpoint 采用「注册制」——业务系统提交通知时只给一个逻辑名
// （如 "crm"），真实 URL、认证凭据、超时和重试策略都由本文件集中定义。
// 调用方不能传 URL。理由见 README，简要说三点：
//  1. 防 SSRF：允许调用方指定任意 URL，就等于在内网里开了一个探测代理。
//  2. 凭据集中：供应商 token 不再散落到每个业务系统的配置里。
//  3. 变更收敛：供应商换域名/换认证方式时只改这里，业务方不需要发版。
//
// 代价是接入新供应商需要改配置并重启，这个代价在第一版是可以接受的
// （接入频率远低于通知量），演进方向见 README。
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是服务的完整配置。
type Config struct {
	Server     Server     `yaml:"server"`
	Store      Store      `yaml:"store"`
	Dispatcher Dispatcher `yaml:"dispatcher"`
	// Defaults 是所有 endpoint 的默认策略，endpoint 上显式写了的字段会覆盖它。
	Defaults  Endpoint   `yaml:"defaults"`
	Endpoints []Endpoint `yaml:"endpoints"`
}

// Server 是 HTTP 入口配置。
type Server struct {
	Addr string `yaml:"addr"`
	// MaxBodyBytes 限制单条通知的大小。通知系统不是文件传输通道，
	// 不设上限会让一条超大 payload 拖垮整个队列的内存和磁盘。
	MaxBodyBytes int64         `yaml:"max_body_bytes"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	// ShutdownGrace 是收到 SIGTERM 后等待在途投递完成的时长。
	ShutdownGrace time.Duration `yaml:"shutdown_grace"`
	// AdminToken 保护 admin 接口（重投/取消/列表）。空值表示不鉴权，仅限本地开发。
	AdminToken string `yaml:"admin_token"`
}

// Store 是持久化配置。
type Store struct {
	Path        string        `yaml:"path"`
	Synchronous string        `yaml:"synchronous"`
	BusyTimeout time.Duration `yaml:"busy_timeout"`
}

// Dispatcher 是调度器配置。
type Dispatcher struct {
	// InstanceID 用于标记 lease 持有者。留空则用 hostname-pid。
	InstanceID string `yaml:"instance_id"`
	// PollInterval 是空闲时的轮询间隔。有新任务入队时会被立即唤醒，
	// 所以这个值只决定"重试到期"的时间精度，不决定首次投递延迟。
	PollInterval time.Duration `yaml:"poll_interval"`
	// BatchSize 是单个 endpoint 单次领取的任务数上限。
	BatchSize int `yaml:"batch_size"`
	// LeaseDuration 必须显著大于 endpoint 的 timeout，
	// 否则请求还在飞、lease 已过期，会造成大量无意义的重复投递。
	LeaseDuration time.Duration `yaml:"lease_duration"`
	// ReaperInterval 是 lease 回收扫描间隔。
	ReaperInterval time.Duration `yaml:"reaper_interval"`
}

// Backoff 是指数退避参数。
type Backoff struct {
	Base time.Duration `yaml:"base"`
	Max  time.Duration `yaml:"max"`
	// Jitter 是抖动比例 [0,1]。没有抖动，同一批失败任务会在同一毫秒集体重试，
	// 把下游刚恢复的服务再打挂一次（惊群）。
	Jitter float64 `yaml:"jitter"`
}

// Breaker 是熔断器参数。
type Breaker struct {
	Enabled bool `yaml:"enabled"`
	// FailureThreshold 连续失败多少次后打开熔断。
	FailureThreshold int `yaml:"failure_threshold"`
	// Cooldown 熔断打开后暂停投递的时长。
	Cooldown time.Duration `yaml:"cooldown"`
	// HalfOpenProbes 半开状态下允许的并发探测请求数。
	HalfOpenProbes int `yaml:"half_open_probes"`
}

// Endpoint 是一个外部供应商的投递目标定义。
type Endpoint struct {
	Name   string `yaml:"name"`
	URL    string `yaml:"url"`
	Method string `yaml:"method"`
	// Headers 是固定注入的请求头，值支持 ${env:VAR} 引用环境变量，
	// 这样凭据不进代码库、不进配置文件。
	Headers map[string]string `yaml:"headers"`
	// ContentType 覆盖 Content-Type，默认 application/json。
	ContentType string `yaml:"content_type"`
	// BodyTemplate 是可选的 Go text/template，输入是解析后的 payload。
	// 留空表示原样透传 payload。
	//
	// 这里刻意只提供 text/template 而不是 JSONata/Jolt 这类转换 DSL：
	// payload 的结构映射属于业务语义，主战场应该在业务系统里，
	// 而不是让通知服务变成一个要跟着每个供应商迭代的胶水单点。
	// 这个字段是给"只差一层壳"场景的逃生舱，不是通用转换引擎。
	BodyTemplate string `yaml:"body_template"`

	Timeout     time.Duration `yaml:"timeout"`
	MaxAttempts int           `yaml:"max_attempts"`
	// Concurrency 是该 endpoint 的并发投递上限（单实例）。
	// 这是本系统唯一的过载保护手段，见 README 为什么不做全局 QPS 限流。
	Concurrency int `yaml:"concurrency"`

	Backoff Backoff `yaml:"backoff"`
	Breaker Breaker `yaml:"breaker"`

	// SuccessStatusCodes 判定成功的状态码；留空表示所有 2xx。
	SuccessStatusCodes []int `yaml:"success_status_codes"`
	// RetryStatusCodes 判定为「可重试」的状态码。
	// 未列出的非成功状态码一律视为永久失败，直接进死信。
	RetryStatusCodes []int `yaml:"retry_status_codes"`
	// AllowCallerHeaders 允许调用方透传的 header 名（大小写不敏感）。
	// 留空表示只允许 X- 前缀的自定义头。
	AllowCallerHeaders []string `yaml:"allow_caller_headers"`
}

// DefaultRetryStatusCodes 是默认认定"值得重试"的状态码。
//
// 这个清单是本系统最有观点的地方之一：除了它和全部 5xx，
// 其余 4xx（400/401/403/404/422...）一律判永久失败，直接进死信。
// 401/403 重试十次也不会突然有权限；404 重试十次地址也不会突然存在。
// 与其烧掉重试预算再在几小时后进死信，不如立刻可见、立刻告警、立刻人工介入。
var DefaultRetryStatusCodes = []int{
	408, // Request Timeout
	423, // Locked
	425, // Too Early
	429, // Too Many Requests
}

// Load 读取并校验配置文件。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true) // 配置里的拼写错误必须报错，不能静默忽略
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := c.normalize(); err != nil {
		return nil, err
	}
	return &c, nil
}

// envRef 匹配 ${env:VAR_NAME}。
var envRef = regexp.MustCompile(`\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}`)

// ErrMissingEnv 表示配置引用了未设置的环境变量。
var ErrMissingEnv = errors.New("config: referenced environment variable is not set")

func expandEnv(s string) (string, error) {
	var missing []string
	out := envRef.ReplaceAllStringFunc(s, func(m string) string {
		name := envRef.FindStringSubmatch(m)[1]
		v, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
			return ""
		}
		return v
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("%w: %s", ErrMissingEnv, strings.Join(missing, ", "))
	}
	return out, nil
}

func (c *Config) normalize() error {
	// ---- server ----
	if c.Server.Addr == "" {
		c.Server.Addr = ":8080"
	}
	if c.Server.MaxBodyBytes <= 0 {
		c.Server.MaxBodyBytes = 1 << 20 // 1 MiB
	}
	if c.Server.ReadTimeout <= 0 {
		c.Server.ReadTimeout = 10 * time.Second
	}
	if c.Server.WriteTimeout <= 0 {
		c.Server.WriteTimeout = 10 * time.Second
	}
	if c.Server.ShutdownGrace <= 0 {
		c.Server.ShutdownGrace = 20 * time.Second
	}
	token, err := expandEnv(c.Server.AdminToken)
	if err != nil {
		return err
	}
	c.Server.AdminToken = token

	// ---- store ----
	if c.Store.Path == "" {
		c.Store.Path = "data/notify.db"
	}
	if c.Store.Synchronous == "" {
		c.Store.Synchronous = "FULL"
	}
	if c.Store.BusyTimeout <= 0 {
		c.Store.BusyTimeout = 5 * time.Second
	}

	// ---- dispatcher ----
	d := &c.Dispatcher
	if d.PollInterval <= 0 {
		d.PollInterval = 250 * time.Millisecond
	}
	if d.BatchSize <= 0 {
		d.BatchSize = 20
	}
	if d.LeaseDuration <= 0 {
		d.LeaseDuration = 60 * time.Second
	}
	if d.ReaperInterval <= 0 {
		d.ReaperInterval = 15 * time.Second
	}

	// ---- endpoint 默认值 ----
	applyEndpointFallback(&c.Defaults, builtinDefaults())

	if len(c.Endpoints) == 0 {
		return errors.New("config: at least one endpoint must be configured")
	}
	seen := make(map[string]bool, len(c.Endpoints))
	for i := range c.Endpoints {
		ep := &c.Endpoints[i]
		applyEndpointFallback(ep, c.Defaults)
		if err := ep.validate(); err != nil {
			return err
		}
		if seen[ep.Name] {
			return fmt.Errorf("config: duplicate endpoint name %q", ep.Name)
		}
		seen[ep.Name] = true

		if ep.Timeout >= d.LeaseDuration {
			return fmt.Errorf("config: endpoint %q timeout (%s) must be shorter than dispatcher.lease_duration (%s), "+
				"otherwise leases expire while requests are still in flight and cause duplicate deliveries",
				ep.Name, ep.Timeout, d.LeaseDuration)
		}
	}
	return nil
}

func builtinDefaults() Endpoint {
	return Endpoint{
		Method:      "POST",
		ContentType: "application/json",
		Timeout:     5 * time.Second,
		MaxAttempts: 8,
		Concurrency: 8,
		Backoff: Backoff{
			Base:   time.Second,
			Max:    10 * time.Minute,
			Jitter: 0.3,
		},
		Breaker: Breaker{
			Enabled:          true,
			FailureThreshold: 5,
			Cooldown:         30 * time.Second,
			HalfOpenProbes:   1,
		},
		RetryStatusCodes: DefaultRetryStatusCodes,
	}
}

// applyEndpointFallback 把 src 中的值填进 dst 的零值字段（浅层字段级继承）。
//
// 用显式的字段级 fallback 而不是反射：字段数量有限，显式代码更容易在
// review 时看出"哪些字段可继承"，也不会因为 YAML 里写了 0 值而产生歧义。
func applyEndpointFallback(dst *Endpoint, src Endpoint) {
	if dst.Method == "" {
		dst.Method = src.Method
	}
	if dst.ContentType == "" {
		dst.ContentType = src.ContentType
	}
	if dst.Timeout <= 0 {
		dst.Timeout = src.Timeout
	}
	if dst.MaxAttempts <= 0 {
		dst.MaxAttempts = src.MaxAttempts
	}
	if dst.Concurrency <= 0 {
		dst.Concurrency = src.Concurrency
	}
	if dst.Backoff.Base <= 0 {
		dst.Backoff.Base = src.Backoff.Base
	}
	if dst.Backoff.Max <= 0 {
		dst.Backoff.Max = src.Backoff.Max
	}
	if dst.Backoff.Jitter <= 0 {
		dst.Backoff.Jitter = src.Backoff.Jitter
	}
	if dst.Breaker.FailureThreshold <= 0 {
		dst.Breaker.FailureThreshold = src.Breaker.FailureThreshold
		dst.Breaker.Enabled = src.Breaker.Enabled
	}
	if dst.Breaker.Cooldown <= 0 {
		dst.Breaker.Cooldown = src.Breaker.Cooldown
	}
	if dst.Breaker.HalfOpenProbes <= 0 {
		dst.Breaker.HalfOpenProbes = src.Breaker.HalfOpenProbes
	}
	if len(dst.RetryStatusCodes) == 0 {
		dst.RetryStatusCodes = src.RetryStatusCodes
	}
	if len(dst.SuccessStatusCodes) == 0 {
		dst.SuccessStatusCodes = src.SuccessStatusCodes
	}
	if len(dst.AllowCallerHeaders) == 0 {
		dst.AllowCallerHeaders = src.AllowCallerHeaders
	}
	if dst.Headers == nil && src.Headers != nil {
		dst.Headers = make(map[string]string, len(src.Headers))
	}
	for k, v := range src.Headers {
		if _, ok := dst.Headers[k]; !ok {
			dst.Headers[k] = v
		}
	}
}

func (e *Endpoint) validate() error {
	if e.Name == "" {
		return errors.New("config: endpoint name is required")
	}
	if e.URL == "" {
		return fmt.Errorf("config: endpoint %q: url is required", e.Name)
	}
	expanded, err := expandEnv(e.URL)
	if err != nil {
		return fmt.Errorf("config: endpoint %q url: %w", e.Name, err)
	}
	e.URL = expanded

	u, err := url.Parse(e.URL)
	if err != nil {
		return fmt.Errorf("config: endpoint %q: invalid url: %w", e.Name, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("config: endpoint %q: url scheme must be http or https, got %q", e.Name, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("config: endpoint %q: url must include a host", e.Name)
	}

	e.Method = strings.ToUpper(e.Method)
	switch e.Method {
	case "POST", "PUT", "PATCH", "DELETE", "GET":
	default:
		return fmt.Errorf("config: endpoint %q: unsupported method %q", e.Name, e.Method)
	}

	for k, v := range e.Headers {
		ev, err := expandEnv(v)
		if err != nil {
			return fmt.Errorf("config: endpoint %q header %q: %w", e.Name, k, err)
		}
		e.Headers[k] = ev
	}

	if e.Backoff.Jitter < 0 || e.Backoff.Jitter > 1 {
		return fmt.Errorf("config: endpoint %q: backoff.jitter must be within [0,1]", e.Name)
	}
	if e.Backoff.Max < e.Backoff.Base {
		return fmt.Errorf("config: endpoint %q: backoff.max must be >= backoff.base", e.Name)
	}
	return nil
}

// EndpointMap 把 endpoint 列表转成按名字索引的表，供运行时查找。
func (c *Config) EndpointMap() map[string]Endpoint {
	m := make(map[string]Endpoint, len(c.Endpoints))
	for _, ep := range c.Endpoints {
		m[ep.Name] = ep
	}
	return m
}
