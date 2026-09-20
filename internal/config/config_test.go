package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimalConfig = `
endpoints:
  - name: crm
    url: https://crm.example.com/notify
`

func TestLoadAppliesBuiltinDefaults(t *testing.T) {
	c, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if c.Server.Addr != ":8080" {
		t.Errorf("server.addr = %q, want :8080", c.Server.Addr)
	}
	if c.Store.Synchronous != "FULL" {
		t.Errorf("store.synchronous = %q, want FULL", c.Store.Synchronous)
	}

	ep := c.Endpoints[0]
	if ep.Method != "POST" {
		t.Errorf("method = %q, want POST", ep.Method)
	}
	if ep.Timeout != 5*time.Second {
		t.Errorf("timeout = %s, want 5s", ep.Timeout)
	}
	if ep.MaxAttempts != 8 {
		t.Errorf("max_attempts = %d, want 8", ep.MaxAttempts)
	}
	if !ep.Breaker.Enabled {
		t.Error("breaker should be enabled by default")
	}
	// 退避默认必须带抖动，否则重试惊群会在第一次大规模故障时就暴露出来。
	if ep.Backoff.Jitter <= 0 {
		t.Errorf("backoff.jitter = %v, want a non-zero default", ep.Backoff.Jitter)
	}
	if len(ep.RetryStatusCodes) == 0 {
		t.Error("retry_status_codes should have a default")
	}
}

// defaults 段是为了避免在每个 endpoint 上重复抄一遍策略，
// 同时 endpoint 上显式写的值必须能覆盖它。
func TestEndpointInheritsDefaultsAndCanOverrideThem(t *testing.T) {
	c, err := Load(writeConfig(t, `
defaults:
  timeout: 3s
  max_attempts: 4
  concurrency: 2
  headers:
    X-Common: shared
  backoff:
    base: 5s
    max: 1m
    jitter: 0.1
endpoints:
  - name: inherits
    url: https://a.example.com/x
  - name: overrides
    url: https://b.example.com/x
    timeout: 30s
    max_attempts: 9
    headers:
      X-Special: mine
`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	m := c.EndpointMap()

	inherits := m["inherits"]
	if inherits.Timeout != 3*time.Second || inherits.MaxAttempts != 4 || inherits.Concurrency != 2 {
		t.Errorf("inherited values wrong: %+v", inherits)
	}
	if inherits.Backoff.Base != 5*time.Second {
		t.Errorf("backoff.base = %s, want 5s from defaults", inherits.Backoff.Base)
	}
	if inherits.Headers["X-Common"] != "shared" {
		t.Errorf("default headers not inherited: %v", inherits.Headers)
	}

	over := m["overrides"]
	if over.Timeout != 30*time.Second || over.MaxAttempts != 9 {
		t.Errorf("overrides did not take effect: %+v", over)
	}
	// header 是合并语义而不是整体替换：endpoint 自己加的头不该
	// 把公共头（比如统一的 User-Agent、追踪头）意外清掉。
	if over.Headers["X-Special"] != "mine" || over.Headers["X-Common"] != "shared" {
		t.Errorf("headers should merge, got %v", over.Headers)
	}
}

// 配置里的拼写错误必须让启动失败。静默忽略未知字段，
// 会让"我明明配了重试次数"这种问题变成一场漫长的排查。
func TestLoadRejectsUnknownFields(t *testing.T) {
	_, err := Load(writeConfig(t, `
endpoints:
  - name: crm
    url: https://crm.example.com/notify
    max_attemptz: 5
`))
	if err == nil {
		t.Fatal("expected a typo in a config field to fail the load")
	}
	if !strings.Contains(err.Error(), "max_attemptz") {
		t.Errorf("error should name the offending field, got: %v", err)
	}
}

func TestLoadRequiresAtLeastOneEndpoint(t *testing.T) {
	if _, err := Load(writeConfig(t, "endpoints: []\n")); err == nil {
		t.Fatal("expected an empty endpoint list to fail")
	}
}

func TestLoadRejectsDuplicateEndpointNames(t *testing.T) {
	_, err := Load(writeConfig(t, `
endpoints:
  - name: crm
    url: https://a.example.com/x
  - name: crm
    url: https://b.example.com/x
`))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("err = %v, want a duplicate-name error", err)
	}
}

func TestLoadValidatesURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"缺少 scheme", "crm.example.com/notify"},
		{"不支持的 scheme", "ftp://crm.example.com/notify"},
		{"file scheme 会变成本地文件读取漏洞", "file:///etc/passwd"},
		{"缺少 host", "https:///notify"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, "endpoints:\n  - name: x\n    url: \""+tc.url+"\"\n"))
			if err == nil {
				t.Errorf("url %q should have been rejected", tc.url)
			}
		})
	}
}

// lease 比请求超时还短，会导致"请求还在飞、任务已被回收重投"的重复投递风暴。
// 这是一个很容易配错、且线上症状非常迷惑的组合，所以必须在启动时拦住。
func TestLoadRejectsTimeoutLongerThanLease(t *testing.T) {
	_, err := Load(writeConfig(t, `
dispatcher:
  lease_duration: 5s
endpoints:
  - name: slow
    url: https://slow.example.com/x
    timeout: 30s
`))
	if err == nil {
		t.Fatal("expected timeout >= lease_duration to be rejected")
	}
	if !strings.Contains(err.Error(), "lease_duration") {
		t.Errorf("error should explain the lease relationship, got: %v", err)
	}
}

// 凭据通过环境变量注入，不进代码库。
func TestEnvExpansion(t *testing.T) {
	t.Setenv("TEST_CRM_TOKEN", "s3cr3t")
	t.Setenv("TEST_HOST", "crm.internal")

	c, err := Load(writeConfig(t, `
server:
  admin_token: "${env:TEST_CRM_TOKEN}"
endpoints:
  - name: crm
    url: "https://${env:TEST_HOST}/notify"
    headers:
      Authorization: "Bearer ${env:TEST_CRM_TOKEN}"
`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ep := c.Endpoints[0]
	if ep.URL != "https://crm.internal/notify" {
		t.Errorf("url = %q, want the expanded host", ep.URL)
	}
	if ep.Headers["Authorization"] != "Bearer s3cr3t" {
		t.Errorf("header = %q, want the expanded token", ep.Headers["Authorization"])
	}
	if c.Server.AdminToken != "s3cr3t" {
		t.Errorf("admin_token = %q, want the expanded value", c.Server.AdminToken)
	}
}

// 引用了不存在的环境变量时必须启动失败，而不是带着空 token 上线——
// 后者会让服务"看起来正常启动了"，然后对每一条通知都返回 401。
func TestMissingEnvVarFailsLoudly(t *testing.T) {
	_, err := Load(writeConfig(t, `
endpoints:
  - name: crm
    url: https://crm.example.com/notify
    headers:
      Authorization: "Bearer ${env:DEFINITELY_NOT_SET_12345}"
`))
	if !errors.Is(err, ErrMissingEnv) {
		t.Fatalf("err = %v, want ErrMissingEnv", err)
	}
	if !strings.Contains(err.Error(), "DEFINITELY_NOT_SET_12345") {
		t.Errorf("error should name the missing variable, got: %v", err)
	}
}

func TestBackoffValidation(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "jitter 超出 [0,1]",
			yaml: "endpoints:\n  - name: x\n    url: https://a.example.com/x\n    backoff:\n      jitter: 2.5\n",
		},
		{
			name: "max 小于 base",
			yaml: "endpoints:\n  - name: x\n    url: https://a.example.com/x\n    backoff:\n      base: 10s\n      max: 1s\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, tc.yaml)); err == nil {
				t.Error("expected validation to fail")
			}
		})
	}
}

func TestLoadRejectsUnsupportedMethod(t *testing.T) {
	_, err := Load(writeConfig(t, `
endpoints:
  - name: x
    url: https://a.example.com/x
    method: TRACE
`))
	if err == nil {
		t.Fatal("expected an unsupported method to be rejected")
	}
}

// 仓库里附带的两份配置必须始终是有效的，否则 README 里的命令一跑就失败。
func TestShippedConfigsAreValid(t *testing.T) {
	if _, err := Load("../../config.yaml"); err != nil {
		t.Errorf("config.yaml is invalid: %v", err)
	}

	for _, k := range []string{
		"NOTIFYD_ADMIN_TOKEN", "ADNETWORK_CLIENT_ID", "ADNETWORK_CLIENT_SECRET",
		"CRM_API_TOKEN", "ERP_API_KEY",
	} {
		t.Setenv(k, "placeholder")
	}
	if _, err := Load("../../config.example.yaml"); err != nil {
		t.Errorf("config.example.yaml is invalid: %v", err)
	}
}
