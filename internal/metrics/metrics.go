// Package metrics 提供 Prometheus 文本格式的指标采集。
//
// 这里手写了一个极小的指标注册表，而不是引入 prometheus/client_golang。
// 原因很实际：本系统需要的全部指标就是几个计数器、一个直方图和两个 gauge，
// 手写实现约 200 行且零依赖；而 client_golang 会带进十几个传递依赖。
// 依赖不是免费的——它们会出现在每一次安全扫描、每一次版本升级里。
//
// 这个取舍有明确的失效条件：一旦需要 exemplar、原生直方图、
// 或者要接入 pushgateway，就应该换成官方库而不是继续加功能。
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Label 是一个指标标签。用切片而不是 map 传递，保证渲染顺序稳定。
type Label struct {
	Name  string
	Value string
}

// GaugeSample 是一个 gauge 的采样点。
type GaugeSample struct {
	Labels []Label
	Value  float64
}

// defaultBuckets 是投递耗时的直方图分桶（秒）。
//
// 桶边界按"外部 HTTP 调用"这个场景挑的：下界 5ms 够看清同机房的快响应，
// 上界 30s 覆盖到超时；中间在 100ms~2s 加密，因为 SLO 的讨论基本都发生在这一段。
var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

const (
	// MetricIngressTotal 入口收到的通知数，按 endpoint 和处理结果分。
	MetricIngressTotal = "notify_ingress_total"
	// MetricAttemptsTotal 投递尝试数，按 endpoint 和判定结果分。
	MetricAttemptsTotal = "notify_delivery_attempts_total"
	// MetricTerminalTotal 进入终态的通知数，按 endpoint 和终态分。
	MetricTerminalTotal = "notify_terminal_total"
	// MetricLeaseRecoveredTotal 因 lease 过期被回收的任务数。
	// 这个指标持续非零意味着有实例在异常退出，或者 lease 配得太短。
	MetricLeaseRecoveredTotal = "notify_lease_recovered_total"
	// MetricBreakerTripsTotal 熔断打开次数。
	MetricBreakerTripsTotal = "notify_breaker_trips_total"
	// MetricDeliveryDuration 投递耗时直方图（秒）。
	MetricDeliveryDuration = "notify_delivery_duration_seconds"
	// MetricQueueDepth 各状态的任务数。
	MetricQueueDepth = "notify_queue_depth"
	// MetricBreakerState 熔断器状态（1 表示处于该状态）。
	MetricBreakerState = "notify_breaker_state"
	// MetricInFlight 当前正在投递的请求数。
	MetricInFlight = "notify_in_flight"
)

type metricMeta struct {
	kind string // counter | gauge | histogram
	help string
}

var registry = map[string]metricMeta{
	MetricIngressTotal:        {"counter", "Notifications accepted at the ingress API, by endpoint and result."},
	MetricAttemptsTotal:       {"counter", "Delivery attempts made to target systems, by endpoint and outcome."},
	MetricTerminalTotal:       {"counter", "Notifications that reached a terminal state, by endpoint and state."},
	MetricLeaseRecoveredTotal: {"counter", "In-flight notifications reclaimed after their lease expired."},
	MetricBreakerTripsTotal:   {"counter", "Number of times a circuit breaker opened, by endpoint."},
	MetricDeliveryDuration:    {"histogram", "Wall-clock duration of delivery attempts in seconds, by endpoint."},
	MetricQueueDepth:          {"gauge", "Number of notifications per status."},
	MetricBreakerState:        {"gauge", "Circuit breaker state per endpoint (1 = active)."},
	MetricInFlight:            {"gauge", "Delivery attempts currently in flight, by endpoint."},
}

type histogram struct {
	counts []uint64
	sum    float64
	total  uint64
}

// Metrics 是指标注册表，并发安全。
type Metrics struct {
	mu         sync.Mutex
	counters   map[string]map[string]float64
	histograms map[string]map[string]*histogram
	gaugeFuncs map[string]func() []GaugeSample
}

// New 创建指标注册表。
func New() *Metrics {
	return &Metrics{
		counters:   make(map[string]map[string]float64),
		histograms: make(map[string]map[string]*histogram),
		gaugeFuncs: make(map[string]func() []GaugeSample),
	}
}

// Inc 计数器加一。
func (m *Metrics) Inc(name string, labels ...Label) { m.Add(name, 1, labels...) }

// Add 计数器累加。
func (m *Metrics) Add(name string, v float64, labels ...Label) {
	key := renderLabels(labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	series, ok := m.counters[name]
	if !ok {
		series = make(map[string]float64)
		m.counters[name] = series
	}
	series[key] += v
}

// Observe 向直方图写入一个观测值。
func (m *Metrics) Observe(name string, v float64, labels ...Label) {
	key := renderLabels(labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	series, ok := m.histograms[name]
	if !ok {
		series = make(map[string]*histogram)
		m.histograms[name] = series
	}
	h, ok := series[key]
	if !ok {
		h = &histogram{counts: make([]uint64, len(defaultBuckets))}
		series[key] = h
	}
	h.sum += v
	h.total++
	for i, b := range defaultBuckets {
		if v <= b {
			h.counts[i]++
		}
	}
}

// SetGaugeFunc 注册一个在抓取时才求值的 gauge。
//
// 用回调而不是让业务代码主动 Set：队列深度这类指标的真值在数据库里，
// 抓取时查一次远比在每个状态变更点维护一个内存计数器更不容易错。
func (m *Metrics) SetGaugeFunc(name string, fn func() []GaugeSample) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gaugeFuncs[name] = fn
}

// CounterValue 返回某个计数器序列的当前值，仅供测试使用。
func (m *Metrics) CounterValue(name string, labels ...Label) float64 {
	key := renderLabels(labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name][key]
}

// WritePrometheus 以 Prometheus 文本格式输出全部指标。
func (m *Metrics) WritePrometheus(w io.Writer) error {
	m.mu.Lock()
	counters := make(map[string]map[string]float64, len(m.counters))
	for name, series := range m.counters {
		cp := make(map[string]float64, len(series))
		for k, v := range series {
			cp[k] = v
		}
		counters[name] = cp
	}
	hists := make(map[string]map[string]histogram, len(m.histograms))
	for name, series := range m.histograms {
		cp := make(map[string]histogram, len(series))
		for k, h := range series {
			counts := make([]uint64, len(h.counts))
			copy(counts, h.counts)
			cp[k] = histogram{counts: counts, sum: h.sum, total: h.total}
		}
		hists[name] = cp
	}
	gaugeFuncs := make(map[string]func() []GaugeSample, len(m.gaugeFuncs))
	for name, fn := range m.gaugeFuncs {
		gaugeFuncs[name] = fn
	}
	m.mu.Unlock()

	var buf strings.Builder
	for _, name := range sortedKeys(counters) {
		writeHeader(&buf, name)
		for _, labels := range sortedKeys(counters[name]) {
			fmt.Fprintf(&buf, "%s%s %s\n", name, labels, formatFloat(counters[name][labels]))
		}
	}
	for _, name := range sortedKeys(gaugeFuncs) {
		samples := gaugeFuncs[name]()
		if len(samples) == 0 {
			continue
		}
		writeHeader(&buf, name)
		lines := make([]string, 0, len(samples))
		for _, s := range samples {
			lines = append(lines, fmt.Sprintf("%s%s %s", name, renderLabels(s.Labels), formatFloat(s.Value)))
		}
		sort.Strings(lines)
		buf.WriteString(strings.Join(lines, "\n"))
		buf.WriteByte('\n')
	}
	for _, name := range sortedKeys(hists) {
		writeHeader(&buf, name)
		for _, labels := range sortedKeys(hists[name]) {
			h := hists[name][labels]
			// counts[i] 在 Observe 时就是按"≤ 该桶上界"累加的，本身即累积计数，
			// 正好符合 Prometheus 直方图要求的累积语义。
			for i, b := range defaultBuckets {
				fmt.Fprintf(&buf, "%s_bucket%s %d\n", name,
					mergeLabels(labels, Label{"le", strconv.FormatFloat(b, 'g', -1, 64)}), h.counts[i])
			}
			fmt.Fprintf(&buf, "%s_bucket%s %d\n", name, mergeLabels(labels, Label{"le", "+Inf"}), h.total)
			fmt.Fprintf(&buf, "%s_sum%s %s\n", name, labels, formatFloat(h.sum))
			fmt.Fprintf(&buf, "%s_count%s %d\n", name, labels, h.total)
		}
	}
	_, err := io.WriteString(w, buf.String())
	return err
}

func writeHeader(buf *strings.Builder, name string) {
	meta, ok := registry[name]
	if !ok {
		meta = metricMeta{kind: "untyped"}
	}
	if meta.help != "" {
		fmt.Fprintf(buf, "# HELP %s %s\n", name, meta.help)
	}
	fmt.Fprintf(buf, "# TYPE %s %s\n", name, meta.kind)
}

// renderLabels 把标签渲染成 {k="v",...}，按名字排序保证输出稳定。
func renderLabels(labels []Label) string {
	if len(labels) == 0 {
		return ""
	}
	sorted := make([]Label, len(labels))
	copy(sorted, labels)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	var b strings.Builder
	b.WriteByte('{')
	for i, l := range sorted {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(l.Value))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// mergeLabels 往已渲染好的标签串里再插一个标签（用于直方图的 le）。
func mergeLabels(rendered string, extra Label) string {
	tail := extra.Name + `="` + escapeLabelValue(extra.Value) + `"`
	if rendered == "" {
		return "{" + tail + "}"
	}
	return rendered[:len(rendered)-1] + "," + tail + "}"
}

// escapeLabelValue 转义 Prometheus 文本格式要求转义的字符。
// endpoint 名来自配置、outcome 来自内部枚举，但 error 类标签可能含奇怪字符，
// 不转义会直接产出无法解析的指标输出。
func escapeLabelValue(v string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return replacer.Replace(v)
}

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
