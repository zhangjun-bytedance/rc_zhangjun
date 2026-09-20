package metrics

import (
	"strings"
	"testing"
)

func render(t *testing.T, m *Metrics) string {
	t.Helper()
	var sb strings.Builder
	if err := m.WritePrometheus(&sb); err != nil {
		t.Fatalf("write: %v", err)
	}
	return sb.String()
}

func TestCounterOutput(t *testing.T) {
	m := New()
	m.Inc(MetricIngressTotal, Label{"endpoint", "crm"}, Label{"result", "accepted"})
	m.Inc(MetricIngressTotal, Label{"endpoint", "crm"}, Label{"result", "accepted"})
	m.Inc(MetricIngressTotal, Label{"endpoint", "ad"}, Label{"result", "rejected"})

	out := render(t, m)
	for _, want := range []string{
		"# TYPE notify_ingress_total counter",
		`notify_ingress_total{endpoint="crm",result="accepted"} 2`,
		`notify_ingress_total{endpoint="ad",result="rejected"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// 标签顺序必须与传入顺序无关，否则同一个逻辑序列会被拆成两条时间序列。
func TestLabelOrderIsNormalized(t *testing.T) {
	m := New()
	m.Inc(MetricIngressTotal, Label{"result", "accepted"}, Label{"endpoint", "crm"})
	m.Inc(MetricIngressTotal, Label{"endpoint", "crm"}, Label{"result", "accepted"})

	out := render(t, m)
	if !strings.Contains(out, `notify_ingress_total{endpoint="crm",result="accepted"} 2`) {
		t.Errorf("labels were not normalized into a single series:\n%s", out)
	}
}

// 未转义的引号或换行会产出无法被 Prometheus 解析的输出，
// 而错误信息里出现引号是很常见的。
func TestLabelValuesAreEscaped(t *testing.T) {
	m := New()
	m.Inc(MetricTerminalTotal, Label{"endpoint", `we"ird` + "\n" + `back\slash`})

	out := render(t, m)
	if !strings.Contains(out, `we\"ird\nback\\slash`) {
		t.Errorf("label value was not escaped:\n%s", out)
	}
}

func TestHistogramOutputIsCumulative(t *testing.T) {
	m := New()
	for _, v := range []float64{0.004, 0.02, 0.2, 2, 100} {
		m.Observe(MetricDeliveryDuration, v, Label{"endpoint", "crm"})
	}

	out := render(t, m)
	for _, want := range []string{
		"# TYPE notify_delivery_duration_seconds histogram",
		// 0.004 落在第一个桶里
		`notify_delivery_duration_seconds_bucket{endpoint="crm",le="0.005"} 1`,
		// 0.004 + 0.02 都 <= 0.025
		`notify_delivery_duration_seconds_bucket{endpoint="crm",le="0.025"} 2`,
		// 100 超过最大桶，只出现在 +Inf 里
		`notify_delivery_duration_seconds_bucket{endpoint="crm",le="30"} 4`,
		`notify_delivery_duration_seconds_bucket{endpoint="crm",le="+Inf"} 5`,
		`notify_delivery_duration_seconds_count{endpoint="crm"} 5`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestGaugeFuncIsEvaluatedAtScrapeTime(t *testing.T) {
	m := New()
	value := 1.0
	m.SetGaugeFunc(MetricQueueDepth, func() []GaugeSample {
		return []GaugeSample{{Labels: []Label{{"status", "pending"}}, Value: value}}
	})

	if !strings.Contains(render(t, m), `notify_queue_depth{status="pending"} 1`) {
		t.Error("gauge was not rendered")
	}
	// 改变底层真值后，下一次抓取必须反映新值（说明它不是抓取时缓存的快照）。
	value = 42
	if !strings.Contains(render(t, m), `notify_queue_depth{status="pending"} 42`) {
		t.Error("gauge was not re-evaluated on the second scrape")
	}
}

func TestOutputIsDeterministic(t *testing.T) {
	m := New()
	for _, ep := range []string{"zeta", "alpha", "mid"} {
		m.Inc(MetricTerminalTotal, Label{"endpoint", ep}, Label{"state", "dead"})
	}
	// 不稳定的输出顺序会让 diff 和快照测试全都变成噪音。
	if render(t, m) != render(t, m) {
		t.Error("two consecutive scrapes produced different output")
	}
}

func TestConcurrentUpdatesAreSafe(t *testing.T) {
	m := New()
	const workers, perWorker = 8, 200

	done := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < perWorker; i++ {
				m.Inc(MetricAttemptsTotal, Label{"endpoint", "crm"}, Label{"outcome", "success"})
				m.Observe(MetricDeliveryDuration, 0.01, Label{"endpoint", "crm"})
			}
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	got := m.CounterValue(MetricAttemptsTotal,
		Label{"endpoint", "crm"}, Label{"outcome", "success"})
	if want := float64(workers * perWorker); got != want {
		t.Errorf("counter = %v, want %v (lost updates)", got, want)
	}
}

func TestEmptyRegistryRendersNothing(t *testing.T) {
	if out := render(t, New()); out != "" {
		t.Errorf("empty registry rendered %q, want empty", out)
	}
}
