package model

import (
	"errors"
	"testing"
)

// 状态机是本系统正确性的骨架：所有并发写入都要先过这张表。
// 这个用例把允许/禁止的迁移全部列出来当作规格说明。
func TestStateMachineTransitions(t *testing.T) {
	allowed := []struct{ from, to Status }{
		{StatusPending, StatusInFlight},   // 被调度器领取
		{StatusPending, StatusCanceled},   // 人工取消
		{StatusInFlight, StatusSucceeded}, // 投递成功
		{StatusInFlight, StatusPending},   // 可重试失败 / lease 过期回收
		{StatusInFlight, StatusDead},      // 永久失败 / 预算耗尽
		{StatusDead, StatusPending},       // 人工重投
	}
	for _, tc := range allowed {
		if !CanTransition(tc.from, tc.to) {
			t.Errorf("%s -> %s should be allowed", tc.from, tc.to)
		}
		if err := CheckTransition(tc.from, tc.to); err != nil {
			t.Errorf("CheckTransition(%s, %s) = %v, want nil", tc.from, tc.to, err)
		}
	}

	forbidden := []struct {
		from, to Status
		why      string
	}{
		{StatusSucceeded, StatusPending, "重投一条已成功的通知会造成业务可见的重复"},
		{StatusSucceeded, StatusInFlight, "终态不可回退"},
		{StatusSucceeded, StatusDead, "已经成功的不能被改判失败"},
		{StatusCanceled, StatusPending, "取消是用户的明确意图，不能被系统推翻"},
		{StatusCanceled, StatusInFlight, "已取消的不该被投递"},
		{StatusPending, StatusSucceeded, "没投递过就不可能成功"},
		{StatusDead, StatusSucceeded, "必须先回到 pending 重新投递"},
		{StatusDead, StatusInFlight, "重投必须显式经过 pending，便于审计"},
	}
	for _, tc := range forbidden {
		if CanTransition(tc.from, tc.to) {
			t.Errorf("%s -> %s should be forbidden (%s)", tc.from, tc.to, tc.why)
		}
		err := CheckTransition(tc.from, tc.to)
		if !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("CheckTransition(%s, %s) = %v, want ErrInvalidTransition", tc.from, tc.to, err)
		}
	}
}

func TestTerminalStates(t *testing.T) {
	terminal := []Status{StatusSucceeded, StatusDead, StatusCanceled}
	active := []Status{StatusPending, StatusInFlight}

	for _, s := range terminal {
		if !s.Terminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range active {
		if s.Terminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

// 状态值来自外部输入（admin 列表接口的查询参数），必须能校验。
func TestStatusValid(t *testing.T) {
	for _, s := range []Status{
		StatusPending, StatusInFlight, StatusSucceeded, StatusDead, StatusCanceled,
	} {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	for _, s := range []Status{"", "PENDING", "done", "failed", "in-flight"} {
		if s.Valid() {
			t.Errorf("%q should not be valid", s)
		}
	}
}

func TestExhaustedBudget(t *testing.T) {
	tests := []struct {
		attempt, max int
		want         bool
	}{
		{0, 3, false},
		{2, 3, false},
		{3, 3, true},
		{4, 3, true}, // 人工调小 max_attempts 后也要判定为耗尽
	}
	for _, tc := range tests {
		n := &Notification{Attempt: tc.attempt, MaxAttempts: tc.max}
		if got := n.ExhaustedBudget(); got != tc.want {
			t.Errorf("attempt=%d max=%d: got %v, want %v", tc.attempt, tc.max, got, tc.want)
		}
	}
}
