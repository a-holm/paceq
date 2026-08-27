package notify

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// aMsg is one pending delivery the cases hand around.
func aMsg() OutboxMsg {
	return OutboxMsg{
		ID: 7, Topic: "run.failed", Subject: "backup-db",
		Target: "vakt", Payload: `{"event":"run.failed","job":"backup-db"}`, Attempts: 3,
	}
}

func TestBackoffLadderRisesAndCaps(t *testing.T) {
	want := []time.Duration{
		10 * time.Second,
		30 * time.Second,
		2 * time.Minute,
		10 * time.Minute,
		30 * time.Minute,
		time.Hour,
		time.Hour, // attempt 7: capped
		time.Hour, // attempt 99: still capped
	}
	for i, w := range want {
		if got := Backoff(i + 1); got != w {
			t.Errorf("Backoff(%d) = %s, want %s", i+1, got, w)
		}
	}
}

func TestWirePayloadMergesSuppressedFacts(t *testing.T) {
	msg := aMsg()
	if got := WirePayload(msg); got != msg.Payload {
		t.Fatalf("ungrouped payload was touched: %s", got)
	}
	msg.Suppressed = 49
	msg.WindowOpenedAt = time.UnixMilli(1234)
	wire := WirePayload(msg)
	if !strings.Contains(wire, `"similar_suppressed":49`) || !strings.Contains(wire, `"window_opened_at":1234`) {
		t.Fatalf("the wire envelope lacks the merge facts: %s", wire)
	}
	if !strings.Contains(wire, `"job":"backup-db"`) {
		t.Fatalf("merge rewrote stored fields: %s", wire)
	}
	broken := aMsg()
	broken.Payload = "{not json"
	broken.Suppressed = 3
	if got := WirePayload(broken); got != broken.Payload {
		t.Fatalf("unparseable payloads were not handed through verbatim: %s", got)
	}
}

func TestExecNotifierDeniesEnvByDefaultAndPassesTheEvent(t *testing.T) {
	t.Setenv("PACEQ_SECRET_PROBE", "leak-me")
	captured := struct {
		env   []string
		stdin string
	}{}
	e := &ExecNotifier{Name: "vakt", Argv: []string{"/usr/local/bin/pulseq-varsle"}}
	e.Start = func(_ context.Context, _ []string, env []string, stdin string) (wait func() error, cancel func(), err error) {
		captured.env, captured.stdin = append([]string(nil), env...), stdin
		return func() error { return nil }, func() {}, nil
	}
	msg := aMsg()
	if err := e.Send(context.Background(), msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	joined := strings.Join(captured.env, "\n")
	if strings.Contains(joined, "PACEQ_SECRET_PROBE") || strings.Contains(joined, "PATH=") {
		t.Fatalf("the empty-env baseline leaked something:\n%s", joined)
	}
	for _, want := range []string{"PULSEQ_EVENT=run.failed", "PULSEQ_SUBJECT=backup-db", "PULSEQ_TARGET=vakt"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the notifier environment lacks %s:\n%s", want, joined)
		}
	}
	if captured.stdin != msg.Payload {
		t.Errorf("stdin = %q, want the stored payload %q", captured.stdin, msg.Payload)
	}
}

func TestExecNotifierInheritsOnlyNamedVariables(t *testing.T) {
	t.Setenv("PACEQ_INHERIT_ME", "yes")
	t.Setenv("PACEQ_NOT_FOR_YOU", "no")
	var got []string
	e := &ExecNotifier{Name: "vakt", Argv: []string{"/bin/x"}, InheritEnv: []string{"PACEQ_INHERIT_ME", "PATH"}}
	e.Start = func(_ context.Context, _ []string, env []string, _ string) (wait func() error, cancel func(), err error) {
		got = append([]string(nil), env...)
		return func() error { return nil }, func() {}, nil
	}
	if err := e.Send(context.Background(), aMsg()); err != nil {
		t.Fatalf("send: %v", err)
	}
	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, "PACEQ_INHERIT_ME=yes") {
		t.Errorf("the named variable was not inherited:\n%s", joined)
	}
	if strings.Contains(joined, "PACEQ_NOT_FOR_YOU") {
		t.Errorf("an unnamed variable rode along:\n%s", joined)
	}
	for _, kv := range got {
		name, _, _ := strings.Cut(kv, "=")
		if strings.Contains(name, "=") || name == "" {
			t.Errorf("environment entry cannot be passed to execve: %q", kv)
		}
	}
}

func TestExecNotifierReportsFailuresWithStderr(t *testing.T) {
	e := &ExecNotifier{Name: "vakt", Argv: []string{"/usr/local/bin/relay"}}
	e.Start = func(_ context.Context, _ []string, _ []string, _ string) (wait func() error, cancel func(), err error) {
		return func() error { return errors.New("exit status 1") }, func() {}, nil
	}
	err := e.Send(context.Background(), aMsg())
	if err == nil || !strings.Contains(err.Error(), "vakt") || !strings.Contains(err.Error(), "exit status 1") {
		t.Fatalf("failure text lost its identity: %v", err)
	}
	cancelled := &ExecNotifier{Name: "vakt", Argv: []string{"/x"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled.Start = func(cctx context.Context, _ []string, _ []string, _ string) (wait func() error, cancel func(), err error) {
		return func() error { return cctx.Err() }, func() {}, nil
	}
	sendErr := cancelled.Send(ctx, aMsg())
	if sendErr == nil || !strings.Contains(sendErr.Error(), "vakt") {
		t.Fatalf("a dead context must still produce a notifier-identified error: %v", sendErr)
	}
}
