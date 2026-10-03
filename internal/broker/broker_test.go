package broker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

var errNoSuchTarget = errors.New("no such target")

type fakeDevice struct {
	mu       sync.Mutex
	attempts int
	fail     error
	byTarget map[string][]*fakeProcess
}

type fakeProcess struct {
	mu     sync.Mutex
	stdin  bytes.Buffer
	reader *io.PipeReader
	writer *io.PipeWriter
	sizes  []Size
	closed bool
}

func newFakeDevice() *fakeDevice { return &fakeDevice{byTarget: map[string][]*fakeProcess{}} }

func (d *fakeDevice) Start(_ context.Context, spec Spec, size Size) (Process, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attempts++
	if d.fail != nil {
		return nil, d.fail
	}
	reader, writer := io.Pipe()
	process := &fakeProcess{reader: reader, writer: writer, sizes: []Size{size}}
	d.byTarget[spec.Target] = append(d.byTarget[spec.Target], process)
	return process, nil
}

func (d *fakeDevice) latest(target string) *fakeProcess {
	d.mu.Lock()
	defer d.mu.Unlock()
	processes := d.byTarget[target]
	if len(processes) == 0 {
		return nil
	}
	return processes[len(processes)-1]
}

func (d *fakeDevice) startCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attempts
}

func (p *fakeProcess) Read(buffer []byte) (int, error) { return p.reader.Read(buffer) }
func (p *fakeProcess) Write(buffer []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	return p.stdin.Write(buffer)
}
func (p *fakeProcess) Resize(size Size) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sizes = append(p.sizes, size)
	return nil
}
func (p *fakeProcess) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()
	_ = p.writer.Close()
	return p.reader.Close()
}
func (p *fakeProcess) print(text string) { _, _ = io.WriteString(p.writer, text) }
func (p *fakeProcess) exit()             { _ = p.writer.Close() }
func (p *fakeProcess) typed() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stdin.String()
}
func (p *fakeProcess) resizeLog() []Size {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Size(nil), p.sizes...)
}

func planFor(command ...string) Planner {
	return func(_ context.Context, target string) (Spec, error) {
		return Spec{Target: target, Label: target, Mode: ModeExec, Command: command}, nil
	}
}

type watcher struct {
	mu   sync.Mutex
	text bytes.Buffer
}

func watch(client *Client) *watcher {
	result := &watcher{}
	go func() {
		buffer := make([]byte, 4096)
		for {
			n, err := client.Read(buffer)
			if n > 0 {
				result.mu.Lock()
				result.text.Write(buffer[:n])
				result.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	return result
}

func (w *watcher) saw(text string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Contains(w.text.String(), text)
}

type attachment struct {
	session *Session
	client  *Client
	history string
	watcher *watcher
}

func attach(t *testing.T, registry *Registry, target string) *attachment {
	t.Helper()
	session, client, history, _, err := registry.Attach(context.Background(), target, DefaultSize)
	if err != nil {
		t.Fatalf("attach %s: %s", target, err)
	}
	return &attachment{session: session, client: client, history: string(history), watcher: watch(client)}
}

func waitFor(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestSessionSurvivesReloadAndKeepsInputState(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/bash"), Settings{})
	first := attach(t, registry, "r1")
	_, _ = io.WriteString(first.client, "cd /tmp\nexport MARKER=kept\n")
	_ = first.client.Close()

	second := attach(t, registry, "r1")
	_, _ = io.WriteString(second.client, "pwd\n")
	if device.startCount() != 1 {
		t.Fatalf("starts = %d, want one", device.startCount())
	}
	if got := device.latest("r1").typed(); !strings.Contains(got, "cd /tmp") || !strings.Contains(got, "pwd") {
		t.Fatalf("same process received %q", got)
	}
}

func TestSecondBrowserTakesOverOnePTY(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{})
	first := attach(t, registry, "r1")
	second := attach(t, registry, "r1")
	if _, err := first.client.Write([]byte("must not arrive\n")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("old client write error = %v, want closed pipe", err)
	}
	device.latest("r1").print("only current browser\n")
	waitFor(t, "current browser output", func() bool { return second.watcher.saw("only current browser") })
	if device.startCount() != 1 {
		t.Fatalf("starts = %d, want one", device.startCount())
	}
}

func TestDetachedOutputIsReplayed(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{})
	first := attach(t, registry, "r1")
	_ = first.client.Close()
	device.latest("r1").print("background command finished\n")
	waitFor(t, "scrollback", func() bool {
		return strings.Contains(string(registry.Session("r1").Replay()), "background command finished")
	})
	second := attach(t, registry, "r1")
	waitFor(t, "replayed output", func() bool { return second.watcher.saw("background command finished") })
}

func TestLeaseNeverReapsAttachedButEndsDetached(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{Lease: time.Millisecond})
	current := attach(t, registry, "r1")
	registry.Reap(time.Now().Add(time.Hour))
	if !current.session.Live() {
		t.Fatal("attached session was reaped")
	}
	_ = current.client.Close()
	registry.Reap(time.Now().Add(time.Hour))
	waitFor(t, "detached session close", func() bool { return !current.session.Live() })
}

func TestLeaseStartsWhenSessionIsCreated(t *testing.T) {
	session := New(newFakeDevice(), Spec{Target: "r1"}, DefaultScrollbackBytes, time.Hour)
	if session.Expired(time.Now()) {
		t.Fatal("a session expired before its first attach")
	}
}

func TestHistoryIsSeparatedAndCapped(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{History: 2})
	for _, marker := range []string{"ancient", "older", "newest"} {
		current := attach(t, registry, "r1")
		device.latest("r1").print(marker + "\n")
		waitFor(t, marker+" output", func() bool { return current.watcher.saw(marker) })
		device.latest("r1").exit()
		waitFor(t, marker+" exit", func() bool { return !current.session.Live() })
	}
	current := attach(t, registry, "r1")
	if strings.Contains(current.history, "ancient") || !strings.Contains(current.history, "newest") {
		t.Fatalf("history = %q", current.history)
	}
	if strings.Count(current.history, "session ended") != 2 {
		t.Fatalf("history separators = %q", current.history)
	}
}

func TestTargetsAreIsolated(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{})
	r1 := attach(t, registry, "r1")
	s1 := attach(t, registry, "s1")
	device.latest("r1").print("router only\n")
	waitFor(t, "router output", func() bool { return r1.watcher.saw("router only") })
	if s1.watcher.saw("router only") {
		t.Fatal("r1 output leaked into s1")
	}
}

func TestResizeReachesOnlyTheRunningProcess(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{})
	attach(t, registry, "r1")
	if err := registry.Resize("r1", Size{Cols: 132, Rows: 43}); err != nil {
		t.Fatal(err)
	}
	log := device.latest("r1").resizeLog()
	if got := log[len(log)-1]; got != (Size{Cols: 132, Rows: 43}) {
		t.Fatalf("last resize = %v", got)
	}
	if err := registry.Resize("s1", DefaultSize); err == nil {
		t.Fatal("resize of missing session succeeded")
	}
}

func TestUnknownTargetAndFailedStartLeaveNoZombie(t *testing.T) {
	device := newFakeDevice()
	unknown := NewRegistry(device, func(context.Context, string) (Spec, error) {
		return Spec{}, errNoSuchTarget
	}, Settings{})
	if _, _, _, _, err := unknown.Attach(context.Background(), "nope", DefaultSize); !errors.Is(err, errNoSuchTarget) {
		t.Fatalf("unknown target error = %v", err)
	}
	if device.startCount() != 0 {
		t.Fatalf("unknown target started %d processes", device.startCount())
	}

	device.fail = errors.New("API unavailable")
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{})
	if _, _, _, _, err := registry.Attach(context.Background(), "r1", DefaultSize); err == nil {
		t.Fatal("failed start succeeded")
	}
	if session := registry.Session("r1"); session == nil || session.Live() {
		t.Fatal("failed start left a live registry entry")
	}
	device.fail = nil
	attach(t, registry, "r1")
	if device.startCount() != 2 {
		t.Fatalf("start attempts = %d, want failed attempt and retry", device.startCount())
	}
}

func TestConcurrentFirstAttachStartsOnce(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{})
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, client, _, _, err := registry.Attach(context.Background(), "r1", DefaultSize)
			if err == nil {
				defer func() { _ = client.Close() }()
			}
		}()
	}
	close(start)
	wait.Wait()
	if device.startCount() != 1 {
		t.Fatalf("concurrent attaches started %d processes", device.startCount())
	}
}

func TestRunClosesProcessesOnShutdown(t *testing.T) {
	device := newFakeDevice()
	registry := NewRegistry(device, planFor("/bin/sh"), Settings{ReapInterval: time.Millisecond})
	current := attach(t, registry, "r1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { registry.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("registry did not stop")
	}
	if current.session.Live() {
		t.Fatal("session remained live after shutdown")
	}
}
