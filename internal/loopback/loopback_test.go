package loopback

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maintainer64/cms-labs-terminal/internal/broker"
)

type fakeDevice struct {
	mu      sync.Mutex
	starts  int
	process *fakeProcess
}

type fakeProcess struct {
	mu     sync.Mutex
	input  bytes.Buffer
	reader *io.PipeReader
	writer *io.PipeWriter
	sizes  []broker.Size
	once   sync.Once
}

func (d *fakeDevice) Start(_ context.Context, _ broker.Spec, size broker.Size) (broker.Process, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.starts++
	reader, writer := io.Pipe()
	d.process = &fakeProcess{reader: reader, writer: writer, sizes: []broker.Size{size}}
	return d.process, nil
}

func (d *fakeDevice) current() *fakeProcess {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.process
}

func (d *fakeDevice) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.starts
}

func (p *fakeProcess) Read(buffer []byte) (int, error) { return p.reader.Read(buffer) }
func (p *fakeProcess) Write(buffer []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.input.Write(buffer)
}
func (p *fakeProcess) Resize(size broker.Size) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sizes = append(p.sizes, size)
	return nil
}
func (p *fakeProcess) Close() error {
	p.once.Do(func() {
		_ = p.writer.Close()
		_ = p.reader.Close()
	})
	return nil
}
func (p *fakeProcess) print(text string) { _, _ = io.WriteString(p.writer, text) }
func (p *fakeProcess) typed() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.input.String()
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

// memoryListener exercises net/http full-duplex behavior without binding a host port (the test
// sandbox deliberately forbids listen(2)).
type memoryListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

type memoryAddress string

func (a memoryAddress) Network() string { return "memory" }
func (a memoryAddress) String() string  { return string(a) }

func newMemoryListener() *memoryListener {
	return &memoryListener{connections: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *memoryListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *memoryListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *memoryListener) Addr() net.Addr { return memoryAddress("broker") }

func (l *memoryListener) dial(_ context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.connections <- server:
		return client, nil
	case <-l.closed:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	}
}

func (l *memoryListener) client() *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: l.dial, DisableCompression: true}}
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(value)
}
func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestFullDuplexClientReconnectsToTheSameProcess(t *testing.T) {
	device := &fakeDevice{}
	registry := broker.NewRegistry(device, func(_ context.Context, target string) (broker.Spec, error) {
		return broker.Spec{
			Target: target, Label: "Router 1", Mode: broker.ModeExec,
			Namespace: "lab", Pod: "r1-pod", Container: "r1", Command: []string{"/bin/sh"},
		}, nil
	}, broker.Settings{})
	server := NewServer("127.0.0.1:0", registry, slog.New(slog.NewTextHandler(io.Discard, nil)))
	listener := newMemoryListener()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	httpClient := listener.client()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		registry.Close()
	}()

	firstInput, firstWriter := io.Pipe()
	firstOutput := &lockedBuffer{}
	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- (&Client{
			Address: "broker", Target: "r1", Identity: "verified-user",
			Input: firstInput, Output: firstOutput, FD: -1, HTTP: httpClient,
		}).Run(firstContext)
	}()

	waitFor(t, "remote process", func() bool { return device.current() != nil })
	if _, err := io.WriteString(firstWriter, "cd /tmp\n"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "input at the remote process", func() bool {
		return strings.Contains(device.current().typed(), "cd /tmp")
	})
	device.current().print("r1# ready\n")
	waitFor(t, "first browser output", func() bool { return strings.Contains(firstOutput.String(), "r1# ready") })
	cancelFirst()
	_ = firstWriter.Close()
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first loopback client did not detach")
	}

	secondInput, secondWriter := io.Pipe()
	defer func() { _ = secondWriter.Close() }()
	secondOutput := &lockedBuffer{}
	secondContext, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- (&Client{
			Address: "broker", Target: "r1",
			Input: secondInput, Output: secondOutput, FD: -1, HTTP: httpClient,
		}).Run(secondContext)
	}()
	waitFor(t, "scrollback in second browser", func() bool {
		return strings.Contains(secondOutput.String(), "r1# ready")
	})
	if device.count() != 1 {
		t.Fatalf("remote starts = %d, want one across reconnect", device.count())
	}
	cancelSecond()
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second loopback client did not detach")
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("server stopped early: %s", err)
		}
	default:
	}
}

func TestResizeEndpointRejectsMissingSession(t *testing.T) {
	registry := broker.NewRegistry(&fakeDevice{}, func(context.Context, string) (broker.Spec, error) {
		return broker.Spec{}, nil
	}, broker.Settings{})
	server := NewServer("127.0.0.1:0", registry, nil)
	listener := newMemoryListener()
	go server.Serve(listener) //nolint:errcheck // closed by the test.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	request, err := http.NewRequest(http.MethodPut,
		"http://broker/v1/targets/missing/size",
		strings.NewReader(`{"cols":120,"rows":40}`))
	if err != nil {
		t.Fatal(err)
	}
	response, err := listener.client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", response.StatusCode)
	}
}

func TestClabgateIdentityIsDecodedWithoutLoggingTheToken(t *testing.T) {
	t.Parallel()
	value := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user-42","username":"student"}`))
	identity := decodeIdentity(value)
	if identity.Subject != "user-42" || identity.Username != "student" {
		t.Fatalf("identity = %+v", identity)
	}
	if invalid := decodeIdentity("not base64!"); invalid != (workspaceIdentity{}) {
		t.Fatalf("invalid identity = %+v, want empty", invalid)
	}
}

func waitFor(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
