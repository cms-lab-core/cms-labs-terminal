// Package broker owns terminal sessions independently from browser connections.
package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/maintainer64/cms-labs-terminal/internal/scrollback"
)

const (
	// DefaultScrollbackBytes keeps enough raw terminal output for ordinary lab work while keeping
	// the memory cost of one session bounded and predictable.
	DefaultScrollbackBytes = 1 << 20
	// DefaultLease is how long an unattended remote process is retained.
	DefaultLease    = 30 * time.Minute
	clientQueueSize = 32
)

var (
	DefaultSize       = Size{Cols: 80, Rows: 24}
	ErrClientReplaced = errors.New("terminal was opened in another browser")
)

type Size struct {
	Cols int `json:"cols"`
	Rows int `json:"rows"`
}

func (s Size) Valid() bool {
	return s.Cols > 0 && s.Cols <= 65535 && s.Rows > 0 && s.Rows <= 65535
}

func (s Size) OrDefault() Size {
	if !s.Valid() {
		return DefaultSize
	}
	return s
}

// Mode selects the Kubernetes streaming subresource used for a target.
type Mode string

const (
	ModeExec   Mode = "exec"
	ModeAttach Mode = "attach"
)

// Spec is an already authorized and resolved remote terminal.
type Spec struct {
	Target    string
	Label     string
	Mode      Mode
	Namespace string
	Pod       string
	Container string
	Command   []string
}

// Process is the long-lived remote PTY owned by a Session.
type Process interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Resize(Size) error
	Close() error
}

// Device opens a remote PTY. A successful Process must remain usable after ctx is cancelled; the
// Session explicitly closes it when its lease expires or the broker shuts down.
type Device interface {
	Start(ctx context.Context, spec Spec, size Size) (Process, error)
}

type Session struct {
	device Device
	spec   Spec

	scrollbackBytes int
	lease           time.Duration

	startMu sync.Mutex
	mu      sync.Mutex
	buffer  *scrollback.Scrollback
	process Process
	client  *Client
	// lastSeen is the last detach time. Device output must not extend a lease while nobody is
	// watching; a noisy background command would otherwise keep a session forever.
	lastSeen time.Time
	closed   bool
	err      error

	ended   chan struct{}
	endOnce sync.Once
}

// Client is the currently attached browser. A newer attach takes over the PTY and closes the old
// client's output, preventing two tabs from interleaving keystrokes into one shell.
type Client struct {
	session *Session

	mu      sync.Mutex
	queue   chan []byte
	current []byte
	err     error
	closed  bool
	once    sync.Once
}

func New(device Device, spec Spec, scrollbackBytes int, lease time.Duration) *Session {
	if scrollbackBytes <= 0 {
		scrollbackBytes = DefaultScrollbackBytes
	}
	if lease <= 0 {
		lease = DefaultLease
	}

	return &Session{
		device: device, spec: spec, scrollbackBytes: scrollbackBytes, lease: lease,
		buffer: scrollback.New(scrollbackBytes), lastSeen: time.Now(), ended: make(chan struct{}),
	}
}

func (s *Session) Spec() Spec { return s.spec }

func (s *Session) Attach(ctx context.Context, size Size) (*Client, error) {
	if err := s.start(ctx, size.OrDefault()); err != nil {
		return nil, err
	}

	client := &Client{session: s, queue: make(chan []byte, clientQueueSize)}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, s.endedErr()
	}
	if replay := s.buffer.Snapshot(); len(replay) > 0 {
		client.queue <- replay
	}
	previous := s.client
	s.client = client
	s.lastSeen = time.Now()
	s.mu.Unlock()

	if previous != nil {
		previous.closeOutput(ErrClientReplaced)
	}

	go func() {
		<-ctx.Done()
		_ = client.Close()
	}()

	return client, nil
}

func (c *Client) Read(p []byte) (int, error) {
	for {
		c.mu.Lock()
		if len(c.current) > 0 {
			n := copy(p, c.current)
			c.current = c.current[n:]
			c.mu.Unlock()
			return n, nil
		}
		queue := c.queue
		c.mu.Unlock()

		chunk, ok := <-queue
		if !ok {
			c.mu.Lock()
			err := c.err
			c.mu.Unlock()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}

		c.mu.Lock()
		c.current = chunk
		c.mu.Unlock()
	}
}

func (c *Client) Write(p []byte) (int, error) { return c.session.write(c, p) }

func (c *Client) Close() error {
	c.once.Do(func() {
		c.session.detach(c)
		c.closeOutput(nil)
	})
	return nil
}

func (c *Client) closeOutput(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.err = err
	close(c.queue)
}

func (c *Client) deliver(p []byte) bool {
	chunk := append([]byte(nil), p...)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	select {
	case c.queue <- chunk:
		return true
	default:
		return false
	}
}

func (s *Session) write(client *Client, p []byte) (int, error) {
	s.mu.Lock()
	if s.closed || s.client != client || s.process == nil {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	process := s.process
	s.mu.Unlock()
	return process.Write(p)
}

func (s *Session) detach(client *Client) {
	s.mu.Lock()
	if s.client == client {
		s.client = nil
		s.lastSeen = time.Now()
	}
	s.mu.Unlock()
}

func (s *Session) Detached() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client == nil
}

func (s *Session) LastSeen() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSeen
}

func (s *Session) Replay() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.Snapshot()
}

func (s *Session) Live() bool {
	select {
	case <-s.ended:
		return false
	default:
		return true
	}
}

func (s *Session) Wait() <-chan struct{} { return s.ended }

func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Session) Close() { s.end(fmt.Errorf("session for %s was closed", s.spec.Target)) }

func (s *Session) end(err error) {
	s.endOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.err = err
		process, client := s.process, s.client
		s.process = nil
		s.client = nil
		s.mu.Unlock()

		close(s.ended)
		if process != nil {
			_ = process.Close()
		}
		if client != nil {
			client.closeOutput(err)
		}
	})
}

func (s *Session) endedErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	return errors.New("session is closed")
}

func (s *Session) start(ctx context.Context, size Size) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return s.endedErr()
	}
	if s.process != nil {
		process := s.process
		s.mu.Unlock()
		_ = process.Resize(size)
		return nil
	}
	s.mu.Unlock()

	process, err := s.device.Start(context.WithoutCancel(ctx), s.spec, size)
	if err != nil {
		err = fmt.Errorf("starting %v on %s: %w", s.spec.Command, s.spec.Target, err)
		s.end(err)
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = process.Close()
		return s.endedErr()
	}
	s.process = process
	s.lastSeen = time.Now()
	s.mu.Unlock()

	go s.pump(process)
	return nil
}

func (s *Session) pump(process Process) {
	chunk := make([]byte, 32<<10)
	for {
		n, err := process.Read(chunk)
		if n > 0 {
			s.emit(chunk[:n])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.end(nil)
			} else {
				s.end(fmt.Errorf("reading from %s: %w", s.spec.Target, err))
			}
			return
		}
	}
}

func (s *Session) emit(p []byte) {
	s.mu.Lock()
	s.buffer.Append(p)
	client := s.client
	s.mu.Unlock()

	if client != nil && !client.deliver(p) {
		_ = client.Close()
	}
}

func (s *Session) Resize(size Size) error {
	s.mu.Lock()
	process, closed := s.process, s.closed
	s.mu.Unlock()
	if closed || process == nil {
		return fmt.Errorf("session for %s is not running", s.spec.Target)
	}
	return process.Resize(size.OrDefault())
}

func (s *Session) Expired(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client == nil && !s.closed && now.Sub(s.lastSeen) > s.lease
}

func (s *Session) SetLease(lease time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lease = lease
}
