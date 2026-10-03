package broker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultHistory      = 3
	DefaultReapInterval = time.Minute
)

type Planner func(ctx context.Context, target string) (Spec, error)

type Settings struct {
	Lease           time.Duration
	ScrollbackBytes int
	History         int
	ReapInterval    time.Duration
}

type Registry struct {
	device   Device
	plan     Planner
	settings Settings

	mu       sync.Mutex
	sessions map[string]*targetSessions
}

type targetSessions struct {
	mu      sync.Mutex
	session *Session
	history []archived
}

type archived struct {
	ended  time.Time
	replay []byte
}

func NewRegistry(device Device, plan Planner, settings Settings) *Registry {
	if settings.Lease <= 0 {
		settings.Lease = DefaultLease
	}
	if settings.ScrollbackBytes <= 0 {
		settings.ScrollbackBytes = DefaultScrollbackBytes
	}
	if settings.History <= 0 {
		settings.History = DefaultHistory
	}
	if settings.ReapInterval <= 0 {
		settings.ReapInterval = DefaultReapInterval
	}
	return &Registry{device: device, plan: plan, settings: settings, sessions: map[string]*targetSessions{}}
}

// Attach returns one live Session per target. Creation is serialized per target rather than for the
// whole registry, so a slow Kubernetes lookup for r1 cannot block a connection to s1.
func (r *Registry) Attach(
	ctx context.Context, target string, size Size,
) (session *Session, client *Client, history []byte, spec Spec, err error) {
	entry := r.entry(target)
	entry.mu.Lock()

	if entry.session != nil && !entry.session.Live() {
		r.archive(entry)
		entry.session = nil
	}

	if entry.session == nil {
		spec, err = r.plan(ctx, target)
		if err != nil {
			entry.mu.Unlock()
			return nil, nil, nil, Spec{}, err
		}
		entry.session = New(r.device, spec, r.settings.ScrollbackBytes, r.settings.Lease)
	} else {
		spec = entry.session.Spec()
	}

	session = entry.session
	history = r.replay(entry)
	client, err = session.Attach(ctx, size)
	entry.mu.Unlock()
	if err != nil {
		return nil, nil, nil, Spec{}, err
	}
	return session, client, history, spec, nil
}

func (r *Registry) entry(target string) *targetSessions {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.sessions[target]
	if !ok {
		entry = &targetSessions{}
		r.sessions[target] = entry
	}
	return entry
}

func (r *Registry) Resize(target string, size Size) error {
	r.mu.Lock()
	entry := r.sessions[target]
	r.mu.Unlock()
	if entry == nil {
		return fmt.Errorf("no session for %s", target)
	}
	entry.mu.Lock()
	session := entry.session
	entry.mu.Unlock()
	if session == nil {
		return fmt.Errorf("no session for %s", target)
	}
	return session.Resize(size)
}

func (r *Registry) Session(target string) *Session {
	r.mu.Lock()
	entry := r.sessions[target]
	r.mu.Unlock()
	if entry == nil {
		return nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.session
}

func (r *Registry) Reap(now time.Time) {
	r.mu.Lock()
	entries := make([]*targetSessions, 0, len(r.sessions))
	for _, entry := range r.sessions {
		entries = append(entries, entry)
	}
	r.mu.Unlock()

	for _, entry := range entries {
		entry.mu.Lock()
		if entry.session != nil && entry.session.Expired(now) {
			// Keep the per-target lock until the session is marked closed. Otherwise a new
			// browser can attach after Expired reports true but before Close runs, and the
			// reaper would tear down a PTY that has just become active again.
			entry.session.Close()
		}
		entry.mu.Unlock()
	}
}

func (r *Registry) Run(ctx context.Context) {
	ticker := time.NewTicker(r.settings.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.Close()
			return
		case now := <-ticker.C:
			r.Reap(now)
		}
	}
}

func (r *Registry) Close() {
	r.mu.Lock()
	entries := make([]*targetSessions, 0, len(r.sessions))
	for _, entry := range r.sessions {
		entries = append(entries, entry)
	}
	r.mu.Unlock()

	for _, entry := range entries {
		entry.mu.Lock()
		session := entry.session
		entry.mu.Unlock()
		if session != nil {
			session.Close()
		}
	}
}

func (r *Registry) archive(entry *targetSessions) {
	if entry.session == nil {
		return
	}
	replay := entry.session.Replay()
	if len(replay) == 0 {
		return
	}
	entry.history = append(entry.history, archived{ended: time.Now(), replay: replay})
	if over := len(entry.history) - r.settings.History; over > 0 {
		entry.history = append([]archived(nil), entry.history[over:]...)
	}
}

func (r *Registry) replay(entry *targetSessions) []byte {
	var replay []byte
	for _, past := range entry.history {
		replay = append(replay, []byte(separator(past.ended))...)
		replay = append(replay, past.replay...)
	}
	return replay
}

func separator(ended time.Time) string {
	return fmt.Sprintf("\r\n--- session ended %s ---\r\n", ended.Format("15:04:05"))
}
