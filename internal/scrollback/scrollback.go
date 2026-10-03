// Package scrollback keeps the recent output of a terminal so a browser that reconnects can be shown
// what it missed.
//
// It stores raw bytes rather than parsed lines, and that is the whole trick: the bytes coming out of a
// pty are already the terminal's truth, including cursor moves, colours and partial lines. Replaying
// them into a fresh client rebuilds the screen exactly, and costs one loop over a buffer. Anything that
// tried to understand the output instead -- stripping escapes, splitting into lines -- would have to
// reimplement a terminal emulator to get the screen back right, and would still be wrong for
// full-screen programs.
package scrollback

import "bytes"

// Scrollback is a bounded, replayable buffer of terminal output.
//
// The bound is a budget in bytes rather than a line count because a single command can print a
// megabyte: a line limit would be either useless or unbounded depending on what the student ran. Bytes
// also make the memory cost knowable up front, which is what lets a broker hold several sessions at
// once without having to reason about what they might print.
type Scrollback struct {
	limit int
	buf   []byte
	// cut reports that the oldest bytes were dropped. A replay that starts mid-escape-sequence or
	// mid-line draws garbage, so the replay is trimmed forward to the first line boundary whenever this
	// is set.
	cut bool
}

// New returns a Scrollback holding at most limit bytes.
//
// A limit of zero or less is treated as one byte: a zero-sized buffer would make every write a discard
// and every replay empty, which is a valid configuration only if a caller meant something else.
func New(limit int) *Scrollback {
	if limit <= 0 {
		limit = 1
	}

	return &Scrollback{limit: limit, buf: make([]byte, 0, limit)}
}

// Append adds output, discarding the oldest bytes when the budget is exceeded.
func (s *Scrollback) Append(p []byte) {
	if len(p) == 0 {
		return
	}

	if len(p) >= s.limit {
		// The incoming burst alone fills the buffer, so nothing of it survives except its tail. This is
		// also the case that sets `cut`: what remains begins in the middle of whatever was printed.
		s.buf = append(s.buf[:0], p[len(p)-s.limit:]...)
		s.cut = true

		return
	}

	if overflow := len(s.buf) + len(p) - s.limit; overflow > 0 {
		s.buf = append(s.buf[:0], s.buf[overflow:]...)
		s.cut = true
	}

	s.buf = append(s.buf, p...)
}

// Snapshot returns the buffered output, ready to be replayed into a client.
//
// When older bytes have been dropped, the result starts at the first newline in what is left. Cutting
// anywhere else would leave a half-finished escape sequence or a line with no start, and a terminal
// given either draws visible nonsense at the top of the screen. If the buffer holds no newline at all
// then everything left is one partial line, and returning it would be worse than returning nothing.
func (s *Scrollback) Snapshot() []byte {
	if !s.cut {
		return bytes.Clone(s.buf)
	}

	if index := bytes.IndexByte(s.buf, '\n'); index >= 0 {
		return bytes.Clone(s.buf[index+1:])
	}

	return nil
}

// Bytes returns how much output is currently held, including bytes that Snapshot would trim.
func (s *Scrollback) Bytes() int {
	return len(s.buf)
}
