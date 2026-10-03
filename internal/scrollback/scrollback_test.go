package scrollback

import (
	"bytes"
	"strings"
	"testing"
)

// TestAppendKeepsTheMostRecentOutput is the property the bound exists for: whatever the student ran,
// what they reconnect to is the end of it, not the beginning.
func TestAppendKeepsTheMostRecentOutput(t *testing.T) {
	t.Parallel()

	buffer := New(12)

	buffer.Append([]byte("old output\n"))
	buffer.Append([]byte("new line\n"))

	if got := string(buffer.Snapshot()); got != "new line\n" {
		t.Fatalf("snapshot = %q, want the most recent complete line", got)
	}

	if buffer.Bytes() != 12 {
		t.Fatalf("bytes = %d, want the buffer full at 12", buffer.Bytes())
	}
}

// TestAppendDiscardsAWholeBufferWhenOneBurstOverflows covers the command that prints more than the
// entire budget. Keeping the head of it would mean a replay consisting of output from the middle of a
// long dump, with the useful prompt scrolled away somewhere a client can never reach.
func TestAppendDiscardsAWholeBufferWhenOneBurstOverflows(t *testing.T) {
	t.Parallel()

	buffer := New(6)

	buffer.Append([]byte("one\ntwo\nthree\nfour\n"))

	// The burst is larger than the entire budget, so only its tail survives -- and that tail is
	// trimmed to the last complete line rather than replayed from the middle of one.
	if got := string(buffer.Snapshot()); got != "four\n" {
		t.Fatalf("snapshot = %q, want the tail of the burst", got)
	}

	if buffer.Bytes() != 6 {
		t.Fatalf("bytes = %d, want the tail of the burst regardless of replayability", buffer.Bytes())
	}
}

// TestSnapshotTrimsToALineBoundaryOnceOutputHasBeenDropped is what keeps a replay from drawing garbage.
//
// The buffer wraps at an arbitrary byte, which is very likely the middle of an escape sequence. A
// terminal handed a fragment of one will either print it or wait for the rest of it, and in the second
// case it swallows the first real line that follows.
func TestSnapshotTrimsToALineBoundaryOnceOutputHasBeenDropped(t *testing.T) {
	t.Parallel()

	buffer := New(6)

	buffer.Append([]byte("first line\nsecond line\nthird"))

	if got := string(buffer.Snapshot()); got != "third" {
		t.Fatalf("snapshot = %q, want the partial line dropped", got)
	}
}

func TestSnapshotIsExactWhileNothingHasBeenDropped(t *testing.T) {
	t.Parallel()

	buffer := New(1024)

	buffer.Append([]byte("no newline here"))

	if got := string(buffer.Snapshot()); got != "no newline here" {
		t.Fatalf("snapshot = %q, want everything, newline or not", got)
	}
}

func TestSnapshotIsEmptyWhenOnlyAPartialLineSurvives(t *testing.T) {
	t.Parallel()

	buffer := New(4)

	buffer.Append([]byte("abcdefgh"))

	if got := buffer.Snapshot(); got != nil {
		t.Fatalf("snapshot = %q, want nothing: a partial line replays as noise", got)
	}
}

// TestSnapshotDoesNotAliasTheBuffer matters because the replay outlives the next Append: handing out
// the live slice would let a reconnecting client read bytes a student's command produced afterwards.
func TestSnapshotDoesNotAliasTheBuffer(t *testing.T) {
	t.Parallel()

	buffer := New(64)

	buffer.Append([]byte("before"))

	first := buffer.Snapshot()

	buffer.Append([]byte(" after"))

	if !strings.HasPrefix(string(first), "before") || strings.Contains(string(first), "after") {
		t.Fatalf("snapshot changed under the caller: %q", first)
	}
}

func TestAppendIgnoresEmptyWrites(t *testing.T) {
	t.Parallel()

	buffer := New(64)

	buffer.Append(nil)
	buffer.Append([]byte{})

	if buffer.Bytes() != 0 {
		t.Fatalf("bytes = %d, want an empty buffer left empty", buffer.Bytes())
	}
}

// TestNewTreatsANonPositiveLimitAsOneByte covers a misconfigured budget. A buffer that could hold
// nothing would make every replay silently empty, which looks exactly like a session with no history,
// so the limit is floored rather than allowed to reach zero.
func TestNewTreatsANonPositiveLimitAsOneByte(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{0, -1} {
		buffer := New(limit)

		buffer.Append([]byte("output"))

		if buffer.Bytes() != 1 {
			t.Fatalf("New(%d) holds %d bytes, want 1", limit, buffer.Bytes())
		}
	}
}

// TestRealisticOutputSurvivesAReconnect walks the shape of an actual prompt cycle to check the bound is
// in the right order of magnitude: a few prompts and short command output should all still be there,
// while a long dump should have been forgotten rather than crowding them out.
func TestRealisticOutputSurvivesAReconnect(t *testing.T) {
	t.Parallel()

	const limit = 4096

	buffer := New(limit)

	var session bytes.Buffer

	session.WriteString("Welcome to the network lab.\r\nroot@srl1:/# ")
	session.WriteString("sr_cli show version\r\nSRLINUX 25.3.3-158\r\nroot@srl1:/# ")

	buffer.Append(session.Bytes())

	dump := strings.Repeat("interface ethernet-1/1 detail line\r\n", 400)

	buffer.Append([]byte(dump))
	session.Reset()
	session.WriteString("root@srl1:/# ")

	buffer.Append(session.Bytes())

	replay := string(buffer.Snapshot())

	if strings.Contains(replay, "Welcome to the network lab.") {
		t.Error("a 400-line dump should have pushed the welcome banner out of the budget")
	}

	if !strings.Contains(replay, "root@srl1:/# ") {
		t.Error("the final prompt must survive: it is what the student reconnects to")
	}

	if len(replay) > limit {
		t.Fatalf("replay is %d bytes, want at most the %d byte budget", len(replay), limit)
	}
}
