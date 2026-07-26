package checkpoint

import (
	"slices"
	"sync"

	"github.com/jackc/pglogrepl"
)

type Tracker struct {
	mu      sync.Mutex
	tracked []pglogrepl.LSN
	pending map[uint64]struct{}
	safeLSN pglogrepl.LSN
}

func NewTracker() *Tracker {
	return &Tracker{
		pending: make(map[uint64]struct{}),
	}
}

func (t *Tracker) Track(lsn pglogrepl.LSN) {
	if lsn <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// LSN doesn't need to be tracked if it came before a known safe-to-flush LSN.
	if lsn <= t.safeLSN {
		return
	}

	// LSN doesn't need to be tracked if it's already tracked as pending.
	if _, ok := t.pending[uint64(lsn)]; ok {
		return
	}

	// Track LSN as pending.
	t.pending[uint64(lsn)] = struct{}{}

	// Append LSN at the end if it's greater than any other LSN that needs to be tracked.
	// Maintains sorted order of tracked LSNs.
	if len(t.tracked) == 0 || lsn > t.tracked[len(t.tracked)-1] {
		t.tracked = append(t.tracked, lsn)
		return
	}

	// Handle inserting LSN into the middle of the tracked list.
	// This is inefficient but should not be hit as long as LSNs are tracked
	// in order, which is the expected usage pattern.
	idx, found := slices.BinarySearch(t.tracked, lsn)
	if found {
		return
	}
	t.tracked = append(t.tracked, 0)
	copy(t.tracked[idx+1:], t.tracked[idx:])
	t.tracked[idx] = lsn
}

// Ack acknowledges the LSN and returns the latest possible LSN that is
// safe to flush given all acked LSNs.
func (t *Tracker) Ack(lsn pglogrepl.LSN) pglogrepl.LSN {
	if lsn <= 0 {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.pending, uint64(lsn))

	// Nothing to do if LSN is older than an LSN that has already been acked.
	if lsn <= t.safeLSN {
		return t.safeLSN
	}

	// Find highest safe LSN
	for len(t.tracked) > 0 {
		first := t.tracked[0]
		if _, pending := t.pending[uint64(first)]; pending {
			break
		}

		t.safeLSN = first
		t.tracked = t.tracked[1:]
	}

	return t.safeLSN
}
