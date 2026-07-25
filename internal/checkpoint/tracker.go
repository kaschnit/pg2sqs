package checkpoint

import "github.com/jackc/pglogrepl"

type Tracker struct{}

func NewTracker() *Tracker {
	return &Tracker{} // TODO
}

func (t *Tracker) Track(lsn pglogrepl.LSN) {
	panic("TODO")
}

// Ack acknowledges the LSN and returns the latest possible LSN that is
// safe to flush given all acked LSNs.
func (t *Tracker) Ack(lsn pglogrepl.LSN) pglogrepl.LSN {
	panic("TODO")
}
