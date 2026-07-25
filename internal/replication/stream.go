package replication

import (
	"github.com/jackc/pglogrepl"
	"github.com/kaschnit/pg2sqs/internal/event"
)

type Stream struct{}

func NewStream() *Stream {
	return &Stream{} // TODO
}

func (s *Stream) Changes() <-chan event.Change {
	panic("TODO")
}

func (s *Stream) Flush(lsn pglogrepl.LSN) {
	panic("TODO")
}
