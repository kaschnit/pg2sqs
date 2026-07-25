package engine

import (
	"context"

	"github.com/kaschnit/pg2sqs/internal/checkpoint"
	"github.com/kaschnit/pg2sqs/internal/event"
	"github.com/kaschnit/pg2sqs/internal/publish"
	"github.com/kaschnit/pg2sqs/internal/replication"
)

// Pipeline orchestrates change data capture from source to destination.
type Pipeline struct {
	stream  *replication.Stream
	tracker *checkpoint.Tracker
	batcher *publish.Batcher
}

// NewPipeline creates a [Pipeline].
func NewPipeline(stream *replication.Stream, tracker *checkpoint.Tracker, batcher *publish.Batcher) *Pipeline {
	return &Pipeline{
		stream:  stream,
		tracker: tracker,
		batcher: batcher,
	}
}

// Start starts the pipeline
func (p *Pipeline) Start(ctx context.Context) {
	trackedChanges := make(chan event.Change, 10000) // TODO configure buf size

	// Track changes.
	go func() {
		changes := p.stream.Changes()
		for {
			select {
			case <-ctx.Done():
				return
			case change := <-changes:
				p.tracker.Track(change.LSN)
				trackedChanges <- change
			}
		}
	}()

	// Send tracked changes.
	go func() {
		sent := p.batcher.Subscribe(ctx, trackedChanges)
		for {
			select {
			case <-ctx.Done():
				return
			case lsn := <-sent:
				safeLSN := p.tracker.Ack(lsn)
				p.stream.Flush(safeLSN)
			}
		}
	}()
}
