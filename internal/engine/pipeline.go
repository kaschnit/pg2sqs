package engine

import (
	"context"
	"log/slog"

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
	changes := p.stream.Start(ctx)
	go func() {
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
	sent := p.batcher.Start(ctx, trackedChanges)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case lsn := <-sent:
				safeLSN := p.tracker.Ack(lsn)

				// TODO - handle error?
				if err := p.stream.Flush(ctx, safeLSN); err != nil {
					slog.ErrorContext(ctx, "failed to flush LSN",
						slog.Any("err", err),
						slog.Int64("lsn", int64(safeLSN)))
				}
			}
		}
	}()
}
