package engine

import (
	"context"
	"log/slog"
	"sync"

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

// Run starts the pipeline
func (p *Pipeline) Run(ctx context.Context) {
	var wg sync.WaitGroup

	// Track changes.
	changes := p.stream.Start(ctx)
	trackedChanges := make(chan event.Change, 10000) // TODO configure buf size
	wg.Go(func() {
		defer close(trackedChanges)

		for {
			select {
			case <-ctx.Done():
				return
			case change, ok := <-changes:
				if !ok {
					return
				}

				p.tracker.Track(change.LSN)

				select {
				case <-ctx.Done():
					return
				case trackedChanges <- change:
				}
			}
		}
	})

	// Send tracked changes.
	sent := p.batcher.Start(ctx, trackedChanges)
	wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case lsn, ok := <-sent:
				if !ok {
					return
				}

				safeLSN := p.tracker.Ack(lsn)

				// TODO - handle error?
				if err := p.stream.Flush(ctx, safeLSN); err != nil {
					slog.ErrorContext(ctx, "failed to flush LSN",
						slog.Any("err", err),
						slog.Int64("lsn", int64(safeLSN)))
				}
			}
		}
	})

	wg.Wait()
}
