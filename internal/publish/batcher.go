package publish

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pglogrepl"
	"github.com/kaschnit/pg2sqs/internal/event"
)

// Sender allow sending SQS messages.
type Sender interface {
	// SendMessageBatch sends the SQS messages in a batch.
	SendMessageBatch(
		ctx context.Context,
		params *sqs.SendMessageBatchInput,
		optFns ...func(*sqs.Options),
	) (*sqs.SendMessageBatchOutput, error)
}

type BatchOpts struct {
	// Number of workers sending messages.
	Workers int
	// Max number of messages per batch send.
	// This must be at least 1 and can be no higher than 10 (max allowed by AWS SQS).
	MaxMessages int
	// Interval at which to flush if MaxMessages have not been buffered.
	// Set to <0 to disable flushing.
	FlushInterval time.Duration
}

// defaultBatchOpts are the default [BatchOpts].
var defaultBatchOpts = BatchOpts{
	Workers:       10,
	MaxMessages:   1, // No batching by default - request per message
	FlushInterval: 0, // No waiting by default - requests immediately flushed
}

// BatchOptsFunc is a function to configure [BatchOpts].
type BatchOptsFunc func(*BatchOpts)

// WithWorkers configures [BatchOpts.Workers],
func WithWorkers(workers int) BatchOptsFunc {
	// Cannot be below 1 - nonsense value.
	workers = max(1, workers)

	return func(opts *BatchOpts) {
		opts.Workers = workers
	}
}

// WithMaxMessages configures [BatchOpts.MaxMessages],
func WithMaxMessages(maxMessages int) BatchOptsFunc {
	// Cannot be below 1 - nonsense value.
	maxMessages = max(1, maxMessages)
	// Cannot be above 10 - max allowed by AWS SQS.
	maxMessages = min(10, maxMessages)

	return func(opts *BatchOpts) {
		opts.MaxMessages = maxMessages
	}
}

// WithFlushInterval configures [BatchOpts.FlushInterval],
func WithFlushInterval(flushInterval time.Duration) BatchOptsFunc {
	return func(opts *BatchOpts) {
		opts.FlushInterval = flushInterval
	}
}

// Batcher sends SQS messages in batches.
// It buffers until a max number of messages have been enqueued, or the flush interval
// has been reached.
type Batcher struct {
	sender   Sender
	queueURL string
	opts     BatchOpts
}

func NewBatcher(sender Sender, queueURL string, optFns ...BatchOptsFunc) *Batcher {
	batchOpts := defaultBatchOpts
	for _, optFn := range optFns {
		optFn(&batchOpts)
	}

	return &Batcher{
		sender:   sender,
		queueURL: queueURL,
		opts:     batchOpts,
	}
}

func (batcher *Batcher) Subscribe(ctx context.Context, changes <-chan event.Change) <-chan pglogrepl.LSN {
	batches := batcher.batchingStep(ctx, changes)
	completed := batcher.publishingStep(ctx, batches)
	return completed
}

func (batcher *Batcher) batchingStep(ctx context.Context, changes <-chan event.Change) <-chan []event.Change {
	batches := make(chan []event.Change, batcher.opts.Workers*2)
	go func() {
		changeBatch := make([]event.Change, 0, batcher.opts.MaxMessages)

		timer := time.NewTimer(batcher.opts.FlushInterval)
		stopTimer(timer)

		flush := func() {
			if len(changeBatch) == 0 {
				return
			}

			batches <- changeBatch
			changeBatch = make([]event.Change, 0, batcher.opts.MaxMessages)
			stopTimer(timer)
		}
		defer flush()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				flush()
			case change, ok := <-changes:
				if !ok {
					return
				}

				changeBatch = append(changeBatch, change)
				if len(changeBatch) >= batcher.opts.MaxMessages {
					flush()
				} else if len(changeBatch) == 1 {
					stopTimer(timer)
					timer.Reset(batcher.opts.FlushInterval)
				}
			}
		}
	}()

	return batches
}

func (batcher *Batcher) publishingStep(
	ctx context.Context,
	batches <-chan []event.Change,
) <-chan pglogrepl.LSN {
	completed := make(chan pglogrepl.LSN, batcher.opts.Workers*2)
	for range batcher.opts.Workers {
		go func() {
			select {
			case <-ctx.Done():
				return
			case changeBatch, ok := <-batches:
				if !ok {
					return
				}

				req := &sqs.SendMessageBatchInput{
					QueueUrl: &batcher.queueURL,
					Entries:  make([]sqstypes.SendMessageBatchRequestEntry, 0, len(changeBatch)),
				}
				for _, change := range changeBatch {
					body, err := change.Marshal()
					if err != nil {
						panic("TODO handle error: " + err.Error())
					}

					req.Entries = append(req.Entries, sqstypes.SendMessageBatchRequestEntry{
						Id:          new(change.LSN.String()),
						MessageBody: new(string(body)),
					})
				}

				resp, err := batcher.sender.SendMessageBatch(ctx, req)
				if err != nil {
					panic("TODO handle error: " + err.Error())
				}

				for _, entry := range resp.Failed {
					// TODO handle failed
					panic("FAILED TO SEND: " + *entry.Id)
				}

				for _, entry := range resp.Successful {
					lsn, err := pglogrepl.ParseLSN(*entry.Id)
					if err != nil {
						panic("TODO handle error: " + err.Error())
					}

					completed <- lsn
				}
			}
		}()
	}
	return completed
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
