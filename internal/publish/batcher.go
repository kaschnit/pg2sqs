package publish

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
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
	panic("TODO")
}
