package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/kaschnit/pg2sqs/internal/checkpoint"
	"github.com/kaschnit/pg2sqs/internal/engine"
	"github.com/kaschnit/pg2sqs/internal/publish"
	"github.com/kaschnit/pg2sqs/internal/replication"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	var cfg Config
	flag.IntVar(&cfg.Verbosity, "v", 0, "Logging verbosity")
	flag.StringVar(&cfg.SQS.QueueURL, "sqs.queue_url", "", "SQS queue url for publishing")
	flag.Parse()

	// Set up logging
	slog.SetLogLoggerLevel(LogLevel(cfg.Verbosity))

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to load AWS config", slog.Any("err", err))
		os.Exit(1)
	}

	pipeline := engine.NewPipeline(replication.NewStream(), checkpoint.NewTracker(),
		// TODO configure batching options
		publish.NewBatcher(sqs.NewFromConfig(awsCfg), cfg.SQS.QueueURL))

	pipeline.Start(ctx)
}
