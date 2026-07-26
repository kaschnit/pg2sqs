package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/kaschnit/pg2sqs/internal/checkpoint"
	"github.com/kaschnit/pg2sqs/internal/engine"
	"github.com/kaschnit/pg2sqs/internal/publish"
	"github.com/kaschnit/pg2sqs/internal/replication"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	"github.com/spf13/cobra"
)

func main() {
	var (
		cfg     Config
		cfgFile string
	)

	k := koanf.New(".")

	cmd := &cobra.Command{
		Use:   "pg2sqs",
		Short: "Start pg2sqs CDC",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Load cfg file
			if cfgFile != "" {
				if err := k.Load(file.Provider(cfgFile), yaml.Parser()); err != nil {
					return err
				}
			}

			// Override cfg file with env vars
			if err := k.Load(env.Provider(".", env.Opt{
				Prefix: envVarPrefix,
				TransformFunc: func(key, val string) (string, any) {
					return envVarToCfg(key), val
				},
			}), nil); err != nil {
				return err
			}

			// Override with flags
			if err := k.Load(posflag.Provider(cmd.Flags(), ".", k), nil); err != nil {
				return err
			}

			// Unmarshal config
			if err := k.Unmarshal("", &cfg); err != nil {
				return err
			}

			if err := cfg.Validate(); err != nil {
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd.Context(), cfg)
		},
	}

	// Config file
	cmd.Flags().StringVar(&cfgFile, "config", "", "pg2sqs config YAML file")

	// Logging
	cmd.Flags().IntP("verbosity", "v", 0, "Logging verbosity")
	// SQS queue
	cmd.Flags().String("sqs.queue.queue_url", "", "SQS queue url for publishing")
	// SQS publishing
	cmd.Flags().Int("sqs.publishing.workers", 1, "Number of SQS batch publishing workers")
	cmd.Flags().Int("sqs.publishing.max_messages", 1,
		"Max messages published per SQS SendMessage/SendMessageBatch request")
	cmd.Flags().Duration("sqs.publishing.flush_interval", 1*time.Second,
		"SQS batch publishing flush interval")
	// Postgres
	cmd.Flags().String("pg.connection.user", "", "Postgres user")
	cmd.Flags().String("pg.connection.password", "", "Postgres password")
	cmd.Flags().String("pg.connection.host", "", "Postgres host")
	cmd.Flags().Int("pg.connection.port", 5432, "Postgres port")
	cmd.Flags().String("pg.connection.database", "", "Postgres database")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg Config) error {
	slog.SetLogLoggerLevel(LogLevel(cfg.Verbosity))

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	pgConnection, err := pgconn.Connect(context.Background(), cfg.PG.Connection.ConnectionString())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	stream := replication.NewStream(pgConnection)

	publisher := publish.NewBatcher(sqs.NewFromConfig(awsCfg), cfg.SQS.Queue.QueueURL,
		publish.WithWorkers(cfg.SQS.Publishing.Workers),
		publish.WithFlushInterval(cfg.SQS.Publishing.FlushInterval),
		publish.WithMaxMessages(cfg.SQS.Publishing.MaxMessages))

	pipeline := engine.NewPipeline(stream, checkpoint.NewTracker(), publisher)
	pipeline.Run(ctx)

	return nil
}
