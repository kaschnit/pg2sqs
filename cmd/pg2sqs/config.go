package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	errInvalidCfg = errors.New("invalid config")
)

type invalidValue[T any] struct {
	name string
	got  T
	want string
}

func (iv *invalidValue[T]) Error() string {
	if iv.want == "" {
		return fmt.Sprintf("invalid %s '%v'", iv.name, iv.got)
	}
	return fmt.Sprintf("invalid %s '%v'; expected %s", iv.name, iv.got, iv.want)
}

type Config struct {
	Verbosity Verbosity `koanf:"verbosity"`
	SQS       SQSConfig `koanf:"sqs"`
	PG        PGConfig  `koanf:"pg"`
}

func (cfg *Config) Validate() error {
	errs := errors.Join(
		cfg.SQS.Validate(),
		cfg.PG.Validate(),
	)
	if errs != nil {
		return fmt.Errorf("%w: %w", errInvalidCfg, errs)
	}

	return nil
}

type SQSConfig struct {
	Queue      SQSQueueConfig      `koanf:"queue"`
	Publishing SQSPublishingConfig `koanf:"publishing"`
}

func (cfg *SQSConfig) Validate() error {
	return errors.Join(
		cfg.Queue.Validate(),
		cfg.Publishing.Validate(),
	)
}

type SQSQueueConfig struct {
	Region      string `koanf:"region"`
	EndpointURL string `koanf:"endpoint_url"`
	QueueURL    string `koanf:"queue_url"`
}

func (cfg *SQSQueueConfig) Validate() error {
	var errs error

	if cfg.QueueURL == "" {
		errs = errors.Join(errs, &invalidValue[string]{
			name: "queue_url",
			got:  cfg.QueueURL,
		})
	}

	return errs
}

type SQSPublishingConfig struct {
	Workers       int           `koanf:"workers"`
	MaxMessages   int           `koanf:"max_messages"`
	FlushInterval time.Duration `koanf:"flush_interval"`
}

func (cfg *SQSPublishingConfig) Validate() error {
	var errs error

	if cfg.Workers < 1 {
		errs = errors.Join(errs, &invalidValue[int]{
			name: "workers",
			got:  cfg.Workers,
			want: ">=1",
		})
	}
	if cfg.MaxMessages < 1 || cfg.MaxMessages > 10 {
		errs = errors.Join(errs, &invalidValue[int]{
			name: "max_messages",
			got:  cfg.MaxMessages,
			want: ">=1 and <=10",
		})
	}
	if cfg.FlushInterval < 0 {
		errs = errors.Join(errs, &invalidValue[int]{
			name: "flush_interval",
			got:  int(cfg.FlushInterval),
			want: ">=0",
		})
	}

	return errs
}

type PGConfig struct {
	Connection PGConnectionConfig `koanf:"connection"`
}

func (cfg *PGConfig) Validate() error {
	return errors.Join(
		cfg.Connection.Validate(),
	)
}

type PGConnectionConfig struct {
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	Database string `koanf:"database"`
}

func (cfg *PGConnectionConfig) Validate() error {
	var errs error

	if cfg.User == "" {
		errs = errors.Join(errs, &invalidValue[string]{
			name: "user",
			got:  cfg.User,
			want: "a user to be provided",
		})
	}
	if cfg.Password == "" {
		errs = errors.Join(errs, &invalidValue[string]{
			name: "password",
			got:  cfg.Password,
			want: "a password to be provided",
		})
	}
	if cfg.Host == "" {
		errs = errors.Join(errs, &invalidValue[string]{
			name: "host",
			got:  cfg.Host,
			want: "a host to be provided",
		})
	}
	if cfg.Port < 0 {
		errs = errors.Join(errs, &invalidValue[int]{
			name: "host",
			got:  cfg.Port,
			want: "a valid port",
		})
	}
	if cfg.Database == "" {
		errs = errors.Join(errs, &invalidValue[string]{
			name: "database",
			got:  cfg.Database,
			want: "a database to be provided",
		})
	}

	return errs
}

func (cfg *PGConnectionConfig) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
}

const envVarPrefix = "PG2SQS__"

func envVarToCfg(envVarName string) string {
	// Env var names are prefixed by envVarPrefix.
	envVarName = strings.TrimPrefix(envVarName, envVarPrefix)

	// Env var names are uppercase, conver to lowercase to match configs.
	envVarName = strings.ToLower(envVarName)

	// Env var nesting is separated by double '_' (i.e. '__').
	envVarName = strings.ReplaceAll(envVarName, "__", ".")

	return envVarName
}
