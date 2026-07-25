package event

import (
	"time"

	"github.com/bytedance/sonic"
	"github.com/jackc/pglogrepl"
)

type Action string

const (
	ActionInsert Action = "INSERT"
	ActionUpdate Action = "UPDATE"
	ActionDelete Action = "DELETE"
)

type Change struct {
	LSN       pglogrepl.LSN  `json:"lsn"`
	Schema    string         `json:"schema"`
	Table     string         `json:"table"`
	Action    Action         `json:"action"`
	Timestamp time.Time      `json:"timestamp"`
	Data      map[string]any `json:"data"`
	OldData   map[string]any `json:"old_data,omitempty"`
}

func (c Change) Marshal() ([]byte, error) {
	return sonic.Marshal(c)
}
