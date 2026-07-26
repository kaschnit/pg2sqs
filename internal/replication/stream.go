package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/kaschnit/pg2sqs/internal/event"
)

// Stream is a Postgres replication stream.
type Stream struct {
	mu         sync.Mutex
	conn       *pgconn.PgConn
	flushedLSN pglogrepl.LSN
}

func NewStream(conn *pgconn.PgConn) *Stream {
	return &Stream{conn: conn}
}

// Start returns a channel containing changes from the stream.
func (s *Stream) Start(ctx context.Context) <-chan event.Change {
	changes := make(chan event.Change, 1000)
	go func() {
		defer close(changes)

		relations := make(map[uint32]*pglogrepl.RelationMessage)
		inStream := false

		for {
			if ctx.Err() != nil {
				return
			}

			rawMsg, err := s.conn.ReceiveMessage(ctx)
			if err != nil {
				if pgconn.Timeout(err) {
					continue // TODO handle
				}
				// TODO handle
				panic(fmt.Sprintf("replication receive failed: %v", err))
			}

			var copyData *pgproto3.CopyData
			switch msg := rawMsg.(type) {
			case *pgproto3.ErrorResponse:
				// TODO handle
				panic(fmt.Sprintf("received Postgres WAL error: %v", msg))
			case *pgproto3.CopyData:
				if len(msg.Data) == 0 {
					continue // TODO handle
				}
				copyData = msg
			default:
				// TODO handle - ignore since we only care about CopyData?
				continue
			}

			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					continue // TODO handle
				}

				if pkm.ReplyRequested {
					s.mu.Lock()
					if err := s.sendStandbyStatusUpdate(ctx, s.flushedLSN); err != nil {
						continue // TODO handle
					}
					s.mu.Unlock()
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					continue // TODO handle
				}

				logicalMsg, err := pglogrepl.ParseV2(xld.WALData, inStream)
				if err != nil {
					logicalMsg, err = pglogrepl.Parse(xld.WALData)
					if err != nil {
						continue // TODO handle
					}
				}

				switch msg := logicalMsg.(type) {
				case *pglogrepl.StreamStartMessageV2:
					inStream = true
				case *pglogrepl.StreamStopMessageV2:
					inStream = false
				case *pglogrepl.BeginMessage:
					// Non-streaming transaction begin. Nothing to cache here.
				case *pglogrepl.CommitMessage:
					// Non-streaming transaction commit. Keep relation metadata around.
				case *pglogrepl.RelationMessage:
					relations[msg.RelationID] = msg
				case *pglogrepl.RelationMessageV2:
					relations[msg.RelationID] = &msg.RelationMessage
				case *pglogrepl.TypeMessage:
				case *pglogrepl.TypeMessageV2:
				case *pglogrepl.OriginMessage:
				case *pglogrepl.LogicalDecodingMessage:
				case *pglogrepl.LogicalDecodingMessageV2:
				case *pglogrepl.InsertMessage:
					rel, ok := relations[msg.RelationID]
					if !ok {
						continue // TODO handle
					}

					select {
					case <-ctx.Done():
						return
					case changes <- event.Change{
						LSN:       xld.WALStart,
						Schema:    rel.Namespace,
						Table:     rel.RelationName,
						Action:    event.ActionInsert,
						Timestamp: xld.ServerTime,
						Data:      decodeTuple(rel, msg.Tuple),
					}:
					}

				case *pglogrepl.InsertMessageV2:
					rel, ok := relations[msg.RelationID]
					if !ok {
						continue // TODO handle
					}

					select {
					case <-ctx.Done():
						return
					case changes <- event.Change{
						LSN:       xld.WALStart,
						Schema:    rel.Namespace,
						Table:     rel.RelationName,
						Action:    event.ActionInsert,
						Timestamp: xld.ServerTime,
						Data:      decodeTuple(rel, msg.Tuple),
					}:
					}

				case *pglogrepl.UpdateMessage:
					rel, ok := relations[msg.RelationID]
					if !ok {
						continue // TODO handle
					}

					change := event.Change{
						LSN:       xld.WALStart,
						Schema:    rel.Namespace,
						Table:     rel.RelationName,
						Action:    event.ActionUpdate,
						Timestamp: xld.ServerTime,
						Data:      decodeTuple(rel, msg.NewTuple),
						OldData:   decodeTuple(rel, msg.OldTuple),
					}

					select {
					case <-ctx.Done():
						return
					case changes <- change:
					}

				case *pglogrepl.UpdateMessageV2:
					rel, ok := relations[msg.RelationID]
					if !ok {
						continue // TODO handle
					}

					change := event.Change{
						LSN:       xld.WALStart,
						Schema:    rel.Namespace,
						Table:     rel.RelationName,
						Action:    event.ActionUpdate,
						Timestamp: xld.ServerTime,
						Data:      decodeTuple(rel, msg.NewTuple),
						OldData:   decodeTuple(rel, msg.OldTuple),
					}

					select {
					case <-ctx.Done():
						return
					case changes <- change:
					}

				case *pglogrepl.DeleteMessage:
					rel, ok := relations[msg.RelationID]
					if !ok {
						continue // TODO handle
					}

					change := event.Change{
						LSN:       xld.WALStart,
						Schema:    rel.Namespace,
						Table:     rel.RelationName,
						Action:    event.ActionDelete,
						Timestamp: xld.ServerTime,
						OldData:   decodeTuple(rel, msg.OldTuple),
					}

					select {
					case <-ctx.Done():
						return
					case changes <- change:
					}

				case *pglogrepl.DeleteMessageV2:
					rel, ok := relations[msg.RelationID]
					if !ok {
						continue
					}

					change := event.Change{
						LSN:       xld.WALStart,
						Schema:    rel.Namespace,
						Table:     rel.RelationName,
						Action:    event.ActionDelete,
						Timestamp: xld.ServerTime,
						OldData:   decodeTuple(rel, msg.OldTuple),
					}

					select {
					case <-ctx.Done():
						return
					case changes <- change:
					}
				}
			}
		}
	}()

	return changes
}

// Flush flushes the replication watermark to lsn.
func (s *Stream) Flush(ctx context.Context, lsn pglogrepl.LSN) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if lsn <= s.flushedLSN {
		return nil
	}

	if err := s.sendStandbyStatusUpdate(ctx, lsn); err != nil {
		return err
	}

	s.flushedLSN = lsn

	return nil
}

func (s *Stream) sendStandbyStatusUpdate(ctx context.Context, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ClientTime:       time.Now(),
	})
}
