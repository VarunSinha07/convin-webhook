// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	// 1. Fast path: check Redis distributed lock / key cache
	if s.rdb != nil {
		set, err := s.rdb.SetNX(ctx, "dedup:event:"+evt.EventID, "1", 24*time.Hour).Result()
		if err != nil {
			s.log.Warn("redis deduplication check failed, falling back to postgres", "event_id", evt.EventID, "err", err)
		} else if !set {
			s.log.Info("duplicate delivery ignored by redis fast path", "event_id", evt.EventID)
			return nil
		}
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// 2. Durable safety path: Postgres transaction with UNIQUE constraint
	if err := s.store.IngestTransaction(ctx, rec); err != nil {
		if errors.Is(err, store.ErrDuplicateEvent) {
			s.log.Info("duplicate delivery ignored by store", "event_id", evt.EventID)
			return nil
		}
		return err
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		go func() {
			if err := s.processRecording(ctx, rec); err != nil {
				// TODO: handle
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
