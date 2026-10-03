package emergency

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wyw14/cry-98/internal/fill"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/vessel"
)

const partition = "emergency"

// Journal event kinds.
const (
	eventTriggered      = "emergency.stop_triggered"
	eventSession        = "emergency.session_outcome"
	eventCompleted      = "emergency.stop_completed"
	eventZoneResumed    = "emergency.zone_resumed"
	eventFaultRecovered = "emergency.fault_recovered"
)

// EventAppender persists emergency progress. *journal.Store satisfies it.
type EventAppender interface {
	Append(model.Event) (model.Event, error)
	Events(partition string, after uint64) ([]model.Event, error)
}

var errNotSevere = errors.New("alarm does not require emergency stop")

type triggeredPayload struct {
	Stop        StopRecord `json:"stop"`
	TriggeredAt time.Time  `json:"triggered_at"`
}

type sessionPayload struct {
	StopID  uuid.UUID      `json:"stop_id"`
	Outcome SessionOutcome `json:"outcome"`
}

type completedPayload struct {
	ID        uuid.UUID `json:"id"`
	AlarmID   uuid.UUID `json:"alarm_id"`
	Zone      string    `json:"zone"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

type zoneResumedPayload struct {
	Zone      string    `json:"zone"`
	ResumedAt time.Time `json:"resumed_at"`
}

type faultRecoveredPayload struct {
	SessionID   uuid.UUID `json:"session_id"`
	RecoveredAt time.Time `json:"recovered_at"`
}

// Service coordinates emergency stop-fill responses. It is safe for
// concurrent use and serializes handling so a burst of duplicate alarms can
// never drive a second close command to the same valve.
type Service struct {
	mu      sync.Mutex
	records map[uuid.UUID]StopRecord
	fills   *fill.Service
	vessels *vessel.Registry
	store   EventAppender
	now     func() time.Time
}

func NewService(fills *fill.Service, vessels *vessel.Registry, store EventAppender, now func() time.Time) (*Service, error) {
	if fills == nil || vessels == nil || store == nil {
		return nil, errors.New("emergency service dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &Service{
		records: make(map[uuid.UUID]StopRecord),
		fills:   fills,
		vessels: vessels,
		store:   store,
		now:     now,
	}, nil
}

// Hook adapts HandleAlarm to the alarm service's SevereHook signature.
// Non-severe alarms are ignored rather than reported as failures.
func (s *Service) Hook(ctx context.Context, alarm model.Alarm) error {
	if !alarm.Severe() {
		return nil
	}
	_, err := s.HandleAlarm(ctx, alarm)
	return err
}

// HandleAlarm runs the emergency response for a severe alarm. Non-severe
// alarms are ignored. A repeated alarm (same alarm ID) returns the existing
// record without issuing any command.
func (s *Service) HandleAlarm(ctx context.Context, alarm model.Alarm) (StopRecord, error) {
	if !alarm.Severe() {
		return StopRecord{}, errNotSevere
	}
	if err := ctx.Err(); err != nil {
		return StopRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[alarm.ID]; ok {
		return existing, nil
	}

	zone := ""
	if vesselValue, ok := s.vessels.Get(alarm.VesselID); ok {
		zone = vesselValue.Zone
	}
	record := StopRecord{
		ID:          uuid.New(),
		AlarmID:     alarm.ID,
		VesselID:    alarm.VesselID,
		Zone:        zone,
		Kind:        alarm.Kind,
		Status:      StatusInProgress,
		Sessions:    []SessionOutcome{},
		TriggeredAt: s.now().UTC(),
		UpdatedAt:   s.now().UTC(),
	}

	// Block the area first, before any command goes out, so a fill starting
	// concurrently can never slip into the zone while isolation is underway.
	if zone != "" {
		s.fills.Coordinator().BlockZone(zone)
	}

	if err := s.append(eventTriggered, alarm.ID.String(), triggeredPayload{
		Stop:        record,
		TriggeredAt: record.TriggeredAt,
	}); err != nil {
		return StopRecord{}, err
	}
	s.records[alarm.ID] = record

	targets := s.fills.Coordinator().ActiveSessionsInZone(zone)
	reason := "emergency stop: " + string(alarm.Kind)
	for _, session := range targets {
		outcome := s.isolate(ctx, session, reason)
		record.Sessions = append(record.Sessions, outcome)
		record.UpdatedAt = s.now().UTC()
		if err := s.append(eventSession, alarm.ID.String(), sessionPayload{
			StopID:  record.ID,
			Outcome: outcome,
		}); err != nil {
			s.records[alarm.ID] = record
			return StopRecord{}, err
		}
		s.records[alarm.ID] = record
	}

	if len(targets) == 0 {
		record.Status = StatusNoActiveSessions
	} else {
		record.Status = StatusCompleted
	}
	record.UpdatedAt = s.now().UTC()
	if err := s.append(eventCompleted, alarm.ID.String(), completedPayload{
		ID:        record.ID,
		AlarmID:   record.AlarmID,
		Zone:      record.Zone,
		Status:    record.Status,
		UpdatedAt: record.UpdatedAt,
	}); err != nil {
		s.records[alarm.ID] = record
		return StopRecord{}, err
	}
	s.records[alarm.ID] = record
	return record, nil
}

func (s *Service) isolate(ctx context.Context, session model.FillSession, reason string) SessionOutcome {
	result := s.fills.Coordinator().EmergencyClose(ctx, session.ID, reason)
	outcome := SessionOutcome{
		SessionID:         result.SessionID,
		VesselID:          result.VesselID,
		CircuitID:         result.CircuitID,
		CircuitGeneration: result.CircuitGeneration,
		ValveID:           result.ValveID,
		CommandID:         result.CommandID,
		Detail:            result.Detail,
	}
	switch result.Status {
	case fill.EmergencyCloseConfirmed:
		outcome.Status = OutcomeConfirmed
		outcome.ClosedAt = s.now().UTC()
	case fill.EmergencyCloseInactive:
		outcome.Status = OutcomeInactive
	default:
		outcome.Status = OutcomeUncertain
		outcome.ClosedAt = s.now().UTC()
	}
	return outcome
}

func (s *Service) append(kind, aggregateID string, payload any) error {
	event, err := model.NewEvent(partition, kind, aggregateID, payload, s.now())
	if err != nil {
		return err
	}
	_, err = s.store.Append(event)
	return err
}

// Get returns the progress record for one triggering alarm.
func (s *Service) Get(alarmID uuid.UUID) (StopRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[alarmID]
	return record, ok
}

// List returns all emergency stop records, oldest trigger first.
func (s *Service) List() []StopRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]StopRecord, 0, len(s.records))
	for _, record := range s.records {
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].TriggeredAt.Before(records[j].TriggeredAt)
	})
	return records
}

// ResumeZone lifts the new-fill block for a zone once the area has been made
// safe. The decision is journaled so a restart does not silently re-impose
// the block. It does not release faulted circuits; those still require the
// explicit RecoverFault action.
func (s *Service) ResumeZone(ctx context.Context, zone string) error {
	if zone == "" {
		return errors.New("zone is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.append(eventZoneResumed, zone, zoneResumedPayload{
		Zone:      zone,
		ResumedAt: s.now().UTC(),
	}); err != nil {
		return err
	}
	s.fills.Coordinator().ResumeZone(zone)
	return nil
}

// RecoverFault explicitly clears a faulted circuit held by a fill session
// whose close receipt was uncertain. It works after a restart when the
// in-memory session is gone, and the release is journaled so it survives
// further restarts.
func (s *Service) RecoverFault(ctx context.Context, sessionID uuid.UUID) error {
	if sessionID == uuid.Nil {
		return errors.New("session id is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.fills.Coordinator().FaultedCircuitForSession(sessionID); !ok {
		return errors.New("no faulted circuit held by that session")
	}
	if err := s.append(eventFaultRecovered, sessionID.String(), faultRecoveredPayload{
		SessionID:   sessionID,
		RecoveredAt: s.now().UTC(),
	}); err != nil {
		return err
	}
	if _, ok := s.fills.Coordinator().RecoverFaultedBySession(sessionID); !ok {
		return errors.New("faulted circuit vanished during recovery")
	}
	return nil
}
