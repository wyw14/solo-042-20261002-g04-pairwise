// Package emergency implements the emergency stop-fill response. When a
// severe alarm (high pressure or nitrogen leak) is raised, every active fill
// session in the affected area is isolated by replaying its pre-issued close
// command, new fills in that area are blocked, and the handling progress is
// journaled so duplicate alarms and process restarts can never drive a
// second dangerous command to the same valve.
package emergency

import (
	"time"

	"github.com/google/uuid"

	"github.com/wyw14/cry-98/internal/model"
)

// Session statuses recorded on a stop record. They mirror the fill
// coordinator's emergency close outcomes.
const (
	OutcomeConfirmed = "confirmed_close"
	OutcomeUncertain = "close_uncertain"
	OutcomeInactive  = "session_inactive"
)

// Stop overall statuses.
const (
	StatusCompleted        = "completed"
	StatusNoActiveSessions = "no_active_sessions"
	StatusInProgress       = "in_progress"
)

// SessionOutcome records what happened to one active fill session found in
// the stopped area. Confirmed closes and uncertain receipts are kept as
// separate lists so operators can see which circuits still need an explicit
// isolation recovery.
type SessionOutcome struct {
	SessionID         uuid.UUID `json:"session_id"`
	VesselID          string    `json:"vessel_id"`
	CircuitID         string    `json:"circuit_id"`
	CircuitGeneration uint64    `json:"circuit_generation"`
	ValveID           string    `json:"valve_id"`
	CommandID         uuid.UUID `json:"command_id"`
	Status            string    `json:"status"`
	Detail            string    `json:"detail,omitempty"`
	ClosedAt          time.Time `json:"closed_at,omitempty"`
}

// StopRecord is the queryable progress of one emergency stop, keyed by the
// alarm that triggered it.
type StopRecord struct {
	ID          uuid.UUID        `json:"id"`
	AlarmID     uuid.UUID        `json:"alarm_id"`
	VesselID    string           `json:"vessel_id"`
	Zone        string           `json:"zone"`
	Kind        model.AlarmKind  `json:"kind"`
	Status      string           `json:"status"`
	Sessions    []SessionOutcome `json:"sessions"`
	TriggeredAt time.Time        `json:"triggered_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// Confirmed returns the sessions whose close receipts were executed.
func (r StopRecord) Confirmed() []SessionOutcome {
	return r.filter(OutcomeConfirmed)
}

// Uncertain returns the sessions without a trustworthy close receipt. Their
// circuits stay retained as faulted until manually recovered.
func (r StopRecord) Uncertain() []SessionOutcome {
	return r.filter(OutcomeUncertain)
}

func (r StopRecord) filter(status string) []SessionOutcome {
	result := make([]SessionOutcome, 0)
	for _, outcome := range r.Sessions {
		if outcome.Status == status {
			result = append(result, outcome)
		}
	}
	return result
}
