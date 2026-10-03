package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// SevereAlarm reports whether the alarm kind requires an emergency stop of
// all fill activity in the affected zone.
func SevereAlarm(kind AlarmKind) bool {
	return kind == AlarmPressure || kind == AlarmLeak
}

type EmergencyStopState string

const (
	EmergencyActive   EmergencyStopState = "active"
	EmergencyResolved EmergencyStopState = "resolved"
)

type EmergencyActionStatus string

const (
	// EmergencyActionPending means the close intent is journaled but no
	// receipt outcome has been recorded yet.
	EmergencyActionPending EmergencyActionStatus = "pending"
	// EmergencyActionConfirmed means the valve close receipt was confirmed.
	EmergencyActionConfirmed EmergencyActionStatus = "closed_confirmed"
	// EmergencyActionUncertain means the close receipt is unknown; the
	// circuit stays fault-occupied until explicit recovery.
	EmergencyActionUncertain EmergencyActionStatus = "receipt_uncertain"
)

// EmergencyAction records the disposal of one fill session affected by an
// emergency stop. The command identity is fixed before the dangerous close
// is issued so replays and repeated alarms never re-issue it.
type EmergencyAction struct {
	SessionID  uuid.UUID             `json:"session_id"`
	VesselID   string                `json:"vessel_id"`
	CircuitID  string                `json:"circuit_id"`
	ValveID    string                `json:"valve_id"`
	CommandID  uuid.UUID             `json:"command_id"`
	Status     EmergencyActionStatus `json:"status"`
	Detail     string                `json:"detail,omitempty"`
	RecordedAt time.Time             `json:"recorded_at"`
}

type EmergencyStop struct {
	ID        uuid.UUID          `json:"id"`
	AlarmID   uuid.UUID          `json:"alarm_id"`
	Zone      string             `json:"zone"`
	Kind      AlarmKind          `json:"kind"`
	State     EmergencyStopState `json:"state"`
	Actions   []EmergencyAction  `json:"actions"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
}

func NewEmergencyStop(alarmID uuid.UUID, zone string, kind AlarmKind, now time.Time) (EmergencyStop, error) {
	if alarmID == uuid.Nil || zone == "" {
		return EmergencyStop{}, errors.New("emergency stop alarm and zone are required")
	}
	if !SevereAlarm(kind) {
		return EmergencyStop{}, errors.New("alarm kind does not require an emergency stop")
	}
	return EmergencyStop{
		ID:        uuid.New(),
		AlarmID:   alarmID,
		Zone:      zone,
		Kind:      kind,
		State:     EmergencyActive,
		CreatedAt: now.UTC(),
		UpdatedAt: now.UTC(),
	}, nil
}

// UpsertAction records or replaces the disposal action for a session.
func (s EmergencyStop) UpsertAction(action EmergencyAction, now time.Time) EmergencyStop {
	for i, existing := range s.Actions {
		if existing.SessionID == action.SessionID {
			s.Actions[i] = action
			s.UpdatedAt = now.UTC()
			return s
		}
	}
	s.Actions = append(s.Actions, action)
	s.UpdatedAt = now.UTC()
	return s
}

func (s EmergencyStop) Action(sessionID uuid.UUID) (EmergencyAction, bool) {
	for _, action := range s.Actions {
		if action.SessionID == sessionID {
			return action, true
		}
	}
	return EmergencyAction{}, false
}

func (s EmergencyStop) Resolve(now time.Time) (EmergencyStop, error) {
	if s.State != EmergencyActive {
		return EmergencyStop{}, errors.New("emergency stop is not active")
	}
	s.State = EmergencyResolved
	s.UpdatedAt = now.UTC()
	return s, nil
}

type EmergencyStopProgress struct {
	Total     int `json:"total"`
	Confirmed int `json:"confirmed"`
	Uncertain int `json:"uncertain"`
	Pending   int `json:"pending"`
}

func (s EmergencyStop) Progress() EmergencyStopProgress {
	progress := EmergencyStopProgress{Total: len(s.Actions)}
	for _, action := range s.Actions {
		switch action.Status {
		case EmergencyActionConfirmed:
			progress.Confirmed++
		case EmergencyActionUncertain:
			progress.Uncertain++
		default:
			progress.Pending++
		}
	}
	return progress
}
