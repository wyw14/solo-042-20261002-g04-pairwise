package emergency

import (
	"encoding/json"
	"sort"

	"github.com/google/uuid"
)

func sortOutcomes(outcomes []SessionOutcome) {
	sort.Slice(outcomes, func(i, j int) bool {
		if !outcomes[i].ClosedAt.Equal(outcomes[j].ClosedAt) {
			return outcomes[i].ClosedAt.Before(outcomes[j].ClosedAt)
		}
		return outcomes[i].SessionID.String() < outcomes[j].SessionID.String()
	})
}

// Recovery rebuilds emergency stop state from the journal after a restart.
// It never talks to a valve: replay is purely bookkeeping. Zone blocks are
// reinstated unless the zone was later resumed, and circuits tied to
// uncertain close receipts are restored as faulted reservations unless the
// fault was later explicitly recovered.
type Recovery struct {
	service *Service
}

func NewRecovery(service *Service) *Recovery {
	return &Recovery{service: service}
}

type progress struct {
	record      StopRecord
	started     bool
	outcomes    map[uuid.UUID]SessionOutcome
	completed   bool
	finalStatus string
}

func (r *Recovery) Restore() error {
	events, err := r.service.store.Events(partition, 0)
	if err != nil {
		return err
	}
	streams := make(map[uuid.UUID]*progress)
	var order []uuid.UUID
	resumedZones := make(map[string]struct{})
	recoveredSessions := make(map[uuid.UUID]struct{})

	ensure := func(alarmID uuid.UUID) *progress {
		prog, ok := streams[alarmID]
		if !ok {
			prog = &progress{
				record:   StopRecord{AlarmID: alarmID, Sessions: []SessionOutcome{}},
				outcomes: make(map[uuid.UUID]SessionOutcome),
			}
			streams[alarmID] = prog
			order = append(order, alarmID)
		}
		return prog
	}

	for _, event := range events {
		switch event.Kind {
		case eventTriggered:
			var payload triggeredPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			prog := ensure(payload.Stop.AlarmID)
			baseline := payload.Stop
			// Preserve outcomes already replayed; the trigger carries only
			// the baseline record metadata.
			baseline.Sessions = prog.record.Sessions
			prog.record = baseline
			prog.started = true
		case eventSession:
			var payload sessionPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			alarmID, err := uuid.Parse(event.AggregateID)
			if err != nil {
				return err
			}
			prog := ensure(alarmID)
			// De-dupe defensively so a duplicated/retried event can't add a
			// second entry for the same session.
			prog.outcomes[payload.Outcome.SessionID] = payload.Outcome
		case eventCompleted:
			var payload completedPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			prog := ensure(payload.AlarmID)
			prog.completed = true
			prog.finalStatus = payload.Status
			prog.record.ID = payload.ID
			prog.record.Zone = payload.Zone
			prog.record.UpdatedAt = payload.UpdatedAt
		case eventZoneResumed:
			var payload zoneResumedPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			resumedZones[payload.Zone] = struct{}{}
		case eventFaultRecovered:
			var payload faultRecoveredPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			recoveredSessions[payload.SessionID] = struct{}{}
		}
	}

	r.service.mu.Lock()
	defer r.service.mu.Unlock()
	for _, alarmID := range order {
		prog := streams[alarmID]
		record := prog.record

		outcomes := make([]SessionOutcome, 0, len(prog.outcomes))
		for _, outcome := range prog.outcomes {
			outcomes = append(outcomes, outcome)
		}
		sortOutcomes(outcomes)
		record.Sessions = outcomes

		if prog.completed {
			record.Status = prog.finalStatus
		} else if prog.started {
			record.Status = StatusInProgress
		}
		r.service.records[record.AlarmID] = record

		// Re-impose the new-fill block unless the area was later resumed.
		if record.Zone != "" {
			if _, resumed := resumedZones[record.Zone]; !resumed {
				r.service.fills.Coordinator().BlockZone(record.Zone)
			}
		}
		// Uncertain receipts keep their circuits faulted and occupied,
		// unless the fault was later explicitly recovered.
		for _, outcome := range record.Sessions {
			if outcome.Status != OutcomeUncertain {
				continue
			}
			if _, recovered := recoveredSessions[outcome.SessionID]; recovered {
				continue
			}
			r.service.fills.Coordinator().RestoreFaultedCircuit(
				outcome.CircuitID,
				outcome.CircuitGeneration,
				outcome.SessionID,
				outcome.VesselID,
			)
		}
	}
	return nil
}
