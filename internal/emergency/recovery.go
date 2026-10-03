package emergency

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

// Restore rebuilds the emergency stop state from the journal. It never
// issues valve commands: actions whose close intent was journaled without
// an outcome (the process stopped in between) are recovered as
// receipt_uncertain so the circuit keeps its fault occupation and no
// dangerous command is re-sent.
func (s *Service) Restore() error {
	events, err := s.store.Events(Partition, 0)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range events {
		switch event.Kind {
		case EventTriggered, EventResolved:
			var stop model.EmergencyStop
			if err := json.Unmarshal(event.Payload, &stop); err != nil {
				return err
			}
			s.stops[stop.ID] = stop
			if stop.State == model.EmergencyActive {
				s.activeByZone[stop.Zone] = stop.ID
			} else {
				delete(s.activeByZone, stop.Zone)
			}
		case EventCloseIntended, EventCloseConfirmed, EventCloseUncertain:
			var action model.EmergencyAction
			if err := json.Unmarshal(event.Payload, &action); err != nil {
				return err
			}
			stopID, parseErr := parseStopID(event.AggregateID)
			if parseErr != nil {
				return parseErr
			}
			stop, ok := s.stops[stopID]
			if !ok {
				return errors.New("emergency action journaled for unknown stop")
			}
			s.stops[stopID] = stop.UpsertAction(action, action.RecordedAt)
		}
	}
	for id, stop := range s.stops {
		recovered := stop
		for i, action := range stop.Actions {
			if action.Status != model.EmergencyActionPending {
				continue
			}
			stop.Actions[i].Status = model.EmergencyActionUncertain
			stop.Actions[i].Detail = "process restarted before the close receipt was recorded"
			recovered = stop
		}
		s.stops[id] = recovered
	}
	return nil
}

func parseStopID(raw string) (uuid.UUID, error) {
	return uuid.Parse(raw)
}
