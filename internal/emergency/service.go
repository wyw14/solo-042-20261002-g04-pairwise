package emergency

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/fill"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

const (
	Partition         = "emergency"
	EventTriggered    = "emergency.triggered"
	EventCloseIntended = "emergency.close_intended"
	EventCloseConfirmed = "emergency.close_confirmed"
	EventCloseUncertain = "emergency.close_uncertain"
	EventResolved     = "emergency.resolved"
)

var (
	ErrNotFound          = errors.New("emergency stop not found")
	ErrDisposalPending   = errors.New("emergency stop disposal is still in progress")
	ErrUncertainReceipts = errors.New("emergency stop has unconfirmed valve receipts")
)

// Service coordinates emergency stops: when a severe alarm is raised it
// locks the affected zone against new fills, issues close commands to the
// valves of every active session in the zone, and journals each step so a
// repeated alarm or a process restart never re-issues a dangerous command.
type Service struct {
	mu           sync.RWMutex
	stops        map[uuid.UUID]model.EmergencyStop
	activeByZone map[string]uuid.UUID
	store        *journal.Store
	vessels      *vessel.Registry
	fills        *fill.Coordinator
	routes       *valve.RouteService
	now          func() time.Time
}

func NewService(store *journal.Store, vessels *vessel.Registry, fills *fill.Coordinator, routes *valve.RouteService, now func() time.Time) (*Service, error) {
	if store == nil || vessels == nil || fills == nil || routes == nil {
		return nil, errors.New("emergency service dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &Service{
		stops:        make(map[uuid.UUID]model.EmergencyStop),
		activeByZone: make(map[string]uuid.UUID),
		store:        store,
		vessels:      vessels,
		fills:        fills,
		routes:       routes,
		now:          now,
	}, nil
}

// HandleAlarm disposes of the zone affected by a severe alarm. It is
// idempotent per zone: while a stop is active for the zone, repeated alarms
// return the existing record without issuing new commands.
func (s *Service) HandleAlarm(ctx context.Context, alarm model.Alarm) (model.EmergencyStop, error) {
	if !model.SevereAlarm(alarm.Kind) {
		return model.EmergencyStop{}, nil
	}
	vessel, ok := s.vessels.Get(alarm.VesselID)
	if !ok {
		return model.EmergencyStop{}, errors.New("alarm vessel is not registered")
	}
	zone := vessel.Zone

	s.mu.Lock()
	if id, active := s.activeByZone[zone]; active {
		stop := s.stops[id]
		s.mu.Unlock()
		return stop, nil
	}
	stop, err := model.NewEmergencyStop(alarm.ID, zone, alarm.Kind, s.now())
	if err != nil {
		s.mu.Unlock()
		return model.EmergencyStop{}, err
	}
	if err := s.appendLocked(EventTriggered, stop.ID, stop); err != nil {
		s.mu.Unlock()
		return model.EmergencyStop{}, err
	}
	s.stops[stop.ID] = stop
	s.activeByZone[zone] = stop.ID
	s.mu.Unlock()

	for _, session := range s.zoneSessions(zone) {
		s.closeSession(ctx, stop.ID, session)
	}
	stop, _ = s.Get(stop.ID)
	return stop, nil
}

// ZoneLocked reports whether new fills are blocked in the zone.
func (s *Service) ZoneLocked(zone string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, locked := s.activeByZone[zone]
	return locked
}

func (s *Service) Get(id uuid.UUID) (model.EmergencyStop, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stop, ok := s.stops[id]
	return stop, ok
}

func (s *Service) List() []model.EmergencyStop {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.EmergencyStop, 0, len(s.stops))
	for _, stop := range s.stops {
		result = append(result, stop)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

// Resolve lifts the zone lock once the area is safe. Stops with sessions
// whose close receipt is uncertain require acknowledgeUncertain so the
// operator explicitly accepts the residual risk after inspection.
func (s *Service) Resolve(id uuid.UUID, acknowledgeUncertain bool) (model.EmergencyStop, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stop, ok := s.stops[id]
	if !ok {
		return model.EmergencyStop{}, ErrNotFound
	}
	if stop.State != model.EmergencyActive {
		return stop, nil
	}
	progress := stop.Progress()
	if progress.Pending > 0 {
		return model.EmergencyStop{}, ErrDisposalPending
	}
	if progress.Uncertain > 0 && !acknowledgeUncertain {
		return model.EmergencyStop{}, ErrUncertainReceipts
	}
	resolved, err := stop.Resolve(s.now())
	if err != nil {
		return model.EmergencyStop{}, err
	}
	if err := s.appendLocked(EventResolved, resolved.ID, resolved); err != nil {
		return model.EmergencyStop{}, err
	}
	s.stops[id] = resolved
	delete(s.activeByZone, stop.Zone)
	return resolved, nil
}

// zoneSessions lists active fill sessions whose vessel sits in the zone.
func (s *Service) zoneSessions(zone string) []model.FillSession {
	var targets []model.FillSession
	for _, session := range s.fills.List() {
		if !session.Active() {
			continue
		}
		vessel, ok := s.vessels.Get(session.VesselID)
		if !ok || vessel.Zone != zone {
			continue
		}
		targets = append(targets, session)
	}
	return targets
}

// closeSession disposes of one session: the close intent is journaled
// before the dangerous command is issued, and the outcome is journaled
// after, so a crash in between is recovered as receipt_uncertain rather
// than re-issued.
func (s *Service) closeSession(ctx context.Context, stopID uuid.UUID, session model.FillSession) {
	action := model.EmergencyAction{
		SessionID:  session.ID,
		VesselID:   session.VesselID,
		CircuitID:  session.CircuitID,
		Status:     model.EmergencyActionPending,
		RecordedAt: s.now().UTC(),
	}
	route, routed := s.routes.Route(session.ID)
	if routed {
		action.ValveID = route.ValveID
		action.CommandID = route.CloseCommandID
	}
	if err := s.recordAction(stopID, EventCloseIntended, action); err != nil {
		action.Status = model.EmergencyActionUncertain
		action.Detail = "close command withheld: " + err.Error()
		s.terminate(session, false)
		s.storeAction(stopID, action)
		return
	}

	confirmed := false
	detail := ""
	switch {
	case !routed:
		detail = "no tracked valve route for session"
	default:
		if err := s.routes.Close(ctx, session.ID); err != nil {
			detail = err.Error()
		} else {
			confirmed = true
		}
	}

	if confirmed {
		action.Status = model.EmergencyActionConfirmed
	} else {
		action.Status = model.EmergencyActionUncertain
	}
	action.Detail = detail
	if err := s.terminate(session, confirmed); err != nil {
		if action.Detail != "" {
			action.Detail += "; "
		}
		action.Detail += "session termination: " + err.Error()
	}
	action.RecordedAt = s.now().UTC()
	event := EventCloseUncertain
	if confirmed {
		event = EventCloseConfirmed
	}
	if err := s.recordAction(stopID, event, action); err != nil {
		// The command was already issued; keep the in-memory record so the
		// outcome survives until the journal is readable again.
		s.storeAction(stopID, action)
	}
}

// terminate fails the session in the fill coordinator, releasing the
// circuit only when the close receipt was confirmed.
func (s *Service) terminate(session model.FillSession, confirmed bool) error {
	_, err := s.fills.TerminateEmergency(session.ID, "emergency stop: "+session.CircuitID, confirmed)
	return err
}

// recordAction journals the action and applies it to the in-memory stop.
func (s *Service) recordAction(stopID uuid.UUID, kind string, action model.EmergencyAction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendLocked(kind, stopID, action); err != nil {
		return err
	}
	s.applyActionLocked(stopID, action)
	return nil
}

// storeAction applies an action without journaling; used when the journal
// append failed so progress remains queryable in memory.
func (s *Service) storeAction(stopID uuid.UUID, action model.EmergencyAction) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyActionLocked(stopID, action)
}

func (s *Service) applyActionLocked(stopID uuid.UUID, action model.EmergencyAction) {
	stop, ok := s.stops[stopID]
	if !ok {
		return
	}
	s.stops[stopID] = stop.UpsertAction(action, s.now())
}

func (s *Service) appendLocked(kind string, stopID uuid.UUID, payload any) error {
	event, err := model.NewEvent(Partition, kind, stopID.String(), payload, s.now())
	if err != nil {
		return err
	}
	_, err = s.store.Append(event)
	return err
}
