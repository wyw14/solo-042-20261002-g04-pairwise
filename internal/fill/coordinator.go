package fill

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

var (
	ErrActiveSession = errors.New("vessel already has an active fill session")
	ErrZoneBlocked   = errors.New("fill zone is blocked by an emergency stop")
)

type EmergencyCloseStatus string

const (
	EmergencyCloseConfirmed EmergencyCloseStatus = "confirmed_close"
	EmergencyCloseUncertain EmergencyCloseStatus = "close_uncertain"
	EmergencyCloseInactive  EmergencyCloseStatus = "session_inactive"
)

type EmergencyCloseResult struct {
	SessionID         uuid.UUID
	VesselID          string
	CircuitID         string
	CircuitGeneration uint64
	ValveID           string
	CommandID         uuid.UUID
	Status            EmergencyCloseStatus
	Receipt           model.ValveReceipt
	Detail            string
}

type Coordinator struct {
	mu           sync.RWMutex
	sessions     map[uuid.UUID]model.FillSession
	active       map[string]uuid.UUID
	blockedZones map[string]struct{}
	circuits     *circuit.Manager
	circuitList  *circuit.Registry
	routes       *valve.RouteService
	retry        *RetryService
	vessels      *vessel.StateService
	now          func() time.Time
}

func (c *Coordinator) SetRetryService(retry *RetryService) {
	c.mu.Lock()
	c.retry = retry
	c.mu.Unlock()
}

func NewCoordinator(circuits *circuit.Manager, circuitList *circuit.Registry, routes *valve.RouteService, vessels *vessel.StateService, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{
		sessions:     make(map[uuid.UUID]model.FillSession),
		active:       make(map[string]uuid.UUID),
		blockedZones: make(map[string]struct{}),
		circuits:     circuits,
		circuitList:  circuitList,
		routes:       routes,
		vessels:      vessels,
		now:          now,
	}
}

func (c *Coordinator) Start(ctx context.Context, vesselID string, origin model.FillOrigin, liters float64) (model.FillSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.zoneBlockedLocked(vesselID) {
		return model.FillSession{}, ErrZoneBlocked
	}
	if _, exists := c.active[vesselID]; exists {
		return model.FillSession{}, ErrActiveSession
	}
	selected, err := c.selectCircuit()
	if err != nil {
		return model.FillSession{}, err
	}
	session, err := model.NewFillSession(vesselID, origin, 1, liters, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	reservation, err := c.circuits.Reserve(selected.ID, vesselID, session.ID, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	session = session.WithCircuit(selected.ID, c.now())
	session, err = session.Transition(model.FillPrechecking, c.now())
	if err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	if _, err := c.vessels.BeginFill(vesselID); err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	route := valve.Route{
		SessionID:         session.ID,
		CircuitID:         selected.ID,
		CircuitGeneration: reservation.Generation,
		ValveID:           selected.ValveID,
		OpenCommandID:     uuid.New(),
		CloseCommandID:    uuid.New(),
	}
	var openErr error
	if c.retry != nil {
		_, openErr = c.retry.Open(ctx, session.ID, reservation.Generation, selected.ValveID)
		if openErr == nil {
			openErr = c.routes.TrackOpen(route)
		}
	} else {
		openErr = c.routes.Open(ctx, route)
	}
	if openErr != nil {
		_, _ = c.vessels.FinishFill(vesselID)
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, openErr
	}
	session, err = session.Transition(model.FillFilling, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	c.sessions[session.ID] = session
	c.active[vesselID] = session.ID
	return session, nil
}

func (c *Coordinator) StartAutomatic(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	return c.Start(ctx, vesselID, model.FillAutomatic, liters)
}

func (c *Coordinator) StartManual(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	return c.Start(ctx, vesselID, model.FillManual, liters)
}

func (c *Coordinator) BeginSoak(sessionID uuid.UUID) (model.FillSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	next, err := session.Transition(model.FillSoaking, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	c.sessions[sessionID] = next
	return next, nil
}

func (c *Coordinator) Get(id uuid.UUID) (model.FillSession, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	session, ok := c.sessions[id]
	return session, ok
}

func (c *Coordinator) List() []model.FillSession {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]model.FillSession, 0, len(c.sessions))
	for _, session := range c.sessions {
		result = append(result, session)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

// RestoreFaultedCircuit reinstates a faulted reservation during emergency
// recovery after a restart. Circuits tied to uncertain close receipts must
// stay occupied and are only releasable through explicit recovery.
func (c *Coordinator) RestoreFaultedCircuit(circuitID string, generation uint64, sessionID uuid.UUID, vesselID string) {
	if circuitID == "" || sessionID == uuid.Nil {
		return
	}
	c.circuits.RestoreFault(circuit.Reservation{
		CircuitID:  circuitID,
		SessionID:  sessionID,
		VesselID:   vesselID,
		Generation: generation,
	})
}

// FaultedCircuitForSession reports whether a session currently holds a
// faulted reservation, returning its circuit id.
func (c *Coordinator) FaultedCircuitForSession(sessionID uuid.UUID) (string, bool) {
	for _, candidate := range c.circuitList.List() {
		reservation, occupied := c.circuits.Get(candidate.ID)
		if occupied && reservation.SessionID == sessionID && reservation.Faulted {
			return candidate.ID, true
		}
	}
	return "", false
}

// RecoverFaultedBySession explicitly clears a faulted reservation held by a
// session. Unlike the normal fill recovery path it does not require the
// in-memory session, so a fault restored after a restart can still be
// released by an operator. It returns the circuit id that was freed.
func (c *Coordinator) RecoverFaultedBySession(sessionID uuid.UUID) (string, bool) {
	for _, candidate := range c.circuitList.List() {
		reservation, occupied := c.circuits.Get(candidate.ID)
		if !occupied || reservation.SessionID != sessionID || !reservation.Faulted {
			continue
		}
		if err := c.circuits.Recover(candidate.ID, sessionID); err != nil {
			continue
		}
		return candidate.ID, true
	}
	return "", false
}

func (c *Coordinator) selectCircuit() (circuit.Circuit, error) {
	for _, candidate := range c.circuitList.List() {
		if !candidate.Enabled {
			continue
		}
		if _, occupied := c.circuits.Get(candidate.ID); !occupied {
			return candidate, nil
		}
	}
	return circuit.Circuit{}, circuit.ErrCircuitBusy
}

func (c *Coordinator) finishState(session model.FillSession) {
	c.sessions[session.ID] = session
	delete(c.active, session.VesselID)
}

// BlockZone marks every vessel in the zone as ineligible for new fills while
// an emergency stop is in effect. The block is advisory at the coordinator
// level and survives only as long as the process; emergency recovery replays
// it from the journal after a restart.
func (c *Coordinator) BlockZone(zone string) {
	if zone == "" {
		return
	}
	c.mu.Lock()
	c.blockedZones[zone] = struct{}{}
	c.mu.Unlock()
}

// ResumeZone lifts an emergency new-fill block. Faulted circuit reservations
// are unaffected and still need explicit per-session recovery.
func (c *Coordinator) ResumeZone(zone string) {
	if zone == "" {
		return
	}
	c.mu.Lock()
	delete(c.blockedZones, zone)
	c.mu.Unlock()
}

func (c *Coordinator) ZoneBlocked(zone string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, blocked := c.blockedZones[zone]
	return blocked
}

func (c *Coordinator) BlockedZones() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	zones := make([]string, 0, len(c.blockedZones))
	for zone := range c.blockedZones {
		zones = append(zones, zone)
	}
	sort.Strings(zones)
	return zones
}

func (c *Coordinator) zoneBlockedLocked(vesselID string) bool {
	vesselValue, ok := c.vessels.Registry().Get(vesselID)
	if !ok {
		return false
	}
	_, blocked := c.blockedZones[vesselValue.Zone]
	return blocked
}

// ActiveSessionsInZone returns the sessions still running on any vessel that
// belongs to the zone, ordered by creation time so emergency commands go out
// oldest-session-first in a stable sequence.
func (c *Coordinator) ActiveSessionsInZone(zone string) []model.FillSession {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]model.FillSession, 0)
	for _, session := range c.sessions {
		if !session.Active() {
			continue
		}
		vesselValue, ok := c.vessels.Registry().Get(session.VesselID)
		if !ok || vesselValue.Zone != zone {
			continue
		}
		result = append(result, session)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

// EmergencyClose drives the pre-issued close command for one session exactly
// once. A confirmed receipt releases the circuit normally; a transport
// failure or non-executed receipt means the valve state cannot be trusted, so
// the circuit is retained as faulted and must be recovered explicitly.
func (c *Coordinator) EmergencyClose(ctx context.Context, sessionID uuid.UUID, reason string) EmergencyCloseResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	result := EmergencyCloseResult{SessionID: sessionID}
	if !ok {
		result.Status = EmergencyCloseInactive
		result.Detail = "fill session not found"
		return result
	}
	result.VesselID = session.VesselID
	result.CircuitID = session.CircuitID
	if route, tracked := c.routes.RouteFor(session.ID); tracked {
		result.ValveID = route.ValveID
		result.CommandID = route.CloseCommandID
		result.CircuitGeneration = route.CircuitGeneration
	}
	if !session.Active() {
		result.Status = EmergencyCloseInactive
		result.Detail = "session is no longer active"
		return result
	}
	receipt, err := c.routes.Close(ctx, session.ID)
	result.Receipt = receipt
	switch {
	case err != nil:
		result.Status = EmergencyCloseUncertain
		result.Detail = err.Error()
	case !receipt.Executed:
		result.Status = EmergencyCloseUncertain
		result.Detail = receipt.Message
	default:
		result.Status = EmergencyCloseConfirmed
		result.Detail = receipt.Message
	}
	failed := session.Failed(reason+": "+result.Detail, c.now())
	if result.Status == EmergencyCloseUncertain {
		_ = c.circuits.RetainFault(session.CircuitID, session.ID)
		c.sessions[session.ID] = failed
		return result
	}
	if err := c.circuits.Release(session.CircuitID, session.ID); err != nil {
		result.Status = EmergencyCloseUncertain
		result.Detail = err.Error()
		_ = c.circuits.RetainFault(session.CircuitID, session.ID)
		c.sessions[session.ID] = session.Failed(reason+": "+err.Error(), c.now())
		return result
	}
	if _, err := c.vessels.FinishFill(session.VesselID); err != nil {
		result.Status = EmergencyCloseUncertain
		result.Detail = err.Error()
		_ = c.circuits.RetainFault(session.CircuitID, session.ID)
		c.sessions[session.ID] = session.Failed(reason+": "+err.Error(), c.now())
		return result
	}
	c.finishState(failed)
	return result
}
