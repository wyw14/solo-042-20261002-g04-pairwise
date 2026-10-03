package fill

import (
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

var ErrZoneLocked = errors.New("zone is locked by an active emergency stop")

// ZoneLocker reports whether a zone is blocked from starting new fills.
type ZoneLocker interface {
	ZoneLocked(zone string) bool
}

// zoneLocked reports whether the vessel's zone is blocked by an emergency
// stop. An unregistered vessel is left to the normal start validation.
func (c *Coordinator) zoneLocked(vesselID string) bool {
	if c.zoneLock == nil {
		return false
	}
	vessel, ok := c.vessels.Registry().Get(vesselID)
	if !ok {
		return false
	}
	return c.zoneLock.ZoneLocked(vessel.Zone)
}

// TerminateEmergency fails a session disposed by the emergency stop
// service. When the valve close receipt was confirmed the circuit is
// released normally; when it is uncertain the circuit keeps its fault
// occupation and the vessel keeps its active mapping, so neither can be
// released automatically — recovery stays an explicit operator action.
func (c *Coordinator) TerminateEmergency(sessionID uuid.UUID, reason string, closeConfirmed bool) (model.FillSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	if !session.Active() {
		return session, nil
	}
	failed := session.Failed(reason, c.now())
	if !closeConfirmed {
		if err := c.circuits.RetainFault(session.CircuitID, session.ID); err != nil {
			return model.FillSession{}, err
		}
		c.sessions[session.ID] = failed
		return failed, nil
	}
	if err := c.circuits.Release(session.CircuitID, session.ID); err != nil {
		return model.FillSession{}, err
	}
	if _, err := c.vessels.FinishFill(session.VesselID); err != nil {
		return model.FillSession{}, err
	}
	c.finishState(failed)
	return failed, nil
}
