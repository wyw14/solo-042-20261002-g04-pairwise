package fill

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type stubZoneLocker struct {
	locked map[string]bool
}

func (s stubZoneLocker) ZoneLocked(zone string) bool {
	return s.locked[zone]
}

func newTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	vessels := vessel.NewRegistry()
	item, err := model.NewVessel("V-01", "Cryogenic vessel V-01", "north", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := vessels.Register(item); err != nil {
		t.Fatal(err)
	}
	circuitRegistry := circuit.NewRegistry(
		circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true},
	)
	manager := circuit.NewManager("C-A")
	controller, err := valve.NewController(valve.NewMemoryTransport(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	routes := valve.NewRouteService(valve.NewCommandService(controller, nil))
	return NewCoordinator(manager, circuitRegistry, routes, vessel.NewStateService(vessels, nil), nil)
}

func TestStartRejectedWhileZoneLocked(t *testing.T) {
	coordinator := newTestCoordinator(t)
	coordinator.SetZoneLocker(stubZoneLocker{locked: map[string]bool{"north": true}})
	_, err := coordinator.Start(context.Background(), "V-01", model.FillManual, 2.1)
	if !errors.Is(err, ErrZoneLocked) {
		t.Fatalf("locked zone start err=%v", err)
	}
	if _, occupied := coordinator.active["V-01"]; occupied {
		t.Fatal("rejected start left an active session")
	}
}

func TestStartAllowedAfterZoneUnlock(t *testing.T) {
	coordinator := newTestCoordinator(t)
	locker := stubZoneLocker{locked: map[string]bool{"north": true}}
	coordinator.SetZoneLocker(locker)
	if _, err := coordinator.Start(context.Background(), "V-01", model.FillManual, 2.1); !errors.Is(err, ErrZoneLocked) {
		t.Fatalf("locked zone start err=%v", err)
	}
	locker.locked["north"] = false
	if _, err := coordinator.Start(context.Background(), "V-01", model.FillManual, 2.1); err != nil {
		t.Fatalf("unlocked zone start failed: %v", err)
	}
}

func TestTerminateEmergencyConfirmedReleasesCircuit(t *testing.T) {
	coordinator := newTestCoordinator(t)
	session, err := coordinator.Start(context.Background(), "V-01", model.FillManual, 2.1)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := coordinator.TerminateEmergency(session.ID, "emergency stop", true)
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != model.FillFailed {
		t.Fatalf("session state=%s", failed.State)
	}
	if _, occupied := coordinator.circuits.Get("C-A"); occupied {
		t.Fatal("confirmed termination kept the circuit occupied")
	}
	if _, active := coordinator.active["V-01"]; active {
		t.Fatal("confirmed termination kept the vessel active mapping")
	}
}

func TestTerminateEmergencyUncertainRetainsFault(t *testing.T) {
	coordinator := newTestCoordinator(t)
	session, err := coordinator.Start(context.Background(), "V-01", model.FillManual, 2.1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.TerminateEmergency(session.ID, "emergency stop", false); err != nil {
		t.Fatal(err)
	}
	reservation, occupied := coordinator.circuits.Get("C-A")
	if !occupied || !reservation.Faulted {
		t.Fatal("uncertain termination must retain the circuit fault")
	}
	if _, active := coordinator.active["V-01"]; !active {
		t.Fatal("uncertain termination must keep the vessel active mapping")
	}
	if err := coordinator.circuits.Release("C-A", session.ID); err == nil {
		t.Fatal("faulted circuit was released automatically")
	}
}
