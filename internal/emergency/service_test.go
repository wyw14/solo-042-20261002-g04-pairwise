package emergency

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/fill"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type stubTransport struct {
	mu       sync.Mutex
	executed []model.ValveCommand
	failOn   model.ValveAction
}

func (t *stubTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ValveReceipt{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failOn != "" && command.Action == t.failOn {
		return model.ValveReceipt{}, errors.New("valve transport unavailable")
	}
	t.executed = append(t.executed, command)
	return model.ValveReceipt{
		CommandID:  command.ID,
		Executed:   true,
		Message:    "accepted",
		ReceivedAt: time.Now().UTC(),
	}, nil
}

func (t *stubTransport) count(action model.ValveAction) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	total := 0
	for _, command := range t.executed {
		if command.Action == action {
			total++
		}
	}
	return total
}

func (t *stubTransport) last(action model.ValveAction) (model.ValveCommand, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := len(t.executed) - 1; i >= 0; i-- {
		if t.executed[i].Action == action {
			return t.executed[i], true
		}
	}
	return model.ValveCommand{}, false
}

type world struct {
	store       *journal.Store
	vessels     *vessel.Registry
	circuits    *circuit.Manager
	routes      *valve.RouteService
	coordinator *fill.Coordinator
	transport   *stubTransport
	service     *Service
}

func newWorld(t *testing.T, store *journal.Store) *world {
	t.Helper()
	vessels := vessel.NewRegistry()
	for _, seed := range []struct{ id, zone string }{
		{"V-01", "north"},
		{"V-02", "north"},
		{"V-03", "south"},
	} {
		item, err := model.NewVessel(seed.id, "Cryogenic vessel "+seed.id, seed.zone, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := vessels.Register(item); err != nil {
			t.Fatal(err)
		}
	}
	circuitRegistry := circuit.NewRegistry(
		circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true},
		circuit.Circuit{ID: "C-B", Zone: "south", ValveID: "VALVE-B", Enabled: true},
	)
	manager := circuit.NewManager("C-A", "C-B")
	transport := &stubTransport{}
	controller, err := valve.NewController(transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	commands := valve.NewCommandService(controller, nil)
	routes := valve.NewRouteService(commands)
	vesselState := vessel.NewStateService(vessels, nil)
	coordinator := fill.NewCoordinator(manager, circuitRegistry, routes, vesselState, nil)
	service, err := NewService(store, vessels, coordinator, routes, nil)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.SetZoneLocker(service)
	return &world{
		store:       store,
		vessels:     vessels,
		circuits:    manager,
		routes:      routes,
		coordinator: coordinator,
		transport:   transport,
		service:     service,
	}
}

func newStore(t *testing.T, dir string) *journal.Store {
	t.Helper()
	store, err := journal.NewStore(filepath.Join(dir, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func raiseAlarm(t *testing.T, vesselID string, kind model.AlarmKind) model.Alarm {
	t.Helper()
	alarm, err := model.NewAlarm(vesselID, kind, "alarm raised", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return alarm
}

func startFill(t *testing.T, w *world, vesselID string) model.FillSession {
	t.Helper()
	session, err := w.coordinator.Start(context.Background(), vesselID, model.FillManual, 2.1)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestSevereAlarmClosesZoneSessionsAndLocksZone(t *testing.T) {
	w := newWorld(t, newStore(t, t.TempDir()))
	session := startFill(t, w, "V-01")

	stop, err := w.service.HandleAlarm(context.Background(), raiseAlarm(t, "V-02", model.AlarmPressure))
	if err != nil {
		t.Fatal(err)
	}
	if stop.ID == uuid.Nil || stop.Zone != "north" || stop.State != model.EmergencyActive {
		t.Fatalf("unexpected stop record: %+v", stop)
	}
	if len(stop.Actions) != 1 {
		t.Fatalf("expected one disposal action, got %d", len(stop.Actions))
	}
	action := stop.Actions[0]
	if action.SessionID != session.ID || action.Status != model.EmergencyActionConfirmed {
		t.Fatalf("unexpected action: %+v", action)
	}
	if action.ValveID != "VALVE-A" || action.CommandID == uuid.Nil {
		t.Fatalf("action missing valve identity: %+v", action)
	}
	// The journaled command identity must be the command actually sent.
	sent, ok := w.transport.last(model.ValveClose)
	if !ok || sent.ID != action.CommandID {
		t.Fatalf("journaled command %s does not match sent command %+v", action.CommandID, sent)
	}

	updated, ok := w.coordinator.Get(session.ID)
	if !ok || updated.State != model.FillFailed {
		t.Fatalf("session state=%v", updated.State)
	}
	if _, occupied := w.circuits.Get("C-A"); occupied {
		t.Fatal("confirmed close must release the circuit")
	}
	vessel, _ := w.vessels.Get("V-01")
	if vessel.Status != model.VesselReady {
		t.Fatalf("vessel status=%s", vessel.Status)
	}
	if !w.service.ZoneLocked("north") || w.service.ZoneLocked("south") {
		t.Fatal("zone lock must cover the affected zone only")
	}
	if _, err := w.coordinator.Start(context.Background(), "V-02", model.FillManual, 2.1); !errors.Is(err, fill.ErrZoneLocked) {
		t.Fatalf("locked zone start err=%v", err)
	}
	if _, err := w.coordinator.Start(context.Background(), "V-03", model.FillManual, 2.1); err != nil {
		t.Fatalf("unaffected zone must stay fillable: %v", err)
	}

	events, err := w.store.Events(Partition, 0)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]string, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	want := []string{EventTriggered, EventCloseIntended, EventCloseConfirmed}
	if len(kinds) != len(want) {
		t.Fatalf("journaled kinds=%v", kinds)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("journaled kinds=%v, want intent before command before outcome", kinds)
		}
	}
}

func TestNonSevereAlarmDoesNotStop(t *testing.T) {
	w := newWorld(t, newStore(t, t.TempDir()))
	startFill(t, w, "V-01")
	for _, kind := range []model.AlarmKind{model.AlarmLowLevel, model.AlarmProbe} {
		stop, err := w.service.HandleAlarm(context.Background(), raiseAlarm(t, "V-01", kind))
		if err != nil {
			t.Fatal(err)
		}
		if stop.ID != uuid.Nil {
			t.Fatalf("non-severe alarm %s produced a stop", kind)
		}
	}
	if w.service.ZoneLocked("north") {
		t.Fatal("non-severe alarm locked the zone")
	}
	if w.transport.count(model.ValveClose) != 0 {
		t.Fatal("non-severe alarm issued a close command")
	}
}

func TestRepeatedAlarmDoesNotReissueCommands(t *testing.T) {
	w := newWorld(t, newStore(t, t.TempDir()))
	startFill(t, w, "V-01")
	first, err := w.service.HandleAlarm(context.Background(), raiseAlarm(t, "V-02", model.AlarmLeak))
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.service.HandleAlarm(context.Background(), raiseAlarm(t, "V-01", model.AlarmPressure))
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatal("repeated alarm created a second stop for the same zone")
	}
	if got := w.transport.count(model.ValveClose); got != 1 {
		t.Fatalf("close commands sent=%d, want 1", got)
	}
	if len(w.service.List()) != 1 {
		t.Fatal("repeated alarm duplicated the stop record")
	}
}

func TestUncertainReceiptRetainsCircuitFault(t *testing.T) {
	w := newWorld(t, newStore(t, t.TempDir()))
	w.transport.failOn = model.ValveClose
	session := startFill(t, w, "V-01")

	stop, err := w.service.HandleAlarm(context.Background(), raiseAlarm(t, "V-02", model.AlarmLeak))
	if err != nil {
		t.Fatal(err)
	}
	if len(stop.Actions) != 1 || stop.Actions[0].Status != model.EmergencyActionUncertain {
		t.Fatalf("expected uncertain receipt, got %+v", stop.Actions)
	}
	reservation, occupied := w.circuits.Get("C-A")
	if !occupied || !reservation.Faulted {
		t.Fatal("uncertain receipt must keep the circuit fault-occupied")
	}
	if err := w.circuits.Release("C-A", session.ID); err == nil {
		t.Fatal("faulted circuit was released automatically")
	}
	updated, _ := w.coordinator.Get(session.ID)
	if updated.State != model.FillFailed {
		t.Fatalf("session state=%s", updated.State)
	}
	if _, err := w.coordinator.Start(context.Background(), "V-01", model.FillManual, 2.1); !errors.Is(err, fill.ErrActiveSession) {
		t.Fatalf("vessel must stay occupied until recovery, err=%v", err)
	}
	if _, err := w.service.Resolve(stop.ID, false); !errors.Is(err, ErrUncertainReceipts) {
		t.Fatalf("resolve with uncertain receipts err=%v", err)
	}
	if _, err := w.service.Resolve(stop.ID, true); err != nil {
		t.Fatalf("acknowledged resolve failed: %v", err)
	}
	if w.service.ZoneLocked("north") {
		t.Fatal("resolved stop kept the zone locked")
	}
	if err := w.circuits.Recover("C-A", session.ID); err != nil {
		t.Fatalf("explicit recovery must remain possible: %v", err)
	}
}

func TestRestartDoesNotReissueCommands(t *testing.T) {
	dir := t.TempDir()
	first := newWorld(t, newStore(t, dir))
	startFill(t, first, "V-01")
	stop, err := first.service.HandleAlarm(context.Background(), raiseAlarm(t, "V-02", model.AlarmPressure))
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a process restart: fresh in-memory state over the same journal.
	restarted := newWorld(t, newStore(t, dir))
	if err := restarted.service.Restore(); err != nil {
		t.Fatal(err)
	}
	if !restarted.service.ZoneLocked("north") {
		t.Fatal("zone lock was lost across restart")
	}
	again, err := restarted.service.HandleAlarm(context.Background(), raiseAlarm(t, "V-01", model.AlarmLeak))
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != stop.ID {
		t.Fatal("restart created a new stop for an already-disposed zone")
	}
	if got := restarted.transport.count(model.ValveClose); got != 0 {
		t.Fatalf("restart re-issued %d close commands", got)
	}
	restored, ok := restarted.service.Get(stop.ID)
	if !ok || restored.Actions[0].Status != model.EmergencyActionConfirmed {
		t.Fatalf("restored stop lost its confirmed outcome: %+v", restored.Actions)
	}
}

func TestRestartRecoversPendingIntentAsUncertain(t *testing.T) {
	dir := t.TempDir()
	store := newStore(t, dir)
	stop, err := model.NewEmergencyStop(uuid.New(), "north", model.AlarmLeak, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	appendEvent := func(kind string, payload any) {
		t.Helper()
		event, err := model.NewEvent(Partition, kind, stop.ID.String(), payload, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(event); err != nil {
			t.Fatal(err)
		}
	}
	appendEvent(EventTriggered, stop)
	appendEvent(EventCloseIntended, model.EmergencyAction{
		SessionID:  uuid.New(),
		VesselID:   "V-01",
		CircuitID:  "C-A",
		ValveID:    "VALVE-A",
		CommandID:  uuid.New(),
		Status:     model.EmergencyActionPending,
		RecordedAt: time.Now().UTC(),
	})

	w := newWorld(t, newStore(t, dir))
	if err := w.service.Restore(); err != nil {
		t.Fatal(err)
	}
	restored, ok := w.service.Get(stop.ID)
	if !ok {
		t.Fatal("stop was not restored")
	}
	if len(restored.Actions) != 1 || restored.Actions[0].Status != model.EmergencyActionUncertain {
		t.Fatalf("pending intent must recover as uncertain, got %+v", restored.Actions)
	}
	if got := w.transport.count(model.ValveClose); got != 0 {
		t.Fatalf("recovery re-issued %d close commands", got)
	}
	if _, err := w.service.Resolve(stop.ID, false); !errors.Is(err, ErrUncertainReceipts) {
		t.Fatalf("recovered uncertainty must block resolve, err=%v", err)
	}
}

func TestResolveLifecycle(t *testing.T) {
	w := newWorld(t, newStore(t, t.TempDir()))
	startFill(t, w, "V-01")
	stop, err := w.service.HandleAlarm(context.Background(), raiseAlarm(t, "V-02", model.AlarmPressure))
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := w.service.Resolve(stop.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.State != model.EmergencyResolved || w.service.ZoneLocked("north") {
		t.Fatal("confirmed stop did not resolve cleanly")
	}
	again, err := w.service.Resolve(stop.ID, false)
	if err != nil || again.State != model.EmergencyResolved {
		t.Fatal("resolve must be idempotent")
	}
	if _, err := w.service.Resolve(uuid.New(), false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown stop resolve err=%v", err)
	}
	if _, err := w.coordinator.Start(context.Background(), "V-02", model.FillManual, 2.1); err != nil {
		t.Fatalf("resolved zone must accept fills again: %v", err)
	}
}
