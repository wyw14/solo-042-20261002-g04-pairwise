package emergency

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/fill"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/pressure"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

// scriptedTransport counts close commands and can be switched into a mode
// where every close fails, simulating an untrusted/missing valve receipt.
type scriptedTransport struct {
	mu        sync.Mutex
	closes    int
	failClose bool
	now       func() time.Time
}

func (t *scriptedTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ValveReceipt{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if command.Action == model.ValveClose {
		t.closes++
		if t.failClose {
			return model.ValveReceipt{}, errors.New("valve controller unreachable")
		}
	}
	return model.ValveReceipt{
		CommandID:  command.ID,
		Executed:   true,
		Message:    "accepted",
		ReceivedAt: t.now().UTC(),
	}, nil
}

func (t *scriptedTransport) closeCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closes
}

type fixture struct {
	store       *journal.Store
	vessels     *vessel.Registry
	registry    *circuit.Registry
	manager     *circuit.Manager
	transport   *scriptedTransport
	coordinator *fill.Coordinator
	fills       *fill.Service
	service     *Service
}

func newFixture(t *testing.T, root string, transport *scriptedTransport) *fixture {
	t.Helper()
	now := time.Now
	var err error
	var store *journal.Store
	if root == "" {
		store, err = journal.NewStore(t.TempDir())
	} else {
		store, err = journal.NewStore(root)
	}
	if err != nil {
		t.Fatal(err)
	}

	vessels := vessel.NewRegistry()
	for _, seed := range []struct{ id, name, zone string }{
		{"V-01", "Vessel one", "north"},
		{"V-02", "Vessel two", "south"},
	} {
		value, createErr := model.NewVessel(seed.id, seed.name, seed.zone, now())
		if createErr != nil {
			t.Fatal(createErr)
		}
		if err := vessels.Register(value); err != nil {
			t.Fatal(err)
		}
	}
	vesselState := vessel.NewStateService(vessels, now)

	registry := circuit.NewRegistry(
		circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true},
		circuit.Circuit{ID: "C-B", Zone: "south", ValveID: "VALVE-B", Enabled: true},
	)
	manager := circuit.NewManager("C-A", "C-B")

	if transport == nil {
		transport = &scriptedTransport{now: now}
	}
	controller, err := valve.NewController(transport, now)
	if err != nil {
		t.Fatal(err)
	}
	commands := valve.NewCommandService(controller, now)
	routes := valve.NewRouteService(commands)
	coordinator := fill.NewCoordinator(manager, registry, routes, vesselState, now)
	retry, err := fill.NewRetryService(commands, fill.RetryPolicy{Attempts: 1, Delay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.SetRetryService(retry)

	monitor := pressure.NewMonitor(now)
	completion := fill.NewCompletionService(coordinator, monitor, store, now)
	aborts := fill.NewAbortService(coordinator, now)
	fills, err := fill.NewService(coordinator, completion, aborts, 2.1)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(fills, vessels, store, now)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{
		store:       store,
		vessels:     vessels,
		registry:    registry,
		manager:     manager,
		transport:   transport,
		coordinator: coordinator,
		fills:       fills,
		service:     service,
	}
}

func severeAlarm(t *testing.T, vesselID string, kind model.AlarmKind) model.Alarm {
	t.Helper()
	alarm, err := model.NewAlarm(vesselID, kind, "severe condition", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return alarm
}

func startFill(t *testing.T, f *fixture, vesselID string) model.FillSession {
	t.Helper()
	session, err := f.fills.StartManual(context.Background(), vesselID, 9.5)
	if err != nil {
		t.Fatalf("start fill: %v", err)
	}
	return session
}

func TestConfirmedStopIsolatesAndBlocksOnlyAffectedZone(t *testing.T) {
	f := newFixture(t, "", nil)
	session := startFill(t, f, "V-01")

	alarm := severeAlarm(t, "V-01", model.AlarmPressure)
	record, err := f.service.HandleAlarm(context.Background(), alarm)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != StatusCompleted {
		t.Fatalf("status=%s", record.Status)
	}
	if len(record.Confirmed()) != 1 || len(record.Uncertain()) != 0 {
		t.Fatalf("confirmed=%d uncertain=%d", len(record.Confirmed()), len(record.Uncertain()))
	}
	confirmed := record.Confirmed()[0]
	if confirmed.SessionID != session.ID || confirmed.ValveID != "VALVE-A" {
		t.Fatalf("unexpected confirmed outcome: %+v", confirmed)
	}

	// Session failed and the circuit released after a trusted receipt.
	stored, ok := f.fills.Get(session.ID)
	if !ok || stored.State != model.FillFailed {
		t.Fatalf("session state=%v ok=%v", stored.State, ok)
	}
	if _, occupied := f.manager.Get("C-A"); occupied {
		t.Fatal("circuit stayed occupied after confirmed close")
	}

	// The affected area cannot start new fills.
	if _, err := f.fills.StartManual(context.Background(), "V-01", 1); !errors.Is(err, fill.ErrZoneBlocked) {
		t.Fatalf("expected zone block, got %v", err)
	}
	if !f.coordinator.ZoneBlocked("north") {
		t.Fatal("north zone should be blocked")
	}

	// Other zones keep filling normally.
	other := startFill(t, f, "V-02")
	if !other.Active() {
		t.Fatalf("other-zone fill should be active: %+v", other)
	}

	// Progress is queryable.
	got, ok := f.service.Get(alarm.ID)
	if !ok || got.ID != record.ID {
		t.Fatal("stop record not found by alarm id")
	}
	if len(f.service.List()) != 1 {
		t.Fatalf("list len=%d", len(f.service.List()))
	}
}

func TestUncertainReceiptRetainsFaultedCircuit(t *testing.T) {
	f := newFixture(t, "", nil)
	session := startFill(t, f, "V-01")

	// Close commands now fail: the valve position cannot be trusted.
	f.transport.failClose = true
	alarm := severeAlarm(t, "V-01", model.AlarmLeak)
	record, err := f.service.HandleAlarm(context.Background(), alarm)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Uncertain()) != 1 {
		t.Fatalf("uncertain=%d", len(record.Uncertain()))
	}

	reservation, occupied := f.manager.Get("C-A")
	if !occupied || !reservation.Faulted {
		t.Fatal("uncertain close did not retain a faulted circuit")
	}
	// Auto-release is refused; only explicit recovery frees it.
	if err := f.manager.Release("C-A", session.ID); err == nil {
		t.Fatal("faulted circuit was auto-released")
	}
	// New fills remain blocked even though the session already failed.
	if _, err := f.fills.StartManual(context.Background(), "V-01", 1); !errors.Is(err, fill.ErrZoneBlocked) {
		t.Fatalf("expected zone block, got %v", err)
	}
	// Operator explicitly recovers the isolation, mirroring recover-isolation.
	if err := f.manager.Recover("C-A", session.ID); err != nil {
		t.Fatal(err)
	}
	if _, occupied := f.manager.Get("C-A"); occupied {
		t.Fatal("circuit still occupied after explicit recovery")
	}
}

func TestDuplicateAlarmDrivesSingleCloseCommand(t *testing.T) {
	f := newFixture(t, "", nil)
	startFill(t, f, "V-01")
	alarm := severeAlarm(t, "V-01", model.AlarmPressure)

	first, err := f.service.HandleAlarm(context.Background(), alarm)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.service.HandleAlarm(context.Background(), alarm)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatal("duplicate alarm created a second stop record")
	}
	if count := f.transport.closeCount(); count != 1 {
		t.Fatalf("close commands issued=%d, want 1", count)
	}
}

func TestConcurrentDuplicateAlarmsDriveSingleClose(t *testing.T) {
	f := newFixture(t, "", nil)
	startFill(t, f, "V-01")
	alarm := severeAlarm(t, "V-01", model.AlarmPressure)

	const goroutines = 16
	var wg sync.WaitGroup
	results := make(chan uuid.UUID, goroutines)
	errs := make(chan error, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, err := f.service.HandleAlarm(context.Background(), alarm)
			if err != nil {
				errs <- err
				return
			}
			results <- record.ID
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first uuid.UUID
	count := 0
	for id := range results {
		if count == 0 {
			first = id
		} else if id != first {
			t.Fatal("concurrent duplicates produced distinct stop records")
		}
		count++
	}
	if count != goroutines {
		t.Fatalf("results=%d want %d", count, goroutines)
	}
	if closes := f.transport.closeCount(); closes != 1 {
		t.Fatalf("close commands issued=%d, want 1", closes)
	}
}

func TestNonSevereAlarmIgnored(t *testing.T) {
	f := newFixture(t, "", nil)
	startFill(t, f, "V-01")
	alarm := severeAlarm(t, "V-01", model.AlarmLowLevel)
	if _, err := f.service.HandleAlarm(context.Background(), alarm); !errors.Is(err, errNotSevere) {
		t.Fatalf("expected errNotSevere, got %v", err)
	}
	if count := f.transport.closeCount(); count != 0 {
		t.Fatalf("non-severe alarm issued closes=%d", count)
	}
	if f.coordinator.ZoneBlocked("north") {
		t.Fatal("non-severe alarm blocked a zone")
	}
}

func TestStopWithNoActiveSessions(t *testing.T) {
	f := newFixture(t, "", nil)
	alarm := severeAlarm(t, "V-01", model.AlarmLeak)
	record, err := f.service.HandleAlarm(context.Background(), alarm)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != StatusNoActiveSessions || len(record.Sessions) != 0 {
		t.Fatalf("record=%+v", record)
	}
	// The area is still blocked even if nothing was running.
	if _, err := f.fills.StartManual(context.Background(), "V-01", 1); !errors.Is(err, fill.ErrZoneBlocked) {
		t.Fatalf("expected zone block, got %v", err)
	}
}

func TestRecoveryRebuildsBlocksAndFaultsWithoutReissuingCommands(t *testing.T) {
	first := newFixture(t, "", nil)
	northSession := startFill(t, first, "V-01")
	northCircuit := northSession.CircuitID
	southSession := startFill(t, first, "V-02")
	southCircuit := southSession.CircuitID
	if southCircuit == northCircuit {
		t.Fatal("second fill should use the other circuit")
	}

	// A confirmed stop releases the north circuit.
	northAlarm := severeAlarm(t, "V-01", model.AlarmPressure)
	if _, err := first.service.HandleAlarm(context.Background(), northAlarm); err != nil {
		t.Fatal(err)
	}

	// An uncertain stop retains the south circuit as faulted.
	first.transport.failClose = true
	southAlarm := severeAlarm(t, "V-02", model.AlarmLeak)
	southRecord, err := first.service.HandleAlarm(context.Background(), southAlarm)
	if err != nil {
		t.Fatal(err)
	}
	if len(southRecord.Uncertain()) != 1 {
		t.Fatalf("uncertain=%d", len(southRecord.Uncertain()))
	}
	dataDir := first.store.Root()
	closeBefore := first.transport.closeCount()

	// Simulate a process restart: brand new in-memory state, same journal.
	freshTransport := &scriptedTransport{now: time.Now}
	second := newFixture(t, dataDir, freshTransport)
	if err := NewRecovery(second.service).Restore(); err != nil {
		t.Fatal(err)
	}

	// Both affected areas come back blocked.
	if !second.coordinator.ZoneBlocked("north") || !second.coordinator.ZoneBlocked("south") {
		t.Fatal("zone blocks were not restored")
	}
	if _, err := second.fills.StartManual(context.Background(), "V-01", 1); !errors.Is(err, fill.ErrZoneBlocked) {
		t.Fatalf("expected restored north block, got %v", err)
	}

	// Uncertain circuit comes back faulted and cannot be auto-released.
	reservation, occupied := second.manager.Get(southCircuit)
	if !occupied || !reservation.Faulted || reservation.SessionID != southSession.ID {
		t.Fatalf("faulted reservation not restored: %+v occupied=%v", reservation, occupied)
	}
	if err := second.manager.Release(southCircuit, southSession.ID); err == nil {
		t.Fatal("restored faulted circuit was auto-released")
	}
	// Confirmed circuit is not resurrected as occupied.
	if _, occupied := second.manager.Get(northCircuit); occupied {
		t.Fatal("confirmed circuit was incorrectly restored as occupied")
	}

	// Progress is queryable after restart, split into confirmed/uncertain.
	got, ok := second.service.Get(southAlarm.ID)
	if !ok || got.Status != StatusCompleted || len(got.Uncertain()) != 1 {
		t.Fatalf("recovered south record wrong: %+v ok=%v", got, ok)
	}
	recoveredFirst, ok := second.service.Get(northAlarm.ID)
	if !ok || len(recoveredFirst.Confirmed()) != 1 {
		t.Fatalf("recovered first record wrong: %+v ok=%v", recoveredFirst, ok)
	}

	// Re-driving a recovered alarm issues no new command.
	again, err := second.service.HandleAlarm(context.Background(), southAlarm)
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != southRecord.ID {
		t.Fatal("recovered alarm spawned a new stop record")
	}
	if freshTransport.closeCount() != 0 {
		t.Fatalf("restart reissued commands: %d", freshTransport.closeCount())
	}
	if first.transport.closeCount() != closeBefore {
		t.Fatal("original transport received extra commands")
	}
}

func TestResumeAndFaultRecoverySurviveRestart(t *testing.T) {
	first := newFixture(t, "", nil)
	session := startFill(t, first, "V-01")
	circuitID := session.CircuitID
	first.transport.failClose = true
	alarm := severeAlarm(t, "V-01", model.AlarmLeak)
	if _, err := first.service.HandleAlarm(context.Background(), alarm); err != nil {
		t.Fatal(err)
	}
	dataDir := first.store.Root()

	// Operator makes the area safe: lift the block and clear the fault.
	if err := first.service.ResumeZone(context.Background(), "north"); err != nil {
		t.Fatal(err)
	}
	if err := first.service.RecoverFault(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}

	// Restart: replay must not resurrect either the block or the fault.
	fresh := newFixture(t, dataDir, &scriptedTransport{now: time.Now})
	if err := NewRecovery(fresh.service).Restore(); err != nil {
		t.Fatal(err)
	}
	if fresh.coordinator.ZoneBlocked("north") {
		t.Fatal("resumed zone block came back after restart")
	}
	if _, occupied := fresh.manager.Get(circuitID); occupied {
		t.Fatal("recovered fault came back after restart")
	}
	// A new fill in the resumed area is now accepted.
	if _, err := fresh.fills.StartManual(context.Background(), "V-01", 1); err != nil {
		t.Fatalf("expected fill allowed after resume, got %v", err)
	}
}

func TestRecoverFaultRejectsUnknownSession(t *testing.T) {
	f := newFixture(t, "", nil)
	if err := f.service.RecoverFault(context.Background(), uuid.New()); err == nil {
		t.Fatal("expected error recovering a non-existent fault")
	}
}

func TestResumeZoneLiftsBlockButKeepsFault(t *testing.T) {
	f := newFixture(t, "", nil)
	session := startFill(t, f, "V-01")
	f.transport.failClose = true
	alarm := severeAlarm(t, "V-01", model.AlarmLeak)
	if _, err := f.service.HandleAlarm(context.Background(), alarm); err != nil {
		t.Fatal(err)
	}

	if err := f.service.ResumeZone(context.Background(), "north"); err != nil {
		t.Fatal(err)
	}
	if f.coordinator.ZoneBlocked("north") {
		t.Fatal("resume did not lift the zone block")
	}
	// Fault retention is independent: resuming a zone must not release it.
	reservation, occupied := f.manager.Get(session.CircuitID)
	if !occupied || !reservation.Faulted {
		t.Fatalf("resume released the faulted circuit: %+v", reservation)
	}
}
