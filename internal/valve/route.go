package valve

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"

	"github.com/wyw14/cry-98/internal/model"
)

type Route struct {
	SessionID         uuid.UUID
	CircuitID         string
	CircuitGeneration uint64
	ValveID           string
	OpenCommandID     uuid.UUID
	CloseCommandID    uuid.UUID
}

type RouteService struct {
	commands *CommandService
	mu       sync.Mutex
	open     map[uuid.UUID]Route
}

func NewRouteService(commands *CommandService) *RouteService {
	return &RouteService{commands: commands, open: make(map[uuid.UUID]Route)}
}

func (s *RouteService) Open(ctx context.Context, route Route) error {
	if route.SessionID == uuid.Nil || route.OpenCommandID == uuid.Nil {
		return errors.New("route command identity is incomplete")
	}
	if _, err := s.commands.Open(
		ctx,
		route.OpenCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
	); err != nil {
		return err
	}
	return s.TrackOpen(route)
}

func (s *RouteService) TrackOpen(route Route) error {
	if route.SessionID == uuid.Nil || route.CloseCommandID == uuid.Nil {
		return errors.New("route tracking identity is incomplete")
	}
	s.mu.Lock()
	s.open[route.SessionID] = route
	s.mu.Unlock()
	return nil
}

func (s *RouteService) Close(ctx context.Context, sessionID uuid.UUID) (model.ValveReceipt, error) {
	s.mu.Lock()
	route, ok := s.open[sessionID]
	s.mu.Unlock()
	if !ok {
		return model.ValveReceipt{}, errors.New("route is not open")
	}
	receipt, err := s.commands.Close(
		ctx,
		route.CloseCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
	)
	if err != nil {
		return model.ValveReceipt{}, err
	}
	s.mu.Lock()
	delete(s.open, sessionID)
	s.mu.Unlock()
	return receipt, nil
}

// RouteFor reports the tracked open route for a session. Emergency handling
// uses it to drive the same pre-issued close command instead of minting a
// second, possibly conflicting, dangerous command.
func (s *RouteService) RouteFor(sessionID uuid.UUID) (Route, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	route, ok := s.open[sessionID]
	return route, ok
}
