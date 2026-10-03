package alarm

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type Service struct {
	mu        sync.RWMutex
	alarms    map[uuid.UUID]model.Alarm
	delivery  *DeliveryService
	observers []func(context.Context, model.Alarm)
	now       func() time.Time
}

func NewService(delivery *DeliveryService, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{alarms: make(map[uuid.UUID]model.Alarm), delivery: delivery, now: now}
}

// AddObserver registers a callback invoked synchronously when an alarm is
// raised, before notification delivery, so safety-critical reactions run
// first.
func (s *Service) AddObserver(observer func(context.Context, model.Alarm)) {
	s.mu.Lock()
	s.observers = append(s.observers, observer)
	s.mu.Unlock()
}

func (s *Service) Raise(ctx context.Context, vesselID string, kind model.AlarmKind, message string) (model.Alarm, error) {
	alarm, err := model.NewAlarm(vesselID, kind, message, s.now())
	if err != nil {
		return model.Alarm{}, err
	}
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	observers := append([]func(context.Context, model.Alarm){}, s.observers...)
	s.mu.Unlock()
	for _, observer := range observers {
		observer(ctx, alarm)
	}
	delivered, err := s.delivery.Deliver(ctx, alarm)
	s.mu.Lock()
	s.alarms[alarm.ID] = delivered
	s.mu.Unlock()
	return delivered, err
}

func (s *Service) Restore(alarm model.Alarm) {
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.mu.Unlock()
}

func (s *Service) Get(id uuid.UUID) (model.Alarm, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	alarm, ok := s.alarms[id]
	return alarm, ok
}

func (s *Service) List() []model.Alarm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Alarm, 0, len(s.alarms))
	for _, alarm := range s.alarms {
		result = append(result, alarm)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (s *Service) Acknowledge(id uuid.UUID) (model.Alarm, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	alarm, ok := s.alarms[id]
	if !ok {
		return model.Alarm{}, errors.New("alarm not found")
	}
	alarm = alarm.Acknowledge(s.now())
	s.alarms[id] = alarm
	return alarm, nil
}

func (s *Service) Clear(id uuid.UUID) (model.Alarm, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	alarm, ok := s.alarms[id]
	if !ok {
		return model.Alarm{}, errors.New("alarm not found")
	}
	alarm = alarm.Clear(s.now())
	s.alarms[id] = alarm
	return alarm, nil
}
