package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/emergency"
	"github.com/wyw14/cry-98/internal/model"
)

type resolveEmergencyStopRequest struct {
	AcknowledgeUncertain bool `json:"acknowledge_uncertain"`
}

// emergencyStopView flattens the stop record with its disposal progress.
type emergencyStopView struct {
	model.EmergencyStop
	Progress model.EmergencyStopProgress `json:"progress"`
}

func emergencyStopResponse(stop model.EmergencyStop) emergencyStopView {
	return emergencyStopView{EmergencyStop: stop, Progress: stop.Progress()}
}

func (s *Server) listEmergencyStops(writer http.ResponseWriter, request *http.Request) {
	stops := s.deps.Emergency.List()
	items := make([]emergencyStopView, 0, len(stops))
	for _, stop := range stops {
		items = append(items, emergencyStopResponse(stop))
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) getEmergencyStop(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid emergency stop id")
		return
	}
	stop, ok := s.deps.Emergency.Get(id)
	if !ok {
		writeError(writer, http.StatusNotFound, "emergency stop not found")
		return
	}
	writeJSON(writer, http.StatusOK, emergencyStopResponse(stop))
}

func (s *Server) resolveEmergencyStop(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid emergency stop id")
		return
	}
	var input resolveEmergencyStopRequest
	if err := decodeJSON(request, &input); err != nil && !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	stop, err := s.deps.Emergency.Resolve(id, input.AcknowledgeUncertain)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, emergency.ErrNotFound) {
			status = http.StatusNotFound
		}
		writeError(writer, status, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, emergencyStopResponse(stop))
}
