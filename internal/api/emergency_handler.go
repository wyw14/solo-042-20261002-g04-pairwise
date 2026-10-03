package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/wyw14/cry-98/internal/emergency"
)

func (s *Server) listEmergencyStops(writer http.ResponseWriter, request *http.Request) {
	records := s.deps.Emergency.List()
	writeJSON(writer, http.StatusOK, map[string]any{"items": records})
}

func (s *Server) getEmergencyStop(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid alarm id")
		return
	}
	record, ok := s.deps.Emergency.Get(id)
	if !ok {
		writeError(writer, http.StatusNotFound, "emergency stop record not found")
		return
	}
	writeJSON(writer, http.StatusOK, renderStop(record))
}

type resumeZoneRequest struct {
	Zone string `json:"zone"`
}

func (s *Server) resumeEmergencyZone(writer http.ResponseWriter, request *http.Request) {
	var input resumeZoneRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if input.Zone == "" {
		writeError(writer, http.StatusBadRequest, "zone is required")
		return
	}
	if err := s.deps.Emergency.ResumeZone(request.Context(), input.Zone); err != nil {
		writeError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"zone": input.Zone, "blocked": s.deps.Fills.Coordinator().ZoneBlocked(input.Zone)})
}

type recoverFaultRequest struct {
	SessionID uuid.UUID `json:"session_id"`
}

func (s *Server) recoverEmergencyFault(writer http.ResponseWriter, request *http.Request) {
	var input recoverFaultRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if input.SessionID == uuid.Nil {
		writeError(writer, http.StatusBadRequest, "session_id is required")
		return
	}
	if err := s.deps.Emergency.RecoverFault(request.Context(), input.SessionID); err != nil {
		writeError(writer, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"session_id": input.SessionID, "status": "fault_recovered"})
}

// renderStop splits outcomes into confirmed and uncertain lists so the
// response makes clear which circuits are retained after an untrusted close
// receipt and still need explicit isolation recovery.
func renderStop(record emergency.StopRecord) map[string]any {
	return map[string]any{
		"id":           record.ID,
		"alarm_id":     record.AlarmID,
		"vessel_id":    record.VesselID,
		"zone":         record.Zone,
		"kind":         record.Kind,
		"status":       record.Status,
		"sessions":     record.Sessions,
		"confirmed":    record.Confirmed(),
		"uncertain":    record.Uncertain(),
		"triggered_at": record.TriggeredAt,
		"updated_at":   record.UpdatedAt,
	}
}
