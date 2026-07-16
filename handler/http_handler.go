package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"trpc.group/trpc-go/trpc-go/log"
	thttp "trpc.group/trpc-go/trpc-go/http"
)

// HTTPHandler handles HTTP requests, producing Kafka messages and optionally storing in MySQL.
type HTTPHandler struct {
	producer *KafkaProducer
	mysql   *MySQLHandler
}

func NewHTTPHandler(producer *KafkaProducer, mysql *MySQLHandler) *HTTPHandler {
	return &HTTPHandler{producer: producer, mysql: mysql}
}

func (h *HTTPHandler) Register() {
	thttp.HandleFunc("/", h.HandleRequest)
	thttp.HandleFunc("/is_healthy", h.Health)
}

func (h *HTTPHandler) Health(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return nil
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

func (h *HTTPHandler) HandleRequest(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return nil
	}

	var payload map[string]interface{}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return nil
	}

	// Produce to Kafka
	key := ""
	if k, ok := payload["key"].(string); ok {
		key = strings.TrimSpace(k)
	}
	if err := h.producer.Send(r.Context(), key, payload); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return nil
	}

	// If env=prod, store in MySQL
	if envVal, ok := payload["env"].(string); ok && strings.ToLower(envVal) == "prod" {
		// Prepare insert
		record := &SimpleService{
			Data: payload,
		}
		// Insert record
		id, err := h.mysql.InsertSimpleService(r.Context(), record)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to store in database"})
			return nil
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "stored in database", "id": id})
		return nil
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"message": "published to Kafka"})
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if body != nil {
		if err := json.NewEncoder(w).Encode(body); err != nil {
			log.Errorf("writeJSON encode failed: %v", err)
		}
	}
}
