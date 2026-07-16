package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"trpc.group/trpc-go/trpc-go/log"
	thttp "trpc.group/trpc-go/trpc-go/http"
)

// HTTPHandler handles HTTP requests and integrates Kafka and MySQL operations.
type HTTPHandler struct {
	producer *KafkaProducer
	mysql   *MySQLHandler
}

func NewHTTPHandler(producer *KafkaProducer, mysql *MySQLHandler) *HTTPHandler {
	return &HTTPHandler{
		producer: producer,
		mysql:   mysql,
	}
}

func (h *HTTPHandler) Register() {
	thttp.HandleFunc("/is_healthy", h.Health)
	thttp.HandleFunc("/", h.HandleRequest)
}

// Health returns a simple health check response.
func (h *HTTPHandler) Health(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return nil
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

// HandleRequest processes POST requests with JSON body, sends to Kafka, and conditionally stores in MySQL.
func (h *HTTPHandler) HandleRequest(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return nil
	}

	// Read and decode JSON body
	var payload map[string]interface{}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20)) // limit 1MB
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return nil
	}

	// Validate payload is not empty
	if len(payload) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "empty JSON body"})
		return nil
	}

	// Send payload to Kafka
	key := ""
	if k, ok := payload["key"].(string); ok {
		key = strings.TrimSpace(k)
	}
	if err := h.producer.Send(r.Context(), key, payload); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to send to Kafka: " + err.Error()})
		return nil
	}

	// If env == "prod", store in MySQL
	if envVal, ok := payload["env"].(string); ok && strings.ToLower(envVal) == "prod" {
		// Insert into MySQL
		// We store the entire JSON as a string in a table with columns id (auto), data (JSON text)
		jsonData, err := json.Marshal(payload)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to marshal JSON for DB"})
			return nil
		}

		record := &SimpleService{
			Data: string(jsonData),
		}
		id, err := h.mysql.InsertSimpleService(r.Context(), record)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to store in database: " + err.Error()})
			return nil
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "stored in database", "id": id})
		return nil
	}

	// Otherwise, just acknowledge
	writeJSON(w, http.StatusAccepted, map[string]string{"message": "sent to Kafka"})
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
