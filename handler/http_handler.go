package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"trpc.group/trpc-go/trpc-go/log"
	thttp "trpc.group/trpc-go/trpc-go/http"
)

// HTTPHandler handles HTTP requests, producing Kafka messages and optionally storing in MySQL.
type HTTPHandler struct {
	mysqlHandler  *MySQLHandler
	kafkaProducer *KafkaProducer
}

func NewHTTPHandler(mysqlHandler *MySQLHandler, kafkaProducer *KafkaProducer) *HTTPHandler {
	return &HTTPHandler{
		mysqlHandler:  mysqlHandler,
		kafkaProducer: kafkaProducer,
	}
}

func (h *HTTPHandler) Register() {
	thttp.HandleFunc("/", h.HandleRoot)
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

func (h *HTTPHandler) HandleRoot(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return nil
	}

	// Read and decode JSON body
	var payload map[string]interface{}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read request body"})
		return nil
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return nil
	}

	// Produce to Kafka
	key := ""
	if id, ok := payload["id"].(string); ok {
		key = strings.TrimSpace(id)
	}
	if err := h.kafkaProducer.Send(r.Context(), key, payload); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to publish to Kafka"})
		return nil
	}

	// If env=prod, store in MySQL
	if envVal, ok := payload["env"].(string); ok && strings.ToLower(envVal) == "prod" {
		// Insert into MySQL
		// We store the whole JSON as a string in a table named simple_service with a json_data column
		jsonData, err := json.Marshal(payload)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to marshal JSON for DB"})
			return nil
		}
		record := &SimpleService{
			JsonData: string(jsonData),
		}
		_, err = h.mysqlHandler.InsertSimpleService(r.Context(), record)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to store in database"})
			return nil
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "ok"})
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
