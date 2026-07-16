package handler

import (
	"encoding/json"
	"io"
	"net/http"

	"trpc.group/trpc-go/trpc-go/log"
	thttp "trpc.group/trpc-go/trpc-go/http"
)

// HTTPHandler handles HTTP requests for the simple-service.
type HTTPHandler struct {
	producer *KafkaProducer
	db       *MySQLHandler
}

func NewHTTPHandler() *HTTPHandler {
	return &HTTPHandler{
		producer: NewKafkaProducer(),
		db:       NewMySQLHandler(),
	}
}

func (h *HTTPHandler) Register() {
	thttp.HandleFunc("/is_healthy", h.Health)
	thttp.HandleFunc("/", h.HandleRoot)
}

// Health returns 200 OK for health checks.
func (h *HTTPHandler) Health(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return nil
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

// HandleRoot handles POST / requests with JSON body.
// It sends the entire JSON body as a serialized Kafka message with metadata.
// If the JSON contains "env"="prod", it stores user and email fields in MySQL.
func (h *HTTPHandler) HandleRoot(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return nil
	}

	// Read and decode JSON body
	var payload map[string]interface{}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return nil
	}

	// Prepare Kafka message with metadata
	message := map[string]interface{}{
		"metadata": map[string]interface{}{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
		},
		"payload": payload,
	}

	// Send to Kafka
	key := ""
	if k, ok := payload["user"].(string); ok {
		key = k
	}
	if err := h.producer.Send(r.Context(), key, message); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to send to Kafka: " + err.Error()})
		return nil
	}

	// If env=prod, write user and email to MySQL
	if envVal, ok := payload["env"].(string); ok && envVal == "prod" {
		user, uok := payload["user"].(string)
		email, eok := payload["email"].(string)
		if !uok || !eok || user == "" || email == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user and email fields are required for env=prod"})
			return nil
		}

		// Insert into database
		_, err := h.db.client.Exec(r.Context(),
			"INSERT INTO users (user, email) VALUES (?, ?)",
			user, email,
		)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to write to database: " + err.Error()})
			return nil
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"message": "processed"})
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Errorf("writeJSON encode failed: %v", err)
	}
}
