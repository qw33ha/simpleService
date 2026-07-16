package handler

import (
	"context"
	"encoding/json"
	"fmt"

	mysql "trpc.group/trpc-go/trpc-database/mysql"
)

// MySQLHandler owns MySQL operations so transport and business handlers do not
// need to know how the database client is configured.
type MySQLHandler struct {
	client mysql.Client
}

func NewMySQLHandler() *MySQLHandler {
	return &MySQLHandler{
		client: mysql.NewClientProxy("trpc.qw33ha.simpleService.mysql"),
	}
}

// SimpleService represents the data structure stored in MySQL.
type SimpleService struct {
	ID   int64                  `db:"id" json:"id"`
	Data map[string]interface{} `db:"data" json:"data"`
}

// InsertSimpleService writes one simpleService record using parameterized values.
func (h *MySQLHandler) InsertSimpleService(ctx context.Context, record *SimpleService) (int64, error) {
	dataJSON, err := json.Marshal(record.Data)
	if err != nil {
		return 0, fmt.Errorf("marshal data: %w", err)
	}
	result, err := h.client.Exec(ctx,
		"INSERT INTO simple_service (data) VALUES (?)",
		dataJSON,
	)
	if err != nil {
		return 0, fmt.Errorf("insert simpleService: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read inserted simpleService id: %w", err)
	}
	return id, nil
}

// QuerySimpleService reads rows into dest. Callers must use placeholders
// for all external values supplied through args.
func (h *MySQLHandler) QuerySimpleService(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if err := h.client.QueryToStructs(ctx, dest, query, args...); err != nil {
		return fmt.Errorf("query simpleService: %w", err)
	}
	return nil
}

// ExecuteSimpleService runs a parameterized update or other write statement.
func (h *MySQLHandler) ExecuteSimpleService(ctx context.Context, query string, args ...interface{}) (int64, error) {
	return h.executeSimpleService(ctx, "execute", query, args...)
}

// DeleteSimpleService runs a parameterized delete statement.
func (h *MySQLHandler) DeleteSimpleService(ctx context.Context, query string, args ...interface{}) (int64, error) {
	return h.executeSimpleService(ctx, "delete", query, args...)
}

func (h *MySQLHandler) executeSimpleService(ctx context.Context, operation, query string, args ...interface{}) (int64, error) {
	result, err := h.client.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("%s simpleService: %w", operation, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read affected simpleService rows: %w", err)
	}
	return rows, nil
}
