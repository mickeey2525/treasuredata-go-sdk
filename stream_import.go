package treasuredata

import (
	"context"
	"fmt"
	"io"
)

// StreamImportService handles communication with the Stream Import API (api-import endpoints).
type StreamImportService struct {
	client *Client
}

// ImportTableOptions represents options for importing data into a table
type ImportTableOptions struct {
	Database string
	Table    string
	Data     io.Reader
	Format   string // e.g., "msgpack.gz"
}

// ImportResponse represents the response from a table import operation
type ImportResponse struct {
	Database string `json:"database,omitempty"`
	Table    string `json:"table,omitempty"`
	Status   string `json:"status,omitempty"`
}

// ImportTable imports data into a table using the Stream Import API.
// Data should be in msgpack.gz format. The io.Reader is streamed directly
// without being fully buffered in memory.
func (s *StreamImportService) ImportTable(ctx context.Context, database, table string, data io.Reader) (*ImportResponse, error) {
	u := fmt.Sprintf("v3/table/import/%s/%s", database, table)

	req, err := s.client.NewStreamImportRequest("POST", u, data)
	if err != nil {
		return nil, err
	}

	var resp ImportResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// ImportTableWithID imports data into a table with a unique ID for deduplication.
// Data should be in msgpack.gz format. The io.Reader is streamed directly
// without being fully buffered in memory.
func (s *StreamImportService) ImportTableWithID(ctx context.Context, database, table, uniqueID string, data io.Reader) (*ImportResponse, error) {
	u := fmt.Sprintf("v3/table/import_with_id/%s/%s/%s", database, table, uniqueID)

	req, err := s.client.NewStreamImportRequest("POST", u, data)
	if err != nil {
		return nil, err
	}

	var resp ImportResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
