package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// TablesService handles communication with the table related methods of the Treasure Data API.
type TablesService struct {
	client *Client
}

// FlexibleInt64 represents a field that can be either a string or int64
type FlexibleInt64 struct {
	Value *int64
}

// UnmarshalJSON implements the json.Unmarshaler interface for FlexibleInt64
func (f *FlexibleInt64) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as int64 first
	var num int64
	if err := json.Unmarshal(data, &num); err == nil {
		f.Value = &num
		return nil
	}

	// Try to unmarshal as string and convert to int64
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		if str == "" || str == "null" {
			f.Value = nil
			return nil
		}
		if num, err := strconv.ParseInt(str, 10, 64); err == nil {
			f.Value = &num
			return nil
		}
	}

	// If both fail, set to nil
	f.Value = nil
	return nil
}

// MarshalJSON implements the json.Marshaler interface for FlexibleInt64
func (f FlexibleInt64) MarshalJSON() ([]byte, error) {
	if f.Value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(*f.Value)
}

// Table represents a Treasure Data table
type Table struct {
	ID                   int64         `json:"id"`
	Name                 string        `json:"name"`
	Database             string        `json:"database,omitempty"`
	Type                 string        `json:"type"`
	Count                int64         `json:"count"`
	CreatedAt            TDTime        `json:"created_at"`
	UpdatedAt            TDTime        `json:"updated_at"`
	EstimatedStorageSize int64         `json:"estimated_storage_size"`
	LastLogTimestamp     FlexibleInt64 `json:"last_log_timestamp"`
	DeleteProtected      bool          `json:"delete_protected"`
	Schema               string        `json:"schema"`
	ExpireDays           *int          `json:"expire_days"`
	IncludeV             bool          `json:"include_v"`
	CounterUpdatedAt     *TDTime       `json:"counter_updated_at"`
}

// TableListResponse represents the response from the table list API
type TableListResponse struct {
	Database string  `json:"database"`
	Tables   []Table `json:"tables"`
}

// TableCreateResponse represents the response from table creation
type TableCreateResponse struct {
	Database string `json:"database"`
	Table    string `json:"table"`
	Type     string `json:"type"`
}

// List returns all tables in a database
func (s *TablesService) List(ctx context.Context, database string) ([]Table, error) {
	u := fmt.Sprintf("%s/table/list/%s", apiVersion, database)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp TableListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Tables, nil
}

// Get returns a specific table
func (s *TablesService) Get(ctx context.Context, database, table string) (*Table, error) {
	u := fmt.Sprintf("%s/table/show/%s/%s", apiVersion, database, table)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var t Table
	_, err = s.client.Do(ctx, req, &t)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// Create creates a new table
func (s *TablesService) Create(ctx context.Context, database, table string, tableType string) (*TableCreateResponse, error) {
	if tableType == "" {
		tableType = "log"
	}

	u := fmt.Sprintf("%s/table/create/%s/%s/%s", apiVersion, database, table, tableType)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var resp TableCreateResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// Delete deletes a table
func (s *TablesService) Delete(ctx context.Context, database, table string) error {
	u := fmt.Sprintf("%s/table/delete/%s/%s", apiVersion, database, table)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete table: %s.%s", database, table)
	}

	return nil
}

// Swap swaps the contents of two tables
func (s *TablesService) Swap(ctx context.Context, database, table1, table2 string) error {
	u := fmt.Sprintf("%s/table/swap/%s/%s/%s", apiVersion, database, table1, table2)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to swap tables: %s.%s <-> %s.%s", database, table1, database, table2)
	}

	return nil
}

// Rename renames a table
func (s *TablesService) Rename(ctx context.Context, database, oldName, newName string) error {
	u := fmt.Sprintf("%s/table/rename/%s/%s/%s", apiVersion, database, oldName, newName)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to rename table: %s.%s -> %s.%s", database, oldName, database, newName)
	}

	return nil
}

// UpdateOptions represents options for updating a table
type UpdateOptions struct {
	Schema     string `json:"schema,omitempty"`
	ExpireDays *int   `json:"expire_days,omitempty"`
}

// Update updates table properties
func (s *TablesService) Update(ctx context.Context, database, table string, opts *UpdateOptions) error {
	u := fmt.Sprintf("%s/table/update/%s/%s", apiVersion, database, table)

	req, err := s.client.NewRequest("POST", u, opts)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to update table: %s.%s", database, table)
	}

	return nil
}
