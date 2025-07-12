package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// DatabasesService handles communication with the database related methods of the Treasure Data API.
type DatabasesService struct {
	client *Client
}

// Database represents a Treasure Data database
type Database struct {
	Name            string  `json:"name"`
	CreatedAt       TDTime  `json:"created_at"`
	UpdatedAt       TDTime  `json:"updated_at"`
	Count           int64   `json:"count"`
	Organization    *string `json:"organization"`
	Permission      string  `json:"permission"`
	DeleteProtected bool    `json:"delete_protected"`
	ID              string  `json:"id,omitempty"`
}

// DatabaseListResponse represents the response from the database list API
type DatabaseListResponse struct {
	Databases []Database `json:"databases"`
}

// List returns all databases
func (s *DatabasesService) List(ctx context.Context) ([]Database, error) {
	u := fmt.Sprintf("%s/database/list", apiVersion)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp DatabaseListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Databases, nil
}

// Get returns a specific database by name
func (s *DatabasesService) Get(ctx context.Context, name string) (*Database, error) {
	u := fmt.Sprintf("%s/database/show/%s", apiVersion, name)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var db Database
	_, err = s.client.Do(ctx, req, &db)
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// Create creates a new database
func (s *DatabasesService) Create(ctx context.Context, name string) (*Database, error) {
	u := fmt.Sprintf("%s/database/create/%s", apiVersion, name)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var db Database
	_, err = s.client.Do(ctx, req, &db)
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// Delete deletes a database
func (s *DatabasesService) Delete(ctx context.Context, name string) error {
	u := fmt.Sprintf("%s/database/delete/%s", apiVersion, name)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete database: %s", name)
	}

	return nil
}
