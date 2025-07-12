package treasuredata

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
)

// BulkImportService handles communication with the bulk import related methods of the Treasure Data API.
type BulkImportService struct {
	client *Client
}

// BulkImport represents a bulk import session
type BulkImport struct {
	Name         string `json:"name"`
	Database     string `json:"database"`
	Table        string `json:"table"`
	Status       string `json:"status"`
	JobID        string `json:"job_id"`
	ValidRecords int64  `json:"valid_records"`
	ErrorRecords int64  `json:"error_records"`
	ValidParts   int    `json:"valid_parts"`
	ErrorParts   int    `json:"error_parts"`
	UploadFrozen bool   `json:"upload_frozen"`
	CreatedAt    TDTime `json:"created_at,omitempty"`
}

// BulkImportListResponse represents the response from listing bulk imports
type BulkImportListResponse struct {
	BulkImports []BulkImport `json:"bulk_imports"`
}

// Create creates a new bulk import session
func (s *BulkImportService) Create(ctx context.Context, name, database, table string) error {
	u := fmt.Sprintf("%s/bulk_import/create/%s/%s/%s", apiVersion, name, database, table)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// UploadPart uploads a part to a bulk import session
func (s *BulkImportService) UploadPart(ctx context.Context, name, partName string, data io.Reader) error {
	u := fmt.Sprintf("%s/bulk_import/upload_part/%s/%s", apiVersion, name, partName)

	// Create multipart writer
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Create form file field
	part, err := writer.CreateFormFile("file", partName)
	if err != nil {
		return err
	}

	// Copy data to form field
	if _, err := io.Copy(part, data); err != nil {
		return err
	}

	// Close multipart writer
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := s.client.NewRequest("PUT", u, &buf)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// Delete deletes a bulk import session
func (s *BulkImportService) Delete(ctx context.Context, name string) error {
	u := fmt.Sprintf("%s/bulk_import/delete/%s", apiVersion, name)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// Show returns details of a bulk import session
func (s *BulkImportService) Show(ctx context.Context, name string) (*BulkImport, error) {
	u := fmt.Sprintf("%s/bulk_import/show/%s", apiVersion, name)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var bi BulkImport
	_, err = s.client.Do(ctx, req, &bi)
	if err != nil {
		return nil, err
	}

	return &bi, nil
}

// List returns all bulk import sessions
func (s *BulkImportService) List(ctx context.Context) ([]BulkImport, error) {
	u := fmt.Sprintf("%s/bulk_import/list", apiVersion)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp BulkImportListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return resp.BulkImports, nil
}

// Commit commits a bulk import session
func (s *BulkImportService) Commit(ctx context.Context, name string) error {
	u := fmt.Sprintf("%s/bulk_import/commit/%s", apiVersion, name)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// Freeze freezes a bulk import session
func (s *BulkImportService) Freeze(ctx context.Context, name string) error {
	u := fmt.Sprintf("%s/bulk_import/freeze/%s", apiVersion, name)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// Unfreeze unfreezes a bulk import session
func (s *BulkImportService) Unfreeze(ctx context.Context, name string) error {
	u := fmt.Sprintf("%s/bulk_import/unfreeze/%s", apiVersion, name)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// Perform performs a bulk import job
func (s *BulkImportService) Perform(ctx context.Context, name string) (*Job, error) {
	u := fmt.Sprintf("%s/bulk_import/perform/%s", apiVersion, name)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var job Job
	_, err = s.client.Do(ctx, req, &job)
	if err != nil {
		return nil, err
	}

	return &job, nil
}

// BulkImportPart represents a part in a bulk import session
type BulkImportPart struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// BulkImportPartListResponse represents the response from listing parts
type BulkImportPartListResponse struct {
	Parts []BulkImportPart `json:"parts"`
}

// ListParts lists all parts in a bulk import session
func (s *BulkImportService) ListParts(ctx context.Context, name string) ([]BulkImportPart, error) {
	u := fmt.Sprintf("%s/bulk_import/list_parts/%s", apiVersion, name)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp BulkImportPartListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Parts, nil
}
