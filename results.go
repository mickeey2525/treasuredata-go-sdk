package treasuredata

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// ResultsService handles communication with the result related methods of the Treasure Data API.
type ResultsService struct {
	client *Client
}

// ResultFormat represents the format of query results
type ResultFormat string

const (
	// ResultFormatJSON returns results in JSON format
	ResultFormatJSON ResultFormat = "json"
	// ResultFormatCSV returns results in CSV format
	ResultFormatCSV ResultFormat = "csv"
	// ResultFormatTSV returns results in TSV format
	ResultFormatTSV ResultFormat = "tsv"
	// ResultFormatJSONL returns results in JSONL (JSON Lines) format
	ResultFormatJSONL ResultFormat = "jsonl"
	// ResultFormatMessagePack returns results in MessagePack format
	ResultFormatMessagePack ResultFormat = "msgpack"
)

// GetResultOptions represents options for retrieving job results
type GetResultOptions struct {
	Format ResultFormat `url:"format,omitempty"`
	Limit  int          `url:"limit,omitempty"`
}

// GetResult retrieves the results of a completed job
func (s *ResultsService) GetResult(ctx context.Context, jobID string, opts *GetResultOptions) (io.ReadCloser, error) {
	u := fmt.Sprintf("%s/job/result/%s", apiVersion, jobID)

	if opts != nil {
		var err error
		u, err = addOptions(u, opts)
		if err != nil {
			return nil, err
		}
	}

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	if err := CheckResponse(resp); err != nil {
		resp.Body.Close()
		return nil, err
	}

	return resp.Body, nil
}

// GetResultJSON retrieves job results and decodes them as JSON
func (s *ResultsService) GetResultJSON(ctx context.Context, jobID string, v interface{}) error {
	opts := &GetResultOptions{Format: ResultFormatJSON}

	body, err := s.GetResult(ctx, jobID, opts)
	if err != nil {
		return err
	}
	defer body.Close()

	return json.NewDecoder(body).Decode(v)
}

// GetResultJSONL retrieves job results in JSONL format and returns a scanner
func (s *ResultsService) GetResultJSONL(ctx context.Context, jobID string) (*JSONLScanner, error) {
	opts := &GetResultOptions{Format: ResultFormatJSONL}

	body, err := s.GetResult(ctx, jobID, opts)
	if err != nil {
		return nil, err
	}

	return &JSONLScanner{
		scanner: bufio.NewScanner(body),
		closer:  body,
	}, nil
}

// JSONLScanner helps iterate over JSONL results
type JSONLScanner struct {
	scanner *bufio.Scanner
	closer  io.Closer
}

// Scan advances to the next line
func (j *JSONLScanner) Scan() bool {
	return j.scanner.Scan()
}

// Bytes returns the raw bytes of the current line
func (j *JSONLScanner) Bytes() []byte {
	return j.scanner.Bytes()
}

// Text returns the string of the current line
func (j *JSONLScanner) Text() string {
	return j.scanner.Text()
}

// Decode decodes the current line into v
func (j *JSONLScanner) Decode(v interface{}) error {
	return json.Unmarshal(j.scanner.Bytes(), v)
}

// Err returns any error from the scanner
func (j *JSONLScanner) Err() error {
	return j.scanner.Err()
}

// Close closes the underlying reader
func (j *JSONLScanner) Close() error {
	return j.closer.Close()
}

// Result represents a result URL configuration
type Result struct {
	Name     string                 `json:"name"`
	URL      string                 `json:"url"`
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Settings map[string]interface{} `json:"settings,omitempty"`
}

// ResultListResponse represents the response from listing results
type ResultListResponse struct {
	Results []Result `json:"results"`
}

// ListResults lists all result configurations
func (s *ResultsService) ListResults(ctx context.Context) ([]Result, error) {
	u := fmt.Sprintf("%s/result/list", apiVersion)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp ResultListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Results, nil
}

// CreateResult creates a new result configuration
func (s *ResultsService) CreateResult(ctx context.Context, name, url string, settings map[string]interface{}) (*Result, error) {
	u := fmt.Sprintf("%s/result/create/%s", apiVersion, name)

	body := map[string]interface{}{
		"url": url,
	}
	if settings != nil {
		body["settings"] = settings
	}

	req, err := s.client.NewRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var result Result
	_, err = s.client.Do(ctx, req, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// DeleteResult deletes a result configuration
func (s *ResultsService) DeleteResult(ctx context.Context, name string) error {
	u := fmt.Sprintf("%s/result/delete/%s", apiVersion, name)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}
