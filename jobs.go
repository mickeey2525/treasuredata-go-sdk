package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

// JobsService handles communication with the job related methods of the Treasure Data API.
type JobsService struct {
	client *Client
}

// QueryField represents a query that can be either a string or an object
type QueryField struct {
	Value string
}

// UnmarshalJSON implements the json.Unmarshaler interface for QueryField
func (q *QueryField) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as string first
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		q.Value = str
		return nil
	}

	// If it's not a string, try to unmarshal as object and convert to string
	var obj map[string]interface{}
	if err := json.Unmarshal(data, &obj); err == nil {
		// Convert object back to JSON string
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		q.Value = string(jsonBytes)
		return nil
	}

	// If both fail, store the raw data as string
	q.Value = string(data)
	return nil
}

// MarshalJSON implements the json.Marshaler interface for QueryField
func (q QueryField) MarshalJSON() ([]byte, error) {
	return json.Marshal(q.Value)
}

// String returns the string representation of the query
func (q QueryField) String() string {
	return q.Value
}

// FlexibleString represents a field that can be either a string or number
type FlexibleString struct {
	Value *string
}

// UnmarshalJSON implements the json.Unmarshaler interface for FlexibleString
func (f *FlexibleString) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as string first
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		if str == "" || str == "null" {
			f.Value = nil
		} else {
			f.Value = &str
		}
		return nil
	}

	// Try to unmarshal as number and convert to string
	var num float64
	if err := json.Unmarshal(data, &num); err == nil {
		str := strconv.FormatFloat(num, 'f', -1, 64)
		f.Value = &str
		return nil
	}

	// If both fail, set to nil
	f.Value = nil
	return nil
}

// MarshalJSON implements the json.Marshaler interface for FlexibleString
func (f FlexibleString) MarshalJSON() ([]byte, error) {
	if f.Value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(*f.Value)
}

// Job represents a Treasure Data job
type Job struct {
	JobID                   string         `json:"job_id"`
	Type                    string         `json:"type"`
	Database                string         `json:"database"`
	Query                   QueryField     `json:"query"`
	Status                  string         `json:"status"`
	URL                     string         `json:"url"`
	UserName                string         `json:"user_name"`
	CreatedAt               TDTime         `json:"created_at"`
	UpdatedAt               TDTime         `json:"updated_at"`
	StartAt                 TDTime         `json:"start_at"`
	EndAt                   TDTime         `json:"end_at"`
	Duration                int            `json:"duration"`
	CPUTime                 *int           `json:"cpu_time"`
	ResultSize              int64          `json:"result_size"`
	NumRecords              int64          `json:"num_records"`
	Priority                int            `json:"priority"`
	RetryLimit              int            `json:"retry_limit"`
	Organization            *string        `json:"organization"`
	HiveResultSchema        string         `json:"hive_result_schema"`
	Result                  string         `json:"result"`
	LinkedResultExportJobID FlexibleString `json:"linked_result_export_job_id"`
	ResultExportTargetJobID FlexibleString `json:"result_export_target_job_id"`
	Debug                   *JobDebug      `json:"debug,omitempty"`
}

// JobDebug contains debug information for a job
type JobDebug struct {
	Cmdout string `json:"cmdout"`
	Stderr string `json:"stderr"`
}

// JobListResponse represents the response from the job list API
type JobListResponse struct {
	Count int   `json:"count"`
	From  *int  `json:"from"`
	To    *int  `json:"to"`
	Jobs  []Job `json:"jobs"`
}

// JobListOptions represents options for listing jobs
type JobListOptions struct {
	From   int    `url:"from,omitempty"`
	To     int    `url:"to,omitempty"`
	Status string `url:"status,omitempty"`
	Slow   bool   `url:"slow,omitempty"`
}

// List returns a list of jobs
func (s *JobsService) List(ctx context.Context, opts *JobListOptions) (*JobListResponse, error) {
	u := fmt.Sprintf("%s/job/list", apiVersion)

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

	var resp JobListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// Get returns a specific job by ID
func (s *JobsService) Get(ctx context.Context, jobID string) (*Job, error) {
	u := fmt.Sprintf("%s/job/show/%s", apiVersion, jobID)

	req, err := s.client.NewRequest("GET", u, nil)
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

// JobStatus represents job status information
type JobStatus struct {
	Status     string `json:"status"`
	CPUTime    *int   `json:"cpu_time"`
	ResultSize int64  `json:"result_size"`
	Duration   int    `json:"duration"`
	JobID      string `json:"job_id"`
	CreatedAt  TDTime `json:"created_at"`
	UpdatedAt  TDTime `json:"updated_at"`
	StartAt    TDTime `json:"start_at"`
	EndAt      TDTime `json:"end_at"`
	NumRecords int64  `json:"num_records"`
}

// Status returns the status of a job
func (s *JobsService) Status(ctx context.Context, jobID string) (*JobStatus, error) {
	u := fmt.Sprintf("%s/job/status/%s", apiVersion, jobID)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var status JobStatus
	_, err = s.client.Do(ctx, req, &status)
	if err != nil {
		return nil, err
	}

	return &status, nil
}

// StatusByDomainKey returns the status of a job by domain key
func (s *JobsService) StatusByDomainKey(ctx context.Context, domainKey string) (*JobStatus, error) {
	u := fmt.Sprintf("%s/job/status_by_domain_key/%s", apiVersion, domainKey)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var status JobStatus
	_, err = s.client.Do(ctx, req, &status)
	if err != nil {
		return nil, err
	}

	return &status, nil
}

// Kill kills a running job
func (s *JobsService) Kill(ctx context.Context, jobID string) error {
	u := fmt.Sprintf("%s/job/kill/%s", apiVersion, jobID)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// ResultExportOptions represents options for exporting job results
type ResultExportOptions struct {
	Result           string                 `json:"result,omitempty"`
	ResultConnection string                 `json:"result_connection,omitempty"`
	ResultSettings   map[string]interface{} `json:"result_settings,omitempty"`
}

// ResultExport exports the results of a job
func (s *JobsService) ResultExport(ctx context.Context, jobID string, opts *ResultExportOptions) (*Job, error) {
	u := fmt.Sprintf("%s/job/result_export/%s", apiVersion, jobID)

	req, err := s.client.NewRequest("POST", u, opts)
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
