package treasuredata

import (
	"context"
	"fmt"
)

// QueriesService handles communication with the query related methods of the Treasure Data API.
type QueriesService struct {
	client *Client
}

// QueryType represents the type of query engine
type QueryType string

const (
	// QueryTypeHive represents Hive query engine
	QueryTypeHive QueryType = "hive"
	// QueryTypeTrino represents Trino query engine
	QueryTypeTrino QueryType = "trino"
	// QueryTypePresto is deprecated, use QueryTypeTrino instead
	QueryTypePresto QueryType = "presto"
)

// IssueQueryOptions represents options for issuing a query
type IssueQueryOptions struct {
	Query         string `json:"query"`
	Priority      int    `json:"priority,omitempty"`
	RetryLimit    int    `json:"retry_limit,omitempty"`
	Result        string `json:"result,omitempty"`
	DomainKey     string `json:"domain_key,omitempty"`
	PoolName      string `json:"pool_name,omitempty"`
	Type          string `json:"type,omitempty"`
	EngineVersion string `json:"engine_version,omitempty"`
}

// IssueQueryResponse represents the response from issuing a query
type IssueQueryResponse struct {
	Job      string `json:"job"`
	JobID    string `json:"job_id"`
	Database string `json:"database"`
}

// Issue submits a new query job
func (s *QueriesService) Issue(ctx context.Context, queryType QueryType, database string, opts *IssueQueryOptions) (*IssueQueryResponse, error) {
	u := fmt.Sprintf("%s/job/issue/%s/%s", apiVersion, queryType, database)

	req, err := s.client.NewRequest("POST", u, opts)
	if err != nil {
		return nil, err
	}

	var resp IssueQueryResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
