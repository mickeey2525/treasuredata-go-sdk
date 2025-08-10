package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// CreateSegment creates a new customer segment within an audience
func (s *CDPService) CreateSegment(ctx context.Context, audienceID, name, description, query string) (*CDPSegment, error) {
	u := fmt.Sprintf("audiences/%s/segments", audienceID)

	body := map[string]string{
		"name":        name,
		"description": description,
		"query":       query,
	}

	req, err := s.client.NewCDPRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var segment CDPSegment
	_, err = s.client.Do(ctx, req, &segment)
	if err != nil {
		return nil, err
	}

	return &segment, nil
}

// ListSegments returns a list of customer segments for an audience
func (s *CDPService) ListSegments(ctx context.Context, audienceID string, opts *CDPSegmentListOptions) (*CDPSegmentListResponse, error) {
	u := fmt.Sprintf("audiences/%s/segments", audienceID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var segments []CDPSegment
	_, err = s.client.Do(ctx, req, &segments)
	if err != nil {
		return nil, err
	}

	return &CDPSegmentListResponse{
		Segments: segments,
		Total:    int64(len(segments)),
	}, nil
}

// ListSegmentsInFolder returns a list of segments in a specific folder
func (s *CDPService) ListSegmentsInFolder(ctx context.Context, audienceID, folderID string, opts *CDPSegmentListOptions) (*CDPSegmentListResponse, error) {
	u := fmt.Sprintf("audiences/%s/folders/%s/segments", audienceID, folderID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var segments []CDPSegment
	_, err = s.client.Do(ctx, req, &segments)
	if err != nil {
		return nil, err
	}

	return &CDPSegmentListResponse{
		Segments: segments,
		Total:    int64(len(segments)),
	}, nil
}

// GetSegment retrieves a specific segment by ID from an audience
func (s *CDPService) GetSegment(ctx context.Context, audienceID, segmentID string) (*CDPSegment, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s", audienceID, segmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var segment CDPSegment
	_, err = s.client.Do(ctx, req, &segment)
	if err != nil {
		return nil, err
	}

	return &segment, nil
}

// UpdateSegment updates a customer segment within an audience
func (s *CDPService) UpdateSegment(ctx context.Context, audienceID, segmentID string, updates map[string]string) (*CDPSegment, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s", audienceID, segmentID)

	req, err := s.client.NewCDPRequest("PUT", u, updates)
	if err != nil {
		return nil, err
	}

	var segment CDPSegment
	_, err = s.client.Do(ctx, req, &segment)
	if err != nil {
		return nil, err
	}

	return &segment, nil
}

// DeleteSegment deletes a customer segment from an audience
func (s *CDPService) DeleteSegment(ctx context.Context, audienceID, segmentID string) error {
	u := fmt.Sprintf("audiences/%s/segments/%s", audienceID, segmentID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete segment: %s", segmentID)
	}

	return nil
}

// GetSegmentFolders retrieves segments in a specific folder
func (s *CDPService) GetSegmentFolders(ctx context.Context, folderID string) (*CDPSegmentFolderListResponse, error) {
	u := fmt.Sprintf("segment_folders/%s/segments", folderID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var folders []CDPSegmentFolder
	_, err = s.client.Do(ctx, req, &folders)
	if err != nil {
		return nil, err
	}

	return &CDPSegmentFolderListResponse{
		Folders: folders,
		Total:   int64(len(folders)),
	}, nil
}

// CreateSegmentQuery creates a new query for a segment
func (s *CDPService) CreateSegmentQuery(ctx context.Context, audienceID, query string) (*CDPSegmentQuery, error) {
	u := fmt.Sprintf("audiences/%s/segments/queries", audienceID)

	body := map[string]string{
		"query": query,
	}

	req, err := s.client.NewCDPRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var segmentQuery CDPSegmentQuery
	_, err = s.client.Do(ctx, req, &segmentQuery)
	if err != nil {
		return nil, err
	}

	return &segmentQuery, nil
}

// GetSegmentSQL retrieves SQL from segment rules
func (s *CDPService) GetSegmentSQL(ctx context.Context, audienceID string, segmentRules interface{}) (*CDPSegmentQuery, error) {
	u := fmt.Sprintf("audiences/%s/segments/query", audienceID)

	req, err := s.client.NewCDPRequest("POST", u, segmentRules)
	if err != nil {
		return nil, err
	}

	var segmentQuery CDPSegmentQuery
	_, err = s.client.Do(ctx, req, &segmentQuery)
	if err != nil {
		return nil, err
	}

	return &segmentQuery, nil
}

// GetSegmentQueryStatus retrieves the status of a segment query
func (s *CDPService) GetSegmentQueryStatus(ctx context.Context, audienceID, queryID string) (*CDPSegmentQuery, error) {
	u := fmt.Sprintf("audiences/%s/segments/queries/%s", audienceID, queryID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var segmentQuery CDPSegmentQuery
	_, err = s.client.Do(ctx, req, &segmentQuery)
	if err != nil {
		return nil, err
	}

	return &segmentQuery, nil
}

// KillSegmentQuery kills a running segment query
func (s *CDPService) KillSegmentQuery(ctx context.Context, audienceID, queryID string) error {
	u := fmt.Sprintf("audiences/%s/segments/queries/%s/kill", audienceID, queryID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to kill segment query: %s", queryID)
	}

	return nil
}

// GetSegmentQueryCustomers retrieves customers from a segment query
func (s *CDPService) GetSegmentQueryCustomers(ctx context.Context, audienceID, queryID string, opts *CDPSegmentCustomerListOptions) (*CDPSegmentCustomerListResponse, error) {
	u := fmt.Sprintf("audiences/%s/segments/queries/%s/customers", audienceID, queryID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var customers []CDPSegmentCustomer
	_, err = s.client.Do(ctx, req, &customers)
	if err != nil {
		return nil, err
	}

	return &CDPSegmentCustomerListResponse{
		Customers: customers,
		Total:     int64(len(customers)),
	}, nil
}

// GetSegmentStatistics retrieves statistics for a segment
func (s *CDPService) GetSegmentStatistics(ctx context.Context, audienceID, segmentID string) ([]CDPSegmentStatisticsPoint, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/statistics", audienceID, segmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var statistics []CDPSegmentStatisticsPoint
	_, err = s.client.Do(ctx, req, &statistics)
	if err != nil {
		return nil, err
	}

	return statistics, nil
}

// Entity Segment Operations (JSON:API format)

// CreateEntitySegment creates a new entity segment using JSON:API format
func (s *CDPService) CreateEntitySegment(ctx context.Context, name, description, segmentType string, parentFolderID string, attributes map[string]interface{}) (*CDPJSONAPIResponse, error) {
	u := "entities/segments"

	// Build JSON:API request
	requestData := CDPJSONAPIResource{
		Type: segmentType, // e.g., "batch-segment", "realtime-segment"
		Attributes: map[string]interface{}{
			"name":        name,
			"description": description,
		},
		Relationships: map[string]interface{}{
			"parentFolder": map[string]interface{}{
				"data": map[string]interface{}{
					"id":   parentFolderID,
					"type": "folder-segment",
				},
			},
		},
	}

	// Add additional attributes
	for key, value := range attributes {
		requestData.Attributes[key] = value
	}

	request := CDPJSONAPIRequest{Data: requestData}

	req, err := s.client.NewCDPJSONAPIRequest("POST", u, request)
	if err != nil {
		return nil, err
	}

	var response CDPJSONAPIResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetEntitySegment retrieves a specific entity segment by ID using JSON:API format
func (s *CDPService) GetEntitySegment(ctx context.Context, segmentID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/segments/%s", segmentID)

	req, err := s.client.NewCDPJSONAPIRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJSONAPIResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// ListEntitySegments retrieves a list of entity segments using JSON:API format
func (s *CDPService) ListEntitySegments(ctx context.Context) (*CDPJSONAPIListResponse, error) {
	u := "entities/segments"

	req, err := s.client.NewCDPJSONAPIRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJSONAPIListResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// UpdateEntitySegment updates an existing entity segment using JSON:API format
func (s *CDPService) UpdateEntitySegment(ctx context.Context, segmentID string, updates map[string]interface{}) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/segments/%s", segmentID)

	requestData := CDPJSONAPIResource{
		ID:         segmentID,
		Type:       "batch-segment", // Default, could be parameterized
		Attributes: updates,
	}

	request := CDPJSONAPIRequest{Data: requestData}

	req, err := s.client.NewCDPJSONAPIRequest("PUT", u, request)
	if err != nil {
		return nil, err
	}

	var response CDPJSONAPIResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// DeleteEntitySegment deletes an entity segment using JSON:API format
func (s *CDPService) DeleteEntitySegment(ctx context.Context, segmentID string) error {
	u := fmt.Sprintf("entities/segments/%s", segmentID)

	req, err := s.client.NewCDPJSONAPIRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to delete entity segment: %s", segmentID)
	}

	return nil
}

// ListParentSegments lists all parent segments
func (c *CDPService) ListParentSegments(ctx context.Context) (*CDPParentSegmentListResponse, error) {
	path := "entities/parent_segments"

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPParentSegmentListResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetParentSegment retrieves a specific parent segment by ID
func (c *CDPService) GetParentSegment(ctx context.Context, parentSegmentID string) (*CDPParentSegmentResponse, error) {
	path := fmt.Sprintf("entities/parent_segments/%s", parentSegmentID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPParentSegmentResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}
