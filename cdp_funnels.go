package treasuredata

import (
	"context"
	"fmt"
)

// Legacy Funnel API Operations

// ListFunnels retrieves a list of funnels for an audience (legacy API)
func (s *CDPService) ListFunnels(ctx context.Context, audienceID string) ([]CDPFunnel, error) {
	u := fmt.Sprintf("audiences/%s/funnels", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var funnels []CDPFunnel
	_, err = s.client.Do(ctx, req, &funnels)
	if err != nil {
		return nil, err
	}

	return funnels, nil
}

// CreateFunnel creates a new funnel (legacy API)
func (s *CDPService) CreateFunnel(ctx context.Context, audienceID string, params CDPFunnelCreateRequest) (*CDPFunnel, error) {
	if len(params.Stages) < 3 || len(params.Stages) > 8 {
		return nil, fmt.Errorf("funnel must have between 3 and 8 stages, got %d", len(params.Stages))
	}

	u := fmt.Sprintf("audiences/%s/funnels", audienceID)

	req, err := s.client.NewCDPRequest("POST", u, params)
	if err != nil {
		return nil, err
	}

	var funnel CDPFunnel
	_, err = s.client.Do(ctx, req, &funnel)
	if err != nil {
		return nil, err
	}

	return &funnel, nil
}

// GetFunnel retrieves a specific funnel by ID (legacy API)
func (s *CDPService) GetFunnel(ctx context.Context, audienceID, funnelID string) (*CDPFunnel, error) {
	u := fmt.Sprintf("audiences/%s/funnels/%s", audienceID, funnelID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var funnel CDPFunnel
	_, err = s.client.Do(ctx, req, &funnel)
	if err != nil {
		return nil, err
	}

	return &funnel, nil
}

// UpdateFunnel updates an existing funnel (legacy API)
func (s *CDPService) UpdateFunnel(ctx context.Context, audienceID, funnelID string, params CDPFunnelCreateRequest) (*CDPFunnel, error) {
	if len(params.Stages) < 3 || len(params.Stages) > 8 {
		return nil, fmt.Errorf("funnel must have between 3 and 8 stages, got %d", len(params.Stages))
	}

	u := fmt.Sprintf("audiences/%s/funnels/%s", audienceID, funnelID)

	req, err := s.client.NewCDPRequest("PUT", u, params)
	if err != nil {
		return nil, err
	}

	var funnel CDPFunnel
	_, err = s.client.Do(ctx, req, &funnel)
	if err != nil {
		return nil, err
	}

	return &funnel, nil
}

// DeleteFunnel deletes a funnel (legacy API)
func (s *CDPService) DeleteFunnel(ctx context.Context, audienceID, funnelID string) (*CDPFunnel, error) {
	u := fmt.Sprintf("audiences/%s/funnels/%s", audienceID, funnelID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return nil, err
	}

	var funnel CDPFunnel
	_, err = s.client.Do(ctx, req, &funnel)
	if err != nil {
		return nil, err
	}

	return &funnel, nil
}

// CloneFunnel clones an existing funnel (legacy API)
func (s *CDPService) CloneFunnel(ctx context.Context, audienceID, funnelID string, params CDPFunnelCloneRequest) (*CDPFunnel, error) {
	u := fmt.Sprintf("audiences/%s/funnels/%s/clone", audienceID, funnelID)

	req, err := s.client.NewCDPRequest("POST", u, params)
	if err != nil {
		return nil, err
	}

	var funnel CDPFunnel
	_, err = s.client.Do(ctx, req, &funnel)
	if err != nil {
		return nil, err
	}

	return &funnel, nil
}

// GetFunnelStatistics retrieves population statistics for a funnel (legacy API)
func (s *CDPService) GetFunnelStatistics(ctx context.Context, audienceID, funnelID string, limit *int64) (*CDPFunnelStatistic, error) {
	u := fmt.Sprintf("audiences/%s/funnels/%s/statistics", audienceID, funnelID)

	if limit != nil {
		u = fmt.Sprintf("%s?limit=%d", u, *limit)
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var stats CDPFunnelStatistic
	_, err = s.client.Do(ctx, req, &stats)
	if err != nil {
		return nil, err
	}

	return &stats, nil
}

// Entity Funnel API Operations (JSON:API format)

// CreateEntityFunnel creates a new funnel using JSON:API format
func (s *CDPService) CreateEntityFunnel(ctx context.Context, params CDPFunnelEntityCreateRequest) (*CDPJSONAPIResponse, error) {
	if len(params.Attributes.Stages) < 3 || len(params.Attributes.Stages) > 8 {
		return nil, fmt.Errorf("funnel must have between 3 and 8 stages, got %d", len(params.Attributes.Stages))
	}

	u := "entities/funnels"

	params.Type = "funnel"

	requestData := CDPJSONAPIResource{
		ID:   params.ID,
		Type: params.Type,
		Attributes: map[string]interface{}{
			"name":        params.Attributes.Name,
			"description": params.Attributes.Description,
			"stages":      params.Attributes.Stages,
		},
		Relationships: map[string]interface{}{
			"parentFolder": params.Relationships.ParentFolder,
		},
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

// GetEntityFunnel retrieves a funnel by ID using JSON:API format
func (s *CDPService) GetEntityFunnel(ctx context.Context, funnelID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/funnels/%s", funnelID)

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

// UpdateEntityFunnel updates an existing funnel using JSON:API format
func (s *CDPService) UpdateEntityFunnel(ctx context.Context, funnelID string, updates map[string]interface{}) (*CDPJSONAPIResponse, error) {
	if stages, ok := updates["stages"].([]interface{}); ok {
		if len(stages) < 3 || len(stages) > 8 {
			return nil, fmt.Errorf("funnel must have between 3 and 8 stages, got %d", len(stages))
		}
	}

	u := fmt.Sprintf("entities/funnels/%s", funnelID)

	requestData := CDPJSONAPIResource{
		ID:         funnelID,
		Type:       "funnel",
		Attributes: updates,
	}

	request := CDPJSONAPIRequest{Data: requestData}

	req, err := s.client.NewCDPJSONAPIRequest("PATCH", u, request)
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
