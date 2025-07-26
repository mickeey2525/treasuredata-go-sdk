package treasuredata

import (
	"context"
	"fmt"
)

// Legacy Predictive Segment API Operations

// ListPredictiveSegments retrieves a list of predictive segments for an audience (legacy API)
func (s *CDPService) ListPredictiveSegments(ctx context.Context, audienceID string) ([]CDPPredictiveSegment, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var segments []CDPPredictiveSegment
	_, err = s.client.Do(ctx, req, &segments)
	if err != nil {
		return nil, err
	}

	return segments, nil
}

// CreatePredictiveSegment creates a new predictive segment (legacy API)
func (s *CDPService) CreatePredictiveSegment(ctx context.Context, audienceID string, params CDPPredictiveSegmentCreateRequest) (*CDPPredictiveSegment, error) {
	if len(params.GradeThresholds) != 3 {
		return nil, fmt.Errorf("gradeThresholds must contain exactly 3 values, got %d", len(params.GradeThresholds))
	}

	u := fmt.Sprintf("audiences/%s/predictive_segments", audienceID)

	req, err := s.client.NewCDPRequest("POST", u, params)
	if err != nil {
		return nil, err
	}

	var segment CDPPredictiveSegment
	_, err = s.client.Do(ctx, req, &segment)
	if err != nil {
		return nil, err
	}

	return &segment, nil
}

// GetPredictiveSegment retrieves a specific predictive segment by ID (legacy API)
func (s *CDPService) GetPredictiveSegment(ctx context.Context, audienceID, predictiveSegmentID string) (*CDPPredictiveSegment, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments/%s", audienceID, predictiveSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var segment CDPPredictiveSegment
	_, err = s.client.Do(ctx, req, &segment)
	if err != nil {
		return nil, err
	}

	return &segment, nil
}

// UpdatePredictiveSegment updates an existing predictive segment (legacy API)
func (s *CDPService) UpdatePredictiveSegment(ctx context.Context, audienceID, predictiveSegmentID string, params CDPPredictiveSegmentCreateRequest) (*CDPPredictiveSegment, error) {
	if len(params.GradeThresholds) != 3 {
		return nil, fmt.Errorf("gradeThresholds must contain exactly 3 values, got %d", len(params.GradeThresholds))
	}

	u := fmt.Sprintf("audiences/%s/predictive_segments/%s", audienceID, predictiveSegmentID)

	req, err := s.client.NewCDPRequest("PATCH", u, params)
	if err != nil {
		return nil, err
	}

	var segment CDPPredictiveSegment
	_, err = s.client.Do(ctx, req, &segment)
	if err != nil {
		return nil, err
	}

	return &segment, nil
}

// DeletePredictiveSegment deletes a predictive segment (legacy API)
func (s *CDPService) DeletePredictiveSegment(ctx context.Context, audienceID, predictiveSegmentID string) (*CDPPredictiveSegment, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments/%s", audienceID, predictiveSegmentID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return nil, err
	}

	var segment CDPPredictiveSegment
	_, err = s.client.Do(ctx, req, &segment)
	if err != nil {
		return nil, err
	}

	return &segment, nil
}

// GetPredictiveSegmentExecutions retrieves executions for a predictive segment (legacy API)
func (s *CDPService) GetPredictiveSegmentExecutions(ctx context.Context, audienceID, predictiveSegmentID string) ([]CDPPredictiveSegmentExecution, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments/%s/executions", audienceID, predictiveSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var executions []CDPPredictiveSegmentExecution
	_, err = s.client.Do(ctx, req, &executions)
	if err != nil {
		return nil, err
	}

	return executions, nil
}

// TrainPredictiveSegment trains a predictive segment model (legacy API)
func (s *CDPService) TrainPredictiveSegment(ctx context.Context, audienceID, predictiveSegmentID string) (*CDPPredictiveSegmentExecution, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments/%s/run", audienceID, predictiveSegmentID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var execution CDPPredictiveSegmentExecution
	_, err = s.client.Do(ctx, req, &execution)
	if err != nil {
		return nil, err
	}

	return &execution, nil
}

// GetPredictiveSegmentGuessRule retrieves guessed rules for predictive segments (legacy API)
func (s *CDPService) GetPredictiveSegmentGuessRule(ctx context.Context, audienceID string) (*CDPPredictiveSegmentGuessRuleResponse, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments/guess_rule_async", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var response CDPPredictiveSegmentGuessRuleResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetPredictiveSegmentModelColumns retrieves model columns for a predictive segment (legacy API)
func (s *CDPService) GetPredictiveSegmentModelColumns(ctx context.Context, audienceID, predictiveSegmentID string) (interface{}, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments/%s/model/columns", audienceID, predictiveSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var columns interface{}
	_, err = s.client.Do(ctx, req, &columns)
	if err != nil {
		return nil, err
	}

	return columns, nil
}

// GetPredictiveSegmentModelFeatures retrieves model features for a predictive segment (legacy API)
func (s *CDPService) GetPredictiveSegmentModelFeatures(ctx context.Context, audienceID, predictiveSegmentID string) (interface{}, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments/%s/model/features", audienceID, predictiveSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var features interface{}
	_, err = s.client.Do(ctx, req, &features)
	if err != nil {
		return nil, err
	}

	return features, nil
}

// GetPredictiveSegmentScoreHistogram retrieves score histogram for a predictive segment (legacy API)
func (s *CDPService) GetPredictiveSegmentScoreHistogram(ctx context.Context, audienceID, predictiveSegmentID string) (interface{}, error) {
	u := fmt.Sprintf("audiences/%s/predictive_segments/%s/score_histogram", audienceID, predictiveSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var histogram interface{}
	_, err = s.client.Do(ctx, req, &histogram)
	if err != nil {
		return nil, err
	}

	return histogram, nil
}

// Entity Predictive Segment API Operations (JSON:API format)

// CreateEntityPredictiveSegment creates a new predictive segment using JSON:API format
func (s *CDPService) CreateEntityPredictiveSegment(ctx context.Context, params CDPPredictiveSegmentEntityCreateRequest) (*CDPJSONAPIResponse, error) {
	if len(params.Attributes.GradeThresholds) != 3 {
		return nil, fmt.Errorf("gradeThresholds must contain exactly 3 values, got %d", len(params.Attributes.GradeThresholds))
	}

	u := "entities/predictive_segments"

	params.Type = "predictive-segment"

	requestData := CDPJSONAPIResource{
		ID:   params.ID,
		Type: params.Type,
		Attributes: map[string]interface{}{
			"name":             params.Attributes.Name,
			"description":      params.Attributes.Description,
			"predictiveName":   params.Attributes.PredictiveName,
			"predictiveColumn": params.Attributes.PredictiveColumn,
			"modelType":        params.Attributes.ModelType,
			"trainingPeriod":   params.Attributes.TrainingPeriod,
			"predictionPeriod": params.Attributes.PredictionPeriod,
			"featureColumns":   params.Attributes.FeatureColumns,
			"gradeThresholds":  params.Attributes.GradeThresholds,
		},
		Relationships: map[string]interface{}{
			"baseSegment":  params.Relationships.BaseSegment,
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

// GetEntityPredictiveSegment retrieves a predictive segment by ID using JSON:API format
func (s *CDPService) GetEntityPredictiveSegment(ctx context.Context, predictiveSegmentID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/predictive_segments/%s", predictiveSegmentID)

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

// UpdateEntityPredictiveSegment updates an existing predictive segment using JSON:API format
func (s *CDPService) UpdateEntityPredictiveSegment(ctx context.Context, predictiveSegmentID string, updates map[string]interface{}) (*CDPJSONAPIResponse, error) {
	if thresholds, ok := updates["gradeThresholds"].([]interface{}); ok {
		if len(thresholds) != 3 {
			return nil, fmt.Errorf("gradeThresholds must contain exactly 3 values, got %d", len(thresholds))
		}
	}

	u := fmt.Sprintf("entities/predictive_segments/%s", predictiveSegmentID)

	requestData := CDPJSONAPIResource{
		ID:         predictiveSegmentID,
		Type:       "predictive-segment",
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

// DeleteEntityPredictiveSegment deletes a predictive segment using JSON:API format
func (s *CDPService) DeleteEntityPredictiveSegment(ctx context.Context, predictiveSegmentID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/predictive_segments/%s", predictiveSegmentID)

	req, err := s.client.NewCDPJSONAPIRequest("DELETE", u, nil)
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

// RunEntityPredictiveSegment runs a predictive segment using JSON:API format
func (s *CDPService) RunEntityPredictiveSegment(ctx context.Context, predictiveSegmentID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/predictive_segments/%s/run", predictiveSegmentID)

	req, err := s.client.NewCDPJSONAPIRequest("POST", u, nil)
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

// GetEntityPredictiveSegmentExecutions retrieves executions using JSON:API format
func (s *CDPService) GetEntityPredictiveSegmentExecutions(ctx context.Context, predictiveSegmentID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/predictive_segments/%s/executions", predictiveSegmentID)

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

// GetEntityPredictiveSegmentModelFeatures retrieves model features using JSON:API format
func (s *CDPService) GetEntityPredictiveSegmentModelFeatures(ctx context.Context, predictiveSegmentID string, limit *int64) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/predictive_segments/%s/model/features", predictiveSegmentID)

	if limit != nil {
		u = fmt.Sprintf("%s?limit=%d", u, *limit)
	}

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

// GetEntityPredictiveSegmentModelColumns retrieves model columns using JSON:API format
func (s *CDPService) GetEntityPredictiveSegmentModelColumns(ctx context.Context, predictiveSegmentID string, limit *int64) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/predictive_segments/%s/model/columns", predictiveSegmentID)

	if limit != nil {
		u = fmt.Sprintf("%s?limit=%d", u, *limit)
	}

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

// GetEntityPredictiveSegmentModelScores retrieves model scores using JSON:API format
func (s *CDPService) GetEntityPredictiveSegmentModelScores(ctx context.Context, predictiveSegmentID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/predictive_segments/%s/model/scores", predictiveSegmentID)

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
