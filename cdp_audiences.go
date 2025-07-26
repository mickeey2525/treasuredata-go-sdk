package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// CreateAudience creates a new audience
func (s *CDPService) CreateAudience(ctx context.Context, name, description, parentDatabaseName, parentTableName string) (*CDPAudience, error) {
	u := "audiences"

	body := map[string]interface{}{
		"name":        name,
		"description": description,
		"master": map[string]string{
			"parentDatabaseName": parentDatabaseName,
			"parentTableName":    parentTableName,
		},
	}

	req, err := s.client.NewCDPRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var audience CDPAudience
	_, err = s.client.Do(ctx, req, &audience)
	if err != nil {
		return nil, err
	}

	return &audience, nil
}

// ListAudiences returns a list of audiences
func (s *CDPService) ListAudiences(ctx context.Context) (*CDPAudienceListResponse, error) {
	u := "audiences"

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var audiences []CDPAudience
	_, err = s.client.Do(ctx, req, &audiences)
	if err != nil {
		return nil, err
	}

	return &CDPAudienceListResponse{
		Audiences: audiences,
		Total:     int64(len(audiences)),
	}, nil
}

// GetAudience retrieves a specific audience by ID
func (s *CDPService) GetAudience(ctx context.Context, audienceID string) (*CDPAudience, error) {
	u := fmt.Sprintf("audiences/%s", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var audience CDPAudience
	_, err = s.client.Do(ctx, req, &audience)
	if err != nil {
		return nil, err
	}

	return &audience, nil
}

// DeleteAudience deletes an audience
func (s *CDPService) DeleteAudience(ctx context.Context, audienceID string) error {
	u := fmt.Sprintf("audiences/%s", audienceID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete audience: %s", audienceID)
	}

	return nil
}

// UpdateAudience updates an existing audience
func (s *CDPService) UpdateAudience(ctx context.Context, audienceID string, req *CDPAudienceUpdateRequest) (*CDPAudience, error) {
	u := fmt.Sprintf("audiences/%s", audienceID)

	request, err := s.client.NewCDPRequest("PUT", u, req)
	if err != nil {
		return nil, err
	}

	var audience CDPAudience
	_, err = s.client.Do(ctx, request, &audience)
	if err != nil {
		return nil, err
	}

	return &audience, nil
}

// GetAudienceAttributes retrieves attributes for a specific audience
func (s *CDPService) GetAudienceAttributes(ctx context.Context, audienceID string) ([]interface{}, error) {
	u := fmt.Sprintf("audiences/%s/attributes", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var attributes []interface{}
	_, err = s.client.Do(ctx, req, &attributes)
	if err != nil {
		return nil, err
	}

	return attributes, nil
}

// GetAudienceBehaviors retrieves behaviors for a specific audience
func (s *CDPService) GetAudienceBehaviors(ctx context.Context, audienceID string) ([]CDPAudienceBehavior, error) {
	u := fmt.Sprintf("audiences/%s/behaviors", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var behaviors []CDPAudienceBehavior
	_, err = s.client.Do(ctx, req, &behaviors)
	if err != nil {
		return nil, err
	}

	return behaviors, nil
}

// RunAudience starts an audience execution/refresh
func (s *CDPService) RunAudience(ctx context.Context, audienceID string) (*CDPAudienceExecution, error) {
	u := fmt.Sprintf("audiences/%s/run", audienceID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var execution CDPAudienceExecution
	_, err = s.client.Do(ctx, req, &execution)
	if err != nil {
		return nil, err
	}

	return &execution, nil
}

// GetAudienceExecutions retrieves execution history for a specific audience
func (s *CDPService) GetAudienceExecutions(ctx context.Context, audienceID string) ([]CDPAudienceExecution, error) {
	u := fmt.Sprintf("audiences/%s/executions", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var executions []CDPAudienceExecution
	_, err = s.client.Do(ctx, req, &executions)
	if err != nil {
		return nil, err
	}

	return executions, nil
}

// GetAudienceStatistics retrieves statistics/population data for a specific audience
func (s *CDPService) GetAudienceStatistics(ctx context.Context, audienceID string) ([]CDPAudienceStatisticsPoint, error) {
	u := fmt.Sprintf("audiences/%s/statistics", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var statistics []CDPAudienceStatisticsPoint
	_, err = s.client.Do(ctx, req, &statistics)
	if err != nil {
		return nil, err
	}

	return statistics, nil
}

// GetAudienceSampleValues retrieves sample values for a specific audience attribute column
func (s *CDPService) GetAudienceSampleValues(ctx context.Context, audienceID, column string) ([]CDPAudienceSampleValue, error) {
	u := fmt.Sprintf("audiences/%s/sample_values?column=%s", audienceID, column)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var sampleValues []CDPAudienceSampleValue
	_, err = s.client.Do(ctx, req, &sampleValues)
	if err != nil {
		return nil, err
	}

	return sampleValues, nil
}

// GetAudienceBehaviorSampleValues retrieves sample values for a specific audience behavior column
func (s *CDPService) GetAudienceBehaviorSampleValues(ctx context.Context, audienceID, behaviorID, column string) ([]CDPAudienceSampleValue, error) {
	u := fmt.Sprintf("audiences/%s/behaviors/%s/sample_values?column=%s", audienceID, behaviorID, column)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var sampleValues []CDPAudienceSampleValue
	_, err = s.client.Do(ctx, req, &sampleValues)
	if err != nil {
		return nil, err
	}

	return sampleValues, nil
}

// Audience Folder Operations

// CreateAudienceFolder creates a new folder for a specific audience
func (s *CDPService) CreateAudienceFolder(ctx context.Context, audienceID string, req *CDPAudienceFolderCreateRequest) (*CDPAudienceFolder, error) {
	u := fmt.Sprintf("audiences/%s/folders", audienceID)

	request, err := s.client.NewCDPRequest("POST", u, req)
	if err != nil {
		return nil, err
	}

	var folder CDPAudienceFolder
	_, err = s.client.Do(ctx, request, &folder)
	if err != nil {
		return nil, err
	}

	return &folder, nil
}

// GetAudienceFolder retrieves a specific folder for an audience
func (s *CDPService) GetAudienceFolder(ctx context.Context, audienceID, folderID string) (*CDPAudienceFolder, error) {
	u := fmt.Sprintf("audiences/%s/folders/%s", audienceID, folderID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var folder CDPAudienceFolder
	_, err = s.client.Do(ctx, req, &folder)
	if err != nil {
		return nil, err
	}

	return &folder, nil
}

// UpdateAudienceFolder updates an existing audience folder
func (s *CDPService) UpdateAudienceFolder(ctx context.Context, audienceID, folderID string, req *CDPAudienceFolderUpdateRequest) (*CDPAudienceFolder, error) {
	u := fmt.Sprintf("audiences/%s/folders/%s", audienceID, folderID)

	request, err := s.client.NewCDPRequest("PATCH", u, req)
	if err != nil {
		return nil, err
	}

	var folder CDPAudienceFolder
	_, err = s.client.Do(ctx, request, &folder)
	if err != nil {
		return nil, err
	}

	return &folder, nil
}

// DeleteAudienceFolder deletes an audience folder
func (s *CDPService) DeleteAudienceFolder(ctx context.Context, audienceID, folderID string) error {
	u := fmt.Sprintf("audiences/%s/folders/%s", audienceID, folderID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete audience folder: %s", folderID)
	}

	return nil
}

// ListFolders returns a list of folders for a specific audience
func (s *CDPService) ListFolders(ctx context.Context, audienceID string) (*CDPAudienceFolderListResponse, error) {
	u := fmt.Sprintf("audiences/%s/folders/", audienceID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var folders []CDPAudienceFolder
	_, err = s.client.Do(ctx, req, &folders)
	if err != nil {
		return nil, err
	}

	return &CDPAudienceFolderListResponse{
		Folders: folders,
		Total:   int64(len(folders)),
	}, nil
}
