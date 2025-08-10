package treasuredata

import (
	"context"
	"fmt"
)

// CreateActivationTemplate creates a new activation template
func (c *CDPService) CreateActivationTemplate(ctx context.Context, request *CDPActivationTemplateRequest) (*CDPActivationTemplateResponse, error) {
	path := "entities/activation_templates"

	req, err := c.client.NewCDPRequest("POST", path, request)
	if err != nil {
		return nil, err
	}

	var response CDPActivationTemplateResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetActivationTemplate retrieves a specific activation template by ID
func (c *CDPService) GetActivationTemplate(ctx context.Context, templateID string) (*CDPActivationTemplateResponse, error) {
	path := fmt.Sprintf("entities/activation_templates/%s", templateID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPActivationTemplateResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// UpdateActivationTemplate updates an existing activation template
func (c *CDPService) UpdateActivationTemplate(ctx context.Context, templateID string, request *CDPActivationTemplateRequest) (*CDPActivationTemplateResponse, error) {
	path := fmt.Sprintf("entities/activation_templates/%s", templateID)

	req, err := c.client.NewCDPRequest("PATCH", path, request)
	if err != nil {
		return nil, err
	}

	var response CDPActivationTemplateResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// DeleteActivationTemplate deletes an activation template
func (c *CDPService) DeleteActivationTemplate(ctx context.Context, templateID string) error {
	path := fmt.Sprintf("entities/activation_templates/%s", templateID)

	req, err := c.client.NewCDPRequest("DELETE", path, nil)
	if err != nil {
		return err
	}

	_, err = c.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	return nil
}

// ListActivationTemplatesByParentSegment retrieves activation templates for a parent segment
func (c *CDPService) ListActivationTemplatesByParentSegment(ctx context.Context, parentSegmentID string) (*CDPActivationTemplateListResponse, error) {
	path := fmt.Sprintf("entities/parent_segments/%s/activation_templates", parentSegmentID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPActivationTemplateListResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}
