package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// LLMService handles communication with the LLM API.
type LLMService struct {
	client *Client
}

// LLMAction represents an AI action in the LLM API
type LLMAction struct {
	ID              string            `json:"id"`
	Type            string            `json:"type"`
	IntegrationID   string            `json:"integrationId"`
	PromptID        string            `json:"promptId"`
	ChatWidgetType  *string           `json:"chatWidgetType,omitempty"`
	ChatWidgetLabel *string           `json:"chatWidgetLabel,omitempty"`
	WebhookTextURL  string            `json:"webhookTextUrl"`
	SlackRequestURL *string           `json:"slackRequestUrl,omitempty"`
	UITags          []string          `json:"uiTags,omitempty"`
	Links           map[string]string `json:"links,omitempty"`
}

// LLMActionListResponse represents the response from listing actions
type LLMActionListResponse struct {
	Data  []LLMAction            `json:"data"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
	Links map[string]string      `json:"links,omitempty"`
}

// LLMActionResponse represents a single action response
type LLMActionResponse struct {
	Data  LLMAction              `json:"data"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
	Links map[string]string      `json:"links,omitempty"`
}

// LLMChatRequest represents a request to the chat endpoint
type LLMChatRequest struct {
	Message string                 `json:"message"`
	Context map[string]interface{} `json:"context,omitempty"`
}

// LLMChatResponse represents a response from the chat endpoint
type LLMChatResponse struct {
	Response string                 `json:"response"`
	Context  map[string]interface{} `json:"context,omitempty"`
}

// LLMIntegration represents an AI integration configuration
type LLMIntegration struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Type          string                 `json:"type"`
	Description   string                 `json:"description,omitempty"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
	Status        string                 `json:"status"`
	CreatedAt     TDTime                 `json:"created_at"`
	UpdatedAt     TDTime                 `json:"updated_at"`
}

// LLMIntegrationListResponse represents the response from listing integrations
type LLMIntegrationListResponse struct {
	Data  []LLMIntegration       `json:"data"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
	Links map[string]string      `json:"links,omitempty"`
}

// LLMPrompt represents an AI prompt configuration
type LLMPrompt struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Content     string   `json:"content"`
	Variables   []string `json:"variables,omitempty"`
	Status      string   `json:"status"`
	CreatedAt   TDTime   `json:"created_at"`
	UpdatedAt   TDTime   `json:"updated_at"`
}

// LLMPromptListResponse represents the response from listing prompts
type LLMPromptListResponse struct {
	Data  []LLMPrompt            `json:"data"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
	Links map[string]string      `json:"links,omitempty"`
}

// LLMProject represents an LLM project
type LLMProject struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Status      string                 `json:"status"`
	Settings    map[string]interface{} `json:"settings,omitempty"`
	CreatedAt   TDTime                 `json:"created_at"`
	UpdatedAt   TDTime                 `json:"updated_at"`
}

// LLMProjectListResponse represents the response from listing projects
type LLMProjectListResponse struct {
	Data  []LLMProject           `json:"data"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
	Links map[string]string      `json:"links,omitempty"`
}

// ListActions returns a list of available AI actions
func (s *LLMService) ListActions(ctx context.Context) (*LLMActionListResponse, error) {
	req, err := s.client.NewLLMRequest("GET", "api/actions", nil)
	if err != nil {
		return nil, err
	}

	var resp LLMActionListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetAction retrieves a specific action by ID
func (s *LLMService) GetAction(ctx context.Context, actionID string) (*LLMActionResponse, error) {
	u := fmt.Sprintf("api/actions/%s", actionID)

	req, err := s.client.NewLLMRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp LLMActionResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// ExecuteAction executes an AI action with the given input
func (s *LLMService) ExecuteAction(ctx context.Context, actionID string, input map[string]interface{}) (map[string]interface{}, error) {
	u := fmt.Sprintf("api/actions/%s/text", actionID)

	req, err := s.client.NewLLMRequest("POST", u, input)
	if err != nil {
		return nil, err
	}

	var resp map[string]interface{}
	httpResp, err := s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("LLM API returned status %d", httpResp.StatusCode)
	}

	return resp, nil
}

// ListIntegrations returns a list of AI integrations
func (s *LLMService) ListIntegrations(ctx context.Context) (*LLMIntegrationListResponse, error) {
	req, err := s.client.NewLLMRequest("GET", "api/integrations", nil)
	if err != nil {
		return nil, err
	}

	var resp LLMIntegrationListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// ListPrompts returns a list of AI prompts
func (s *LLMService) ListPrompts(ctx context.Context) (*LLMPromptListResponse, error) {
	req, err := s.client.NewLLMRequest("GET", "api/prompts", nil)
	if err != nil {
		return nil, err
	}

	var resp LLMPromptListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// ListProjects returns a list of LLM projects
func (s *LLMService) ListProjects(ctx context.Context) (*LLMProjectListResponse, error) {
	req, err := s.client.NewLLMRequest("GET", "api/projects", nil)
	if err != nil {
		return nil, err
	}

	var resp LLMProjectListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetProject retrieves a specific LLM project by ID
func (s *LLMService) GetProject(ctx context.Context, projectID string) (*LLMProject, error) {
	u := fmt.Sprintf("api/projects/%s", projectID)

	req, err := s.client.NewLLMRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data LLMProject `json:"data"`
	}
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp.Data, nil
}
