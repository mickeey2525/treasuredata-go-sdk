package treasuredata

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// CDPJourney represents a customer journey in CDP
type CDPJourney struct {
	ID            string                   `json:"id"`
	Type          string                   `json:"type"`
	Attributes    *CDPJourneyAttributes    `json:"attributes,omitempty"`
	Relationships *CDPJourneyRelationships `json:"relationships,omitempty"`
	Meta          *CDPJourneyMeta          `json:"meta,omitempty"`
}

// CDPJourneyAttributes contains the journey attributes
type CDPJourneyAttributes struct {
	Name                    string            `json:"name"`
	Description             string            `json:"description,omitempty"`
	AudienceID              string            `json:"audience_id"`
	SegmentFolderID         string            `json:"segment_folder_id"`
	Status                  string            `json:"status"`
	IsVisible               bool              `json:"is_visible"`
	HasCampaignGoal         bool              `json:"has_campaign_goal"`
	GoalType                string            `json:"goal_type,omitempty"`
	GoalValue               *float64          `json:"goal_value,omitempty"`
	AggregateWindowDays     int               `json:"aggregate_window_days"`
	MinimumEventsCount      int               `json:"minimum_events_count"`
	MinimumEventsWithinDays int               `json:"minimum_events_within_days"`
	CreatedAt               time.Time         `json:"created_at"`
	UpdatedAt               time.Time         `json:"updated_at"`
	JourneyStages           []CDPJourneyStage `json:"journey_stages,omitempty"`
}

// CDPJourneyRelationships contains journey relationships
type CDPJourneyRelationships struct {
	Audience      *CDPRelationshipData `json:"audience,omitempty"`
	SegmentFolder *CDPRelationshipData `json:"segment_folder,omitempty"`
	CreatedBy     *CDPRelationshipData `json:"created_by,omitempty"`
	UpdatedBy     *CDPRelationshipData `json:"updated_by,omitempty"`
}

// CDPJourneyMeta contains journey metadata
type CDPJourneyMeta struct {
	CanView   bool `json:"can_view"`
	CanEdit   bool `json:"can_edit"`
	CanDelete bool `json:"can_delete"`
}

// CDPJourneyStage represents a stage in a customer journey
type CDPJourneyStage struct {
	ID                  string      `json:"id"`
	Name                string      `json:"name"`
	Description         string      `json:"description,omitempty"`
	Rule                interface{} `json:"rule,omitempty"`
	Position            int         `json:"position"`
	IsGoal              bool        `json:"is_goal"`
	CompletionTimeHours *int        `json:"completion_time_hours,omitempty"`
}

// CDPJourneyStatistics represents journey statistics
type CDPJourneyStatistics struct {
	TotalCustomers        int64                  `json:"total_customers"`
	CompletedCustomers    int64                  `json:"completed_customers"`
	CompletionRate        float64                `json:"completion_rate"`
	AverageCompletionTime *float64               `json:"average_completion_time,omitempty"`
	StageStatistics       []CDPJourneyStageStats `json:"stage_statistics,omitempty"`
}

// CDPJourneyStageStats represents statistics for a journey stage
type CDPJourneyStageStats struct {
	StageID          string  `json:"stage_id"`
	StageName        string  `json:"stage_name"`
	CustomersEntered int64   `json:"customers_entered"`
	CustomersExited  int64   `json:"customers_exited"`
	ConversionRate   float64 `json:"conversion_rate"`
	DropoffRate      float64 `json:"dropoff_rate"`
}

// CDPJourneySankeyChart represents sankey chart data
type CDPJourneySankeyChart struct {
	Nodes []CDPSankeyNode `json:"nodes"`
	Links []CDPSankeyLink `json:"links"`
}

// CDPSankeyNode represents a node in the sankey chart
type CDPSankeyNode struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Value   int64  `json:"value"`
	StageID string `json:"stage_id,omitempty"`
}

// CDPSankeyLink represents a link in the sankey chart
type CDPSankeyLink struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Value  int64  `json:"value"`
	Label  string `json:"label,omitempty"`
}

// CDPJourneyActivation represents a journey activation
type CDPJourneyActivation struct {
	ID            string                    `json:"id"`
	Type          string                    `json:"type"`
	Attributes    *CDPJourneyActivationAttr `json:"attributes,omitempty"`
	Relationships interface{}               `json:"relationships,omitempty"`
}

// CDPJourneyActivationAttr contains journey activation attributes
type CDPJourneyActivationAttr struct {
	Name                 string      `json:"name"`
	Description          string      `json:"description,omitempty"`
	JourneyStageID       string      `json:"journey_stage_id"`
	ActivationTemplateID string      `json:"activation_template_id"`
	Status               string      `json:"status"`
	Configuration        interface{} `json:"configuration,omitempty"`
	CreatedAt            time.Time   `json:"created_at"`
	UpdatedAt            time.Time   `json:"updated_at"`
}

// CDPJourneyCustomer represents a customer in a journey
type CDPJourneyCustomer struct {
	CustomerID           string     `json:"customer_id"`
	CurrentStageID       string     `json:"current_stage_id,omitempty"`
	CompletedStages      []string   `json:"completed_stages,omitempty"`
	JourneyStartedAt     *time.Time `json:"journey_started_at,omitempty"`
	LastStageCompletedAt *time.Time `json:"last_stage_completed_at,omitempty"`
	IsCompleted          bool       `json:"is_completed"`
}

// CDPAvailableBehavior represents available behaviors for journey steps
type CDPAvailableBehavior struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Type        string      `json:"type"`
	Category    string      `json:"category,omitempty"`
	Parameters  interface{} `json:"parameters,omitempty"`
}

// CDPJourneySegmentRule represents segment rules available in journeys
type CDPJourneySegmentRule struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Type        string      `json:"type"`
	Rule        interface{} `json:"rule,omitempty"`
	AudienceID  string      `json:"audience_id"`
	JourneyID   string      `json:"journey_id"`
}

// Journey creation/update request
type CDPJourneyRequest struct {
	Data CDPJourney `json:"data"`
}

// Journey duplicate request
type CDPJourneyDuplicateRequest struct {
	Name             string `json:"name"`
	Description      string `json:"description,omitempty"`
	SourceJourneyID  string `json:"source_journey_id"`
	TargetAudienceID string `json:"target_audience_id"`
	SegmentFolderID  string `json:"segment_folder_id,omitempty"`
}

// Journey list response
type CDPJourneyListResponse struct {
	Data     []CDPJourney      `json:"data"`
	Included []interface{}     `json:"included,omitempty"`
	Meta     *CDPResponseMeta  `json:"meta,omitempty"`
	Links    *CDPResponseLinks `json:"links,omitempty"`
}

// Journey single response
type CDPJourneyResponse struct {
	Data     CDPJourney       `json:"data"`
	Included []interface{}    `json:"included,omitempty"`
	Meta     *CDPResponseMeta `json:"meta,omitempty"`
}

// Journey statistics response
type CDPJourneyStatisticsResponse struct {
	Data CDPJourneyStatistics `json:"data"`
}

// Journey sankey response
type CDPJourneySankeyResponse struct {
	Data CDPJourneySankeyChart `json:"data"`
}

// Journey customers response
type CDPJourneyCustomersResponse struct {
	Data  []CDPJourneyCustomer `json:"data"`
	Meta  *CDPResponseMeta     `json:"meta,omitempty"`
	Links *CDPResponseLinks    `json:"links,omitempty"`
}

// Journey activations response
type CDPJourneyActivationsResponse struct {
	Data     []CDPJourneyActivation `json:"data"`
	Included []interface{}          `json:"included,omitempty"`
	Meta     *CDPResponseMeta       `json:"meta,omitempty"`
}

// Available behaviors response
type CDPAvailableBehaviorsResponse struct {
	Data []CDPAvailableBehavior `json:"data"`
}

// Journey segment rules response
type CDPJourneySegmentRulesResponse struct {
	Data []CDPJourneySegmentRule `json:"data"`
}

// ListJourneys lists all journeys in the specified folder
func (c *CDPService) ListJourneys(ctx context.Context, folderID string) (*CDPJourneyListResponse, error) {
	path := fmt.Sprintf("entities/journeys?folder_id=%s", folderID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyListResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// CreateJourney creates a new journey
func (c *CDPService) CreateJourney(ctx context.Context, request *CDPJourneyRequest) (*CDPJourneyResponse, error) {
	path := "entities/journeys"

	req, err := c.client.NewCDPRequest("POST", path, request)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetJourney retrieves a specific journey by ID
func (c *CDPService) GetJourney(ctx context.Context, journeyID string) (*CDPJourneyResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s", journeyID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// UpdateJourney updates an existing journey
func (c *CDPService) UpdateJourney(ctx context.Context, journeyID string, request *CDPJourneyRequest) (*CDPJourneyResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s", journeyID)

	req, err := c.client.NewCDPRequest("PATCH", path, request)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// DeleteJourney deletes a journey
func (c *CDPService) DeleteJourney(ctx context.Context, journeyID string) error {
	path := fmt.Sprintf("entities/journeys/%s", journeyID)

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

// GetJourneyDetail retrieves detailed information about a journey
func (c *CDPService) GetJourneyDetail(ctx context.Context, journeyID string) (*CDPJourneyResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/detail", journeyID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetAvailableBehaviorsForStep gets available behaviors for a journey step
func (c *CDPService) GetAvailableBehaviorsForStep(ctx context.Context, journeyID string, stepID *string) (*CDPAvailableBehaviorsResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/available_behaviors_for_step", journeyID)
	if stepID != nil {
		path += fmt.Sprintf("?step_id=%s", *stepID)
	}

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPAvailableBehaviorsResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetActivationTemplatesForStep gets activation templates for a journey step
func (c *CDPService) GetActivationTemplatesForStep(ctx context.Context, journeyID string, stepID *string) (*CDPActivationTemplateListResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/activation_templates_for_step", journeyID)
	if stepID != nil {
		path += fmt.Sprintf("?step_id=%s", *stepID)
	}

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

// DuplicateJourney creates a duplicate of an existing journey
func (c *CDPService) DuplicateJourney(ctx context.Context, request *CDPJourneyDuplicateRequest) (*CDPJourneyResponse, error) {
	path := "entities/journeys/duplicate"

	req, err := c.client.NewCDPRequest("POST", path, request)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetJourneyStatistics retrieves statistics for a journey
func (c *CDPService) GetJourneyStatistics(ctx context.Context, journeyID string, from *time.Time, to *time.Time) (*CDPJourneyStatisticsResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/statistics", journeyID)

	params := make(map[string]string)
	if from != nil {
		params["from"] = from.Format("2006-01-02")
	}
	if to != nil {
		params["to"] = to.Format("2006-01-02")
	}

	if len(params) > 0 {
		path += "?"
		first := true
		for k, v := range params {
			if !first {
				path += "&"
			}
			path += fmt.Sprintf("%s=%s", k, v)
			first = false
		}
	}

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyStatisticsResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetJourneyConversionSankeyCharts retrieves conversion sankey chart data
func (c *CDPService) GetJourneyConversionSankeyCharts(ctx context.Context, journeyID string, from *time.Time, to *time.Time) (*CDPJourneySankeyResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/conversion_sankey_charts", journeyID)

	params := make(map[string]string)
	if from != nil {
		params["from"] = from.Format("2006-01-02")
	}
	if to != nil {
		params["to"] = to.Format("2006-01-02")
	}

	if len(params) > 0 {
		path += "?"
		first := true
		for k, v := range params {
			if !first {
				path += "&"
			}
			path += fmt.Sprintf("%s=%s", k, v)
			first = false
		}
	}

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneySankeyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetJourneyActivationSankeyCharts retrieves activation sankey chart data
func (c *CDPService) GetJourneyActivationSankeyCharts(ctx context.Context, journeyID string, from *time.Time, to *time.Time) (*CDPJourneySankeyResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/activation_sankey_charts", journeyID)

	params := make(map[string]string)
	if from != nil {
		params["from"] = from.Format("2006-01-02")
	}
	if to != nil {
		params["to"] = to.Format("2006-01-02")
	}

	if len(params) > 0 {
		path += "?"
		first := true
		for k, v := range params {
			if !first {
				path += "&"
			}
			path += fmt.Sprintf("%s=%s", k, v)
			first = false
		}
	}

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneySankeyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetJourneyCustomers retrieves customers for a journey
func (c *CDPService) GetJourneyCustomers(ctx context.Context, journeyID string, limit *int, offset *int) (*CDPJourneyCustomersResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/customers", journeyID)

	params := make(map[string]string)
	if limit != nil {
		params["limit"] = strconv.Itoa(*limit)
	}
	if offset != nil {
		params["offset"] = strconv.Itoa(*offset)
	}

	if len(params) > 0 {
		path += "?"
		first := true
		for k, v := range params {
			if !first {
				path += "&"
			}
			path += fmt.Sprintf("%s=%s", k, v)
			first = false
		}
	}

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyCustomersResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetJourneyStageCustomers retrieves customers for a specific journey stage
func (c *CDPService) GetJourneyStageCustomers(ctx context.Context, journeyID string, stageID string, limit *int, offset *int) (*CDPJourneyCustomersResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/journey_stages/%s/customers", journeyID, stageID)

	params := make(map[string]string)
	if limit != nil {
		params["limit"] = strconv.Itoa(*limit)
	}
	if offset != nil {
		params["offset"] = strconv.Itoa(*offset)
	}

	if len(params) > 0 {
		path += "?"
		first := true
		for k, v := range params {
			if !first {
				path += "&"
			}
			path += fmt.Sprintf("%s=%s", k, v)
			first = false
		}
	}

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyCustomersResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// ListJourneySegmentRules retrieves segment rules available in journeys
func (c *CDPService) ListJourneySegmentRules(ctx context.Context, audienceID string) (*CDPJourneySegmentRulesResponse, error) {
	path := fmt.Sprintf("entities/journeys/segment_rules?audience_id=%s", audienceID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneySegmentRulesResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// ListJourneyActivations lists activations for a journey
func (c *CDPService) ListJourneyActivations(ctx context.Context, journeyID string) (*CDPJourneyActivationsResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/activations", journeyID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyActivationsResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// CreateJourneyActivation creates a new journey activation
func (c *CDPService) CreateJourneyActivation(ctx context.Context, journeyID string, request *CDPJourneyActivationRequest) (*CDPJourneyActivationResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/activations", journeyID)

	req, err := c.client.NewCDPRequest("POST", path, request)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyActivationResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// GetJourneyActivation retrieves a specific journey activation
func (c *CDPService) GetJourneyActivation(ctx context.Context, journeyID string, activationStepID string) (*CDPJourneyActivationResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/activations/%s", journeyID, activationStepID)

	req, err := c.client.NewCDPRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyActivationResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// UpdateJourneyActivation updates a journey activation
func (c *CDPService) UpdateJourneyActivation(ctx context.Context, journeyID string, activationStepID string, request *CDPJourneyActivationRequest) (*CDPJourneyActivationResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/activations/%s", journeyID, activationStepID)

	req, err := c.client.NewCDPRequest("PATCH", path, request)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyActivationResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// PauseJourney pauses a journey
func (c *CDPService) PauseJourney(ctx context.Context, journeyID string) (*CDPJourneyResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/pause", journeyID)

	req, err := c.client.NewCDPRequest("PATCH", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// ResumeJourney resumes a paused journey
func (c *CDPService) ResumeJourney(ctx context.Context, journeyID string) (*CDPJourneyResponse, error) {
	path := fmt.Sprintf("entities/journeys/%s/resume", journeyID)

	req, err := c.client.NewCDPRequest("PATCH", path, nil)
	if err != nil {
		return nil, err
	}

	var response CDPJourneyResponse
	_, err = c.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// Journey activation request/response types
type CDPJourneyActivationRequest struct {
	Data CDPJourneyActivation `json:"data"`
}

type CDPJourneyActivationResponse struct {
	Data     CDPJourneyActivation `json:"data"`
	Included []interface{}        `json:"included,omitempty"`
	Meta     *CDPResponseMeta     `json:"meta,omitempty"`
}
