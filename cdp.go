package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// CDPService handles communication with the CDP (Customer Data Platform) related methods of the Treasure Data API.
type CDPService struct {
	client *Client
}

// CDPSegment represents a customer segment in CDP
type CDPSegment struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	Query        string `json:"query"`
	CreatedAt    TDTime `json:"created_at"`
	UpdatedAt    TDTime `json:"updated_at"`
	ProfileCount int64  `json:"profile_count"`
	FolderID     string `json:"folder_id,omitempty"`
	Status       string `json:"status,omitempty"`
	QueryID      string `json:"query_id,omitempty"`
}

// CDPSegmentFolder represents a segment folder in CDP
type CDPSegmentFolder struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	ParentID  *string  `json:"parent_id"`
	CreatedAt TDTime   `json:"created_at"`
	UpdatedAt TDTime   `json:"updated_at"`
	Segments  []string `json:"segments,omitempty"`
}

// CDPSegmentQuery represents a segment query execution
type CDPSegmentQuery struct {
	ID         string                 `json:"id"`
	SegmentID  string                 `json:"segment_id"`
	Query      string                 `json:"query"`
	Status     string                 `json:"status"`
	CreatedAt  TDTime                 `json:"created_at"`
	UpdatedAt  TDTime                 `json:"updated_at"`
	StartedAt  *TDTime                `json:"started_at"`
	FinishedAt *TDTime                `json:"finished_at"`
	Results    map[string]interface{} `json:"results,omitempty"`
	Error      string                 `json:"error,omitempty"`
}

// CDPSegmentCustomer represents a customer in a segment
type CDPSegmentCustomer struct {
	ID         string                 `json:"id"`
	Attributes map[string]interface{} `json:"attributes"`
}

// CDPSegmentStatistics represents segment statistics data
type CDPSegmentStatistics struct {
	SegmentID    string                      `json:"segment_id"`
	ProfileCount int64                       `json:"profile_count"`
	LastUpdated  TDTime                      `json:"last_updated"`
	Statistics   []CDPSegmentStatisticsPoint `json:"statistics"`
}

// CDPSegmentStatisticsPoint represents a single statistics data point
type CDPSegmentStatisticsPoint []interface{} // [timestamp, count, hasData]

// CDPFolder represents a folder in CDP for organizing entities
type CDPFolder struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	ParentID    *string  `json:"parent_id,omitempty"`
	Path        string   `json:"path,omitempty"`
	CreatedAt   TDTime   `json:"created_at"`
	UpdatedAt   TDTime   `json:"updated_at"`
	CreatedBy   *CDPUser `json:"created_by,omitempty"`
	UpdatedBy   *CDPUser `json:"updated_by,omitempty"`
}

// CDPAudienceFolder represents a folder specific to an audience
type CDPAudienceFolder struct {
	ID             string   `json:"id"`
	AudienceID     string   `json:"audienceId"`
	Name           string   `json:"name"`
	Description    *string  `json:"description"`
	ParentFolderID *string  `json:"parentFolderId"`
	Path           string   `json:"path,omitempty"`
	CreatedAt      TDTime   `json:"createdAt"`
	UpdatedAt      TDTime   `json:"updatedAt"`
	CreatedBy      *CDPUser `json:"createdBy,omitempty"`
	UpdatedBy      *CDPUser `json:"updatedBy,omitempty"`
}

// CDPEntity represents a generic entity in CDP that can be in folders
type CDPEntity struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"` // "audience", "segment", etc.
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	FolderID    *string                `json:"folder_id,omitempty"`
	CreatedAt   TDTime                 `json:"created_at"`
	UpdatedAt   TDTime                 `json:"updated_at"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// CDPAudience represents an audience in CDP
type CDPAudience struct {
	ID                           string                 `json:"id"`
	Name                         string                 `json:"name"`
	Description                  string                 `json:"description"`
	ScheduleType                 string                 `json:"scheduleType"`
	ScheduleOption               *string                `json:"scheduleOption"`
	Timezone                     string                 `json:"timezone"`
	CreatedAt                    TDTime                 `json:"createdAt"`
	UpdatedAt                    TDTime                 `json:"updatedAt"`
	CreatedBy                    *CDPUser               `json:"createdBy"`
	UpdatedBy                    *CDPUser               `json:"updatedBy"`
	MatrixUpdatedAt              *TDTime                `json:"matrixUpdatedAt"`
	WorkflowHiveOnly             bool                   `json:"workflowHiveOnly"`
	HiveEngineVersion            string                 `json:"hiveEngineVersion"`
	HivePoolName                 *string                `json:"hivePoolName"`
	PrestoPoolName               *string                `json:"prestoPoolName"`
	Population                   int64                  `json:"population"`
	MaxActivationBehaviorRow     int                    `json:"maxActivationBehaviorRow"`
	AllowActivationBehavior      bool                   `json:"allowActivationBehavior"`
	LLMState                     string                 `json:"llmState"`
	EnrichmentWordTaggingEnabled bool                   `json:"enrichmentWordTaggingEnabled"`
	EnrichmentIPEnabled          bool                   `json:"enrichmentIpEnabled"`
	EnrichmentTdJsSdkEnabled     bool                   `json:"enrichmentTdJsSdkEnabled"`
	RootFolderID                 string                 `json:"rootFolderId"`
	LLMEnabled                   bool                   `json:"llmEnabled"`
	Master                       *CDPAudienceMaster     `json:"master"`
	Attributes                   []CDPAudienceAttribute `json:"attributes"`
	Behaviors                    []CDPAudienceBehavior  `json:"behaviors"`
	AudienceFilters              []CDPAudienceFilter    `json:"audienceFilters"`
}

// CDPUser represents a user in CDP
type CDPUser struct {
	ID       string `json:"id"`
	TdUserID string `json:"td_user_id"`
	Name     string `json:"name"`
}

// CDPAudienceMaster represents the master configuration of an audience
type CDPAudienceMaster struct {
	ParentDatabaseName string `json:"parentDatabaseName"`
	ParentTableName    string `json:"parentTableName"`
}

// CDPAudienceAttribute represents an attribute in an audience
type CDPAudienceAttribute struct {
	AudienceID         string  `json:"audienceId"`
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	Type               string  `json:"type"`
	ParentDatabaseName string  `json:"parentDatabaseName"`
	ParentTableName    string  `json:"parentTableName"`
	ParentColumn       string  `json:"parentColumn"`
	ParentKey          string  `json:"parentKey"`
	ForeignKey         string  `json:"foreignKey"`
	MatrixColumnName   string  `json:"matrixColumnName"`
	GroupingName       *string `json:"groupingName"`
}

// CDPAudienceBehavior represents a behavior in an audience
type CDPAudienceBehavior struct {
	AudienceID               string                   `json:"audienceId"`
	ID                       string                   `json:"id"`
	Name                     string                   `json:"name"`
	ParentDatabaseName       string                   `json:"parentDatabaseName"`
	ParentTableName          string                   `json:"parentTableName"`
	ParentKey                string                   `json:"parentKey"`
	ForeignKey               string                   `json:"foreignKey"`
	MatrixDatabaseName       string                   `json:"matrixDatabaseName"`
	MatrixTableName          string                   `json:"matrixTableName"`
	AllColumns               bool                     `json:"allColumns"`
	DefaultTimeFilterEnabled bool                     `json:"defaultTimeFilterEnabled"`
	IsRealtime               bool                     `json:"isRealtime"`
	Schema                   []CDPBehaviorSchemaField `json:"schema"`
}

// CDPAudienceFilter represents a filter in an audience
type CDPAudienceFilter struct {
	// Add fields as needed when API provides filter data
}

// CDPAudienceExecution represents an audience execution/run
type CDPAudienceExecution struct {
	AudienceID        string  `json:"audienceId"`
	WorkflowID        string  `json:"workflowId"`
	WorkflowSessionID string  `json:"workflowSessionId"`
	WorkflowAttemptID string  `json:"workflowAttemptId"`
	CreatedAt         TDTime  `json:"createdAt"`
	FinishedAt        *TDTime `json:"finishedAt"`
	Status            string  `json:"status"`
}

// CDPAudienceStatisticsPoint represents a single data point in audience statistics
type CDPAudienceStatisticsPoint []interface{} // [timestamp, population, hasData]

// CDPAudienceSampleValue represents a sample value with its frequency
type CDPAudienceSampleValue []interface{} // [value, frequency]

// CDPBehaviorSchemaField represents a field in a behavior schema with visibility
type CDPBehaviorSchemaField struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	ParentColumn     string `json:"parentColumn"`
	MatrixColumnName string `json:"matrixColumnName"`
	Visibility       string `json:"visibility"`
}

// CDPActivation represents an activation configuration
type CDPActivation struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Type          string                 `json:"type"`
	AudienceID    string                 `json:"audience_id"`
	Configuration map[string]interface{} `json:"configuration"`
	Status        string                 `json:"status"`
	CreatedAt     TDTime                 `json:"created_at"`
	UpdatedAt     TDTime                 `json:"updated_at"`
}

// CDPActivationExecution represents an execution of an activation
type CDPActivationExecution struct {
	ID              string  `json:"id"`
	ActivationID    string  `json:"activation_id"`
	Status          string  `json:"status"`
	StartedAt       TDTime  `json:"started_at"`
	FinishedAt      *TDTime `json:"finished_at,omitempty"`
	RecordsExported int64   `json:"records_exported,omitempty"`
	ErrorMessage    string  `json:"error_message,omitempty"`
}

// CDPSegmentListOptions specifies optional parameters to Segment List method
type CDPSegmentListOptions struct {
	Limit    int    `url:"limit,omitempty"`
	Offset   int    `url:"offset,omitempty"`
	FolderID string `url:"folder_id,omitempty"`
	Query    string `url:"query,omitempty"`
}

// CDPSegmentCustomerListOptions specifies optional parameters for segment customer list
type CDPSegmentCustomerListOptions struct {
	Limit  int    `url:"limit,omitempty"`
	Offset int    `url:"offset,omitempty"`
	Fields string `url:"fields,omitempty"`
}

// CDPSegmentListResponse represents the response from the segment list API
type CDPSegmentListResponse struct {
	Segments []CDPSegment `json:"segments"`
	Total    int64        `json:"total"`
}

// CDPAudienceListResponse represents the response from the audience list API
type CDPAudienceListResponse struct {
	Audiences []CDPAudience `json:"audiences"`
	Total     int64         `json:"total"`
}

// CDPActivationListResponse represents the response from the activation list API
type CDPActivationListResponse struct {
	Activations []CDPActivation `json:"activations"`
	Total       int64           `json:"total"`
}

// CDPEntitiesActivationListResponse represents the response from the entities activation list API
type CDPEntitiesActivationListResponse struct {
	Data     []CDPActivation `json:"data"`
	Included []interface{}   `json:"included"`
}

// CDPSegmentFolderListResponse represents the response from the segment folder list API
type CDPSegmentFolderListResponse struct {
	Folders []CDPSegmentFolder `json:"folders"`
	Total   int64              `json:"total"`
}

// CDPSegmentCustomerListResponse represents the response from the segment customer list API
type CDPSegmentCustomerListResponse struct {
	Customers []CDPSegmentCustomer `json:"customers"`
	Total     int64                `json:"total"`
}

// CDPFolderListResponse represents the response from the folder list API
type CDPFolderListResponse struct {
	Folders []CDPFolder `json:"folders"`
	Total   int64       `json:"total"`
}

// CDPAudienceFolderListResponse represents the response from the audience folder list API
type CDPAudienceFolderListResponse struct {
	Folders []CDPAudienceFolder `json:"folders"`
	Total   int64               `json:"total"`
}

// CDPEntityListResponse represents the response from the entity list API
type CDPEntityListResponse struct {
	Entities []CDPEntity `json:"entities"`
	Total    int64       `json:"total"`
}

// CDPUserDefinedWorkflowListResponse represents the response from the user-defined workflow list API
type CDPUserDefinedWorkflowListResponse struct {
	Workflows []CDPUserDefinedWorkflow `json:"workflows"`
	Total     int64                    `json:"total"`
}

// CDPUserDefinedWorkflowProjectListResponse represents the response from the user-defined workflow project list API
type CDPUserDefinedWorkflowProjectListResponse struct {
	Projects []CDPUserDefinedWorkflowProject `json:"projects"`
	Total    int64                           `json:"total"`
}

// CDPMatchedActivationListResponse represents the response from the matched activation list API
type CDPMatchedActivationListResponse struct {
	Activations []CDPMatchedActivation `json:"activations"`
	Total       int64                  `json:"total"`
}

// CDPTokenListResponse represents the response from the token list API
type CDPTokenListResponse struct {
	Tokens []CDPToken `json:"tokens"`
	Total  int64      `json:"total"`
}

// CDPFolderCreateRequest represents a request to create a folder
type CDPFolderCreateRequest struct {
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	ParentID    *string `json:"parent_id,omitempty"`
}

// CDPEntityFolderCreateRequest represents a JSON API request to create an entity folder
type CDPEntityFolderCreateRequest struct {
	ID            string                              `json:"id,omitempty"`
	Type          string                              `json:"type"`
	Attributes    CDPEntityFolderCreateAttributes     `json:"attributes"`
	Relationships *CDPEntityFolderCreateRelationships `json:"relationships,omitempty"`
}

// CDPEntityFolderCreateAttributes represents the attributes for entity folder creation
type CDPEntityFolderCreateAttributes struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// CDPEntityFolderCreateRelationships represents the relationships for entity folder creation
type CDPEntityFolderCreateRelationships struct {
	ParentFolder *CDPEntityFolderParentData `json:"parentFolder,omitempty"`
}

// CDPEntityFolderParentData represents the parent folder relationship data
type CDPEntityFolderParentData struct {
	Data *CDPEntityFolderParentInfo `json:"data,omitempty"`
}

// CDPEntityFolderParentInfo represents the parent folder info
type CDPEntityFolderParentInfo struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// CDPFolderUpdateRequest represents a request to update a folder
type CDPFolderUpdateRequest struct {
	Name        string  `json:"name,omitempty"`
	Description string  `json:"description,omitempty"`
	ParentID    *string `json:"parent_id,omitempty"`
}

// CDPAudienceFolderCreateRequest represents a request to create an audience folder
type CDPAudienceFolderCreateRequest struct {
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	ParentID    *string `json:"parent_id,omitempty"`
}

// CDPActivation represents an activation configuration

// CDPActivationCreateRequest represents a request to create an activation
type CDPActivationCreateRequest struct {
	Name            string                 `json:"name"`
	Description     string                 `json:"description,omitempty"`
	Type            string                 `json:"type"`
	Configuration   map[string]interface{} `json:"configuration"`
	SegmentFolderID *string                `json:"segment_folder_id,omitempty"`
	AudienceID      *string                `json:"audience_id,omitempty"`
}

// CDPActivationUpdateRequest represents a request to update an activation
type CDPActivationUpdateRequest struct {
	Name          string                 `json:"name,omitempty"`
	Description   string                 `json:"description,omitempty"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
	Status        string                 `json:"status,omitempty"`
}

// CDPActivationListOptions specifies optional parameters for activation list
type CDPActivationListOptions struct {
	Limit           int    `url:"limit,omitempty"`
	Offset          int    `url:"offset,omitempty"`
	Type            string `url:"type,omitempty"`
	Status          string `url:"status,omitempty"`
	SegmentFolderID string `url:"segment_folder_id,omitempty"`
	AudienceID      string `url:"audience_id,omitempty"`
}

// CDPUserDefinedWorkflow represents a user-defined workflow
type CDPUserDefinedWorkflow struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Description   string                 `json:"description,omitempty"`
	ProjectID     string                 `json:"project_id"`
	Status        string                 `json:"status"`
	Configuration map[string]interface{} `json:"configuration"`
	CreatedAt     TDTime                 `json:"created_at"`
	UpdatedAt     TDTime                 `json:"updated_at"`
	CreatedBy     *CDPUser               `json:"created_by,omitempty"`
	UpdatedBy     *CDPUser               `json:"updated_by,omitempty"`
}

// CDPUserDefinedWorkflowProject represents a user-defined workflow project
type CDPUserDefinedWorkflowProject struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Status      string                 `json:"status"`
	Settings    map[string]interface{} `json:"settings,omitempty"`
	CreatedAt   TDTime                 `json:"created_at"`
	UpdatedAt   TDTime                 `json:"updated_at"`
	CreatedBy   *CDPUser               `json:"created_by,omitempty"`
	UpdatedBy   *CDPUser               `json:"updated_by,omitempty"`
}

// CDPMatchedActivation represents a matched activation for a segment
type CDPMatchedActivation struct {
	ID              string                 `json:"id"`
	Name            string                 `json:"name"`
	Type            string                 `json:"type"`
	Status          string                 `json:"status"`
	Configuration   map[string]interface{} `json:"configuration"`
	SegmentID       string                 `json:"segment_id"`
	ParentSegmentID string                 `json:"parent_segment_id"`
	MatchCriteria   map[string]interface{} `json:"match_criteria,omitempty"`
	CreatedAt       TDTime                 `json:"created_at"`
	UpdatedAt       TDTime                 `json:"updated_at"`
}

// CDPActivationCreateRequest represents a request to create an activation

// CDPToken represents a token in CDP
type CDPToken struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Type        string                 `json:"type"`
	Value       string                 `json:"value,omitempty"`
	Status      string                 `json:"status"`
	ExpiresAt   *TDTime                `json:"expires_at,omitempty"`
	Scopes      []string               `json:"scopes,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt   TDTime                 `json:"created_at"`
	UpdatedAt   TDTime                 `json:"updated_at"`
	CreatedBy   *CDPUser               `json:"created_by,omitempty"`
	UpdatedBy   *CDPUser               `json:"updated_by,omitempty"`
}

// CDPTokenUpdateRequest represents a request to update a token
type CDPTokenUpdateRequest struct {
	Name        string                 `json:"name,omitempty"`
	Description string                 `json:"description,omitempty"`
	Status      string                 `json:"status,omitempty"`
	ExpiresAt   *TDTime                `json:"expires_at,omitempty"`
	Scopes      []string               `json:"scopes,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// CDPTokenListOptions specifies optional parameters for token list
type CDPTokenListOptions struct {
	Limit  int    `url:"limit,omitempty"`
	Offset int    `url:"offset,omitempty"`
	Type   string `url:"type,omitempty"`
	Status string `url:"status,omitempty"`
}

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
func (s *CDPService) CreateSegmentQuery(ctx context.Context, audienceID, segmentID, query string) (*CDPSegmentQuery, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/new_query", audienceID, segmentID)

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

// ExecuteSegmentQuery executes a query for a segment
func (s *CDPService) ExecuteSegmentQuery(ctx context.Context, audienceID, segmentID string) (*CDPSegmentQuery, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/query", audienceID, segmentID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
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
func (s *CDPService) GetSegmentQueryStatus(ctx context.Context, audienceID, segmentID, queryID string) (*CDPSegmentQuery, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/query_status/%s", audienceID, segmentID, queryID)

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
func (s *CDPService) KillSegmentQuery(ctx context.Context, audienceID, segmentID, queryID string) error {
	u := fmt.Sprintf("audiences/%s/segments/%s/kill_query/%s", audienceID, segmentID, queryID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to kill segment query: %s/%s", segmentID, queryID)
	}

	return nil
}

// GetSegmentCustomers retrieves customers in a segment
func (s *CDPService) GetSegmentCustomers(ctx context.Context, audienceID, segmentID string, opts *CDPSegmentCustomerListOptions) (*CDPSegmentCustomerListResponse, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/query_customers", audienceID, segmentID)
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
func (s *CDPService) GetSegmentStatistics(ctx context.Context, audienceID, segmentID string) (*CDPSegmentStatistics, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/statistics", audienceID, segmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var statistics CDPSegmentStatistics
	_, err = s.client.Do(ctx, req, &statistics)
	if err != nil {
		return nil, err
	}

	return &statistics, nil
}

// CreateAudience creates a new audience
func (s *CDPService) CreateAudience(ctx context.Context, name, description, parentDatabaseName, parentTableName string) (*CDPAudience, error) {
	u := fmt.Sprintf("audiences")

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
	u := fmt.Sprintf("audiences")

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

// CreateActivation creates a new activation for a segment
func (s *CDPService) CreateActivation(ctx context.Context, segmentID, name, description string, attributes map[string]interface{}) (*CDPActivation, error) {
	u := fmt.Sprintf("entities/segments/%s/syndications", segmentID)

	// Build the JSON API format request body
	body := map[string]interface{}{
		"type": "syndication",
		"attributes": map[string]interface{}{
			"name":        name,
			"description": description,
		},
	}

	// Merge additional attributes
	if attributes != nil {
		for key, value := range attributes {
			body["attributes"].(map[string]interface{})[key] = value
		}
	}

	req, err := s.client.NewCDPRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var response struct {
		Data CDPActivation `json:"data"`
	}
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &response.Data, nil
}

// CreateActivationWithRequest creates a new activation using a request struct
func (s *CDPService) CreateActivationWithRequest(ctx context.Context, segmentID string, req *CDPActivationCreateRequest) (*CDPActivation, error) {
	u := fmt.Sprintf("entities/segments/%s/syndications", segmentID)

	// Build the JSON API format request body
	body := map[string]interface{}{
		"type": "syndication",
		"attributes": map[string]interface{}{
			"name":          req.Name,
			"description":   req.Description,
			"type":          req.Type,
			"configuration": req.Configuration,
		},
	}

	// Add optional fields
	if req.SegmentFolderID != nil {
		body["attributes"].(map[string]interface{})["segmentFolderId"] = *req.SegmentFolderID
	}
	if req.AudienceID != nil {
		body["attributes"].(map[string]interface{})["audienceId"] = *req.AudienceID
	}

	request, err := s.client.NewCDPRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var response struct {
		Data CDPActivation `json:"data"`
	}
	_, err = s.client.Do(ctx, request, &response)
	if err != nil {
		return nil, err
	}

	return &response.Data, nil
}

// ListActivations returns a list of activations for a specific audience
func (s *CDPService) ListActivations(ctx context.Context, audienceID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("audiences/%s/syndications", audienceID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// ListSegmentActivations returns a list of activations for a specific segment
func (s *CDPService) ListSegmentActivations(ctx context.Context, segmentID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("entities/segments/%s/syndications", segmentID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var response CDPEntitiesActivationListResponse
	_, err = s.client.Do(ctx, req, &response)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: response.Data,
		Total:       int64(len(response.Data)),
	}, nil
}

// GetActivation retrieves a specific activation (syndication) by ID from an audience segment
func (s *CDPService) GetActivation(ctx context.Context, audienceID, segmentID, activationID string) (*CDPActivation, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s", audienceID, segmentID, activationID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activation CDPActivation
	_, err = s.client.Do(ctx, req, &activation)
	if err != nil {
		return nil, err
	}

	return &activation, nil
}

// UpdateActivationStatus updates the status of an activation (syndication)
func (s *CDPService) UpdateActivationStatus(ctx context.Context, audienceID, segmentID, activationID, status string) (*CDPActivation, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s/status", audienceID, segmentID, activationID)

	body := map[string]string{
		"status": status,
	}

	req, err := s.client.NewCDPRequest("PUT", u, body)
	if err != nil {
		return nil, err
	}

	var activation CDPActivation
	_, err = s.client.Do(ctx, req, &activation)
	if err != nil {
		return nil, err
	}

	return &activation, nil
}

// UpdateActivation updates an existing activation (syndication)
func (s *CDPService) UpdateActivation(ctx context.Context, audienceID, segmentID, activationID string, req *CDPActivationUpdateRequest) (*CDPActivation, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s", audienceID, segmentID, activationID)

	request, err := s.client.NewCDPRequest("PATCH", u, req)
	if err != nil {
		return nil, err
	}

	var activation CDPActivation
	_, err = s.client.Do(ctx, request, &activation)
	if err != nil {
		return nil, err
	}

	return &activation, nil
}

// DeleteActivation deletes an activation (syndication)
func (s *CDPService) DeleteActivation(ctx context.Context, audienceID, segmentID, activationID string) error {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s", audienceID, segmentID, activationID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete activation: %s", activationID)
	}

	return nil
}

// ExecuteActivation executes an activation (syndication)
func (s *CDPService) ExecuteActivation(ctx context.Context, audienceID, segmentID, activationID string) (*CDPActivationExecution, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s/executions", audienceID, segmentID, activationID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var execution CDPActivationExecution
	_, err = s.client.Do(ctx, req, &execution)
	if err != nil {
		return nil, err
	}

	return &execution, nil
}

// GetActivationExecutions retrieves execution history for an activation (syndication)
func (s *CDPService) GetActivationExecutions(ctx context.Context, audienceID, segmentID, activationID string) ([]CDPActivationExecution, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications/%s/executions", audienceID, segmentID, activationID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var executions []CDPActivationExecution
	_, err = s.client.Do(ctx, req, &executions)
	if err != nil {
		return nil, err
	}

	return executions, nil
}

// GetAudienceActivations retrieves activations for a specific audience
func (s *CDPService) GetAudienceActivations(ctx context.Context, audienceID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("audiences/%s/activations", audienceID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// GetSegmentFolderActivations retrieves activations for a specific segment folder
func (s *CDPService) GetSegmentFolderActivations(ctx context.Context, segmentFolderID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("segment_folders/%s/activations", segmentFolderID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// RunSegmentActivation runs an activation for a specific segment
func (s *CDPService) RunSegmentActivation(ctx context.Context, segmentID, activationID string) (*CDPActivationExecution, error) {
	u := fmt.Sprintf("entities/segments/%s/activations/%s/run", segmentID, activationID)

	req, err := s.client.NewCDPRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var execution CDPActivationExecution
	_, err = s.client.Do(ctx, req, &execution)
	if err != nil {
		return nil, err
	}

	return &execution, nil
}

// GetParentSegmentActivations retrieves activations for a parent segment
func (s *CDPService) GetParentSegmentActivations(ctx context.Context, parentSegmentID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("entities/parent_segments/%s/activations", parentSegmentID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
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

// GetEntitiesByFolder retrieves entities that belong to a specific folder
func (s *CDPService) GetEntitiesByFolder(ctx context.Context, folderID string) (*CDPEntityListResponse, error) {
	u := fmt.Sprintf("entities/by-folder/%s", folderID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var entities []CDPEntity
	_, err = s.client.Do(ctx, req, &entities)
	if err != nil {
		return nil, err
	}

	return &CDPEntityListResponse{
		Entities: entities,
		Total:    int64(len(entities)),
	}, nil
}

// NewEntityFolderCreateRequest creates a JSON API request body for entity folder creation
func NewEntityFolderCreateRequest(name, description string, parentFolderID *string) *CDPEntityFolderCreateRequest {
	req := &CDPEntityFolderCreateRequest{
		Type: "folder-segment",
		Attributes: CDPEntityFolderCreateAttributes{
			Name:        name,
			Description: description,
		},
	}

	if parentFolderID != nil && *parentFolderID != "" {
		req.Relationships = &CDPEntityFolderCreateRelationships{
			ParentFolder: &CDPEntityFolderParentData{
				Data: &CDPEntityFolderParentInfo{
					ID:   *parentFolderID,
					Type: "folder-segment",
				},
			},
		}
	}

	return req
}

// CreateEntityFolderWithParams creates a new entity folder with direct parameters
func (s *CDPService) CreateEntityFolderWithParams(ctx context.Context, name, description string, parentFolderID *string) (*CDPFolder, error) {
	req := &CDPFolderCreateRequest{
		Name:        name,
		Description: description,
		ParentID:    parentFolderID,
	}
	return s.CreateEntityFolder(ctx, req)
}

// CreateEntityFolder creates a new entity folder using JSON API format
func (s *CDPService) CreateEntityFolder(ctx context.Context, req *CDPFolderCreateRequest) (*CDPFolder, error) {
	u := "entities/folders"

	// Convert to JSON API format
	jsonAPIReq := NewEntityFolderCreateRequest(req.Name, req.Description, req.ParentID)

	request, err := s.client.NewCDPRequest("POST", u, jsonAPIReq)
	if err != nil {
		return nil, err
	}

	var folder CDPFolder
	_, err = s.client.Do(ctx, request, &folder)
	if err != nil {
		return nil, err
	}

	return &folder, nil
}

// GetEntityFolder retrieves a specific entity folder by ID
func (s *CDPService) GetEntityFolder(ctx context.Context, folderID string) (*CDPFolder, error) {
	u := fmt.Sprintf("entities/folders/%s", folderID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var folder CDPFolder
	_, err = s.client.Do(ctx, req, &folder)
	if err != nil {
		return nil, err
	}

	return &folder, nil
}

// UpdateEntityFolder updates an existing entity folder
func (s *CDPService) UpdateEntityFolder(ctx context.Context, folderID string, req *CDPFolderUpdateRequest) (*CDPFolder, error) {
	u := fmt.Sprintf("entities/folders/%s", folderID)

	request, err := s.client.NewCDPRequest("PATCH", u, req)
	if err != nil {
		return nil, err
	}

	var folder CDPFolder
	_, err = s.client.Do(ctx, request, &folder)
	if err != nil {
		return nil, err
	}

	return &folder, nil
}

// DeleteEntityFolder deletes an entity folder
func (s *CDPService) DeleteEntityFolder(ctx context.Context, folderID string) error {
	u := fmt.Sprintf("entities/folders/%s", folderID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete entity folder: %s", folderID)
	}

	return nil
}

// GetAudienceFolderSyndications retrieves syndications for a specific audience folder
func (s *CDPService) GetAudienceFolderSyndications(ctx context.Context, audienceID, folderID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("audiences/%s/folders/%s/syndications", audienceID, folderID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// GetSegmentSyndications retrieves syndications for a specific segment
func (s *CDPService) GetSegmentSyndications(ctx context.Context, audienceID, segmentID string, opts *CDPActivationListOptions) (*CDPActivationListResponse, error) {
	u := fmt.Sprintf("audiences/%s/segments/%s/syndications", audienceID, segmentID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// GetParentSegmentUserDefinedWorkflowProjects retrieves user-defined workflow projects for a parent segment
func (s *CDPService) GetParentSegmentUserDefinedWorkflowProjects(ctx context.Context, parentSegmentID string) (*CDPUserDefinedWorkflowProjectListResponse, error) {
	u := fmt.Sprintf("entities/parent_segments/%s/user_defined_workflow_projects", parentSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var projects []CDPUserDefinedWorkflowProject
	_, err = s.client.Do(ctx, req, &projects)
	if err != nil {
		return nil, err
	}

	return &CDPUserDefinedWorkflowProjectListResponse{
		Projects: projects,
		Total:    int64(len(projects)),
	}, nil
}

// GetParentSegmentUserDefinedWorkflows retrieves user-defined workflows for a parent segment
func (s *CDPService) GetParentSegmentUserDefinedWorkflows(ctx context.Context, parentSegmentID string) (*CDPUserDefinedWorkflowListResponse, error) {
	u := fmt.Sprintf("entities/parent_segments/%s/user_defined_workflows", parentSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var workflows []CDPUserDefinedWorkflow
	_, err = s.client.Do(ctx, req, &workflows)
	if err != nil {
		return nil, err
	}

	return &CDPUserDefinedWorkflowListResponse{
		Workflows: workflows,
		Total:     int64(len(workflows)),
	}, nil
}

// GetParentSegmentMatchedActivations retrieves matched activations for a parent segment
func (s *CDPService) GetParentSegmentMatchedActivations(ctx context.Context, parentSegmentID string) (*CDPMatchedActivationListResponse, error) {
	u := fmt.Sprintf("entities/parent_segments/%s/matched_activations", parentSegmentID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var activations []CDPMatchedActivation
	_, err = s.client.Do(ctx, req, &activations)
	if err != nil {
		return nil, err
	}

	return &CDPMatchedActivationListResponse{
		Activations: activations,
		Total:       int64(len(activations)),
	}, nil
}

// ListTokens returns a list of tokens
func (s *CDPService) ListTokens(ctx context.Context, audienceID string, opts *CDPTokenListOptions) (*CDPTokenListResponse, error) {
	u := fmt.Sprintf("audiences/%s/tokens", audienceID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var tokens []CDPToken
	_, err = s.client.Do(ctx, req, &tokens)
	if err != nil {
		return nil, err
	}

	return &CDPTokenListResponse{
		Tokens: tokens,
		Total:  int64(len(tokens)),
	}, nil
}

// GetEntityToken retrieves a specific entity token by ID
func (s *CDPService) GetEntityToken(ctx context.Context, tokenID string) (*CDPToken, error) {
	u := fmt.Sprintf("entities/tokens/%s", tokenID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var token CDPToken
	_, err = s.client.Do(ctx, req, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// UpdateEntityToken updates an existing entity token
func (s *CDPService) UpdateEntityToken(ctx context.Context, tokenID string, req *CDPTokenUpdateRequest) (*CDPToken, error) {
	u := fmt.Sprintf("entities/tokens/%s", tokenID)

	request, err := s.client.NewCDPRequest("PATCH", u, req)
	if err != nil {
		return nil, err
	}

	var token CDPToken
	_, err = s.client.Do(ctx, request, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// DeleteEntityToken deletes an entity token
func (s *CDPService) DeleteEntityToken(ctx context.Context, tokenID string) error {
	u := fmt.Sprintf("entities/tokens/%s", tokenID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete entity token: %s", tokenID)
	}

	return nil
}
