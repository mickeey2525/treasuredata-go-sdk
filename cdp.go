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
	ID              string      `json:"id"`
	AudienceID      string      `json:"audienceId"`
	Name            string      `json:"name"`
	Description     string      `json:"description"`
	Realtime        bool        `json:"realtime"`
	IsVisible       bool        `json:"isVisible"`
	NumSyndications int         `json:"numSyndications"`
	SegmentFolderID string      `json:"segmentFolderId,omitempty"`
	Population      int64       `json:"population"`
	CreatedAt       TDTime      `json:"createdAt"`
	UpdatedAt       TDTime      `json:"updatedAt"`
	CreatedBy       *CDPUser    `json:"createdBy,omitempty"`
	UpdatedBy       *CDPUser    `json:"updatedBy,omitempty"`
	Kind            int         `json:"kind"`
	Rule            interface{} `json:"rule,omitempty"`
	// Legacy fields for compatibility
	Query        string `json:"query,omitempty"`
	ProfileCount int64  `json:"profile_count,omitempty"`
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
	AudienceID           string  `json:"audienceId"`
	ID                   string  `json:"id"`
	Name                 string  `json:"name"`
	Type                 string  `json:"type"`
	ParentDatabaseName   string  `json:"parentDatabaseName"`
	ParentTableName      string  `json:"parentTableName"`
	ParentColumn         string  `json:"parentColumn"`
	ParentKey            string  `json:"parentKey"`
	ForeignKey           string  `json:"foreignKey"`
	MatrixColumnName     string  `json:"matrixColumnName"`
	GroupingName         *string `json:"groupingName"`
	UsedBySegmentInsight bool    `json:"usedBySegmentInsight,omitempty"`
	Visibility           string  `json:"visibility,omitempty"`
}

// CDPAudienceMasterAttribute represents a master table attribute
type CDPAudienceMasterAttribute struct {
	Name               string `json:"name"`
	Type               string `json:"type"`
	ParentDatabaseName string `json:"parentDatabaseName"`
	ParentTableName    string `json:"parentTableName"`
	ParentColumn       string `json:"parentColumn"`
	ParentKey          string `json:"parentKey"`
	ForeignKey         string `json:"foreignKey"`
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

// CDPActivationColumn represents a column mapping in an activation
type CDPActivationColumn struct {
	ID     string                    `json:"id"`
	Column string                    `json:"column"`
	Source CDPActivationColumnSource `json:"source"`
}

// CDPActivationColumnSource represents the source mapping for an activation column
type CDPActivationColumnSource struct {
	Column     string                         `json:"column,omitempty"`
	Parameters []CDPActivationColumnParameter `json:"parameters,omitempty"`
	String     string                         `json:"string,omitempty"`
	Functions  []interface{}                  `json:"functions,omitempty"`
}

// CDPActivationColumnParameter represents a parameter in an activation column source
type CDPActivationColumnParameter struct {
	Type   string `json:"type"`
	String string `json:"string,omitempty"`
	Format string `json:"format,omitempty"`
}

// CDPActivation represents an activation configuration
type CDPActivation struct {
	ID                   string                   `json:"id"`
	Name                 string                   `json:"name"`
	Type                 string                   `json:"type"`
	Description          string                   `json:"description"`
	SegmentID            string                   `json:"segmentId"`
	AudienceID           string                   `json:"audienceId"`
	ActivationTemplateID string                   `json:"activationTemplateId"`
	AllColumns           bool                     `json:"allColumns"`
	ConnectionID         string                   `json:"connectionId"`
	ScheduleType         string                   `json:"scheduleType"`
	ScheduleOption       *string                  `json:"scheduleOption"`
	RepeatSubFrequency   []int                    `json:"repeatSubFrequency"`
	Timezone             string                   `json:"timezone"`
	CreatedBy            *CDPUser                 `json:"createdBy"`
	UpdatedBy            *CDPUser                 `json:"updatedBy"`
	NotifyOn             []string                 `json:"notifyOn"`
	EmailRecipients      []int                    `json:"emailRecipients"`
	ConnectorConfig      map[string]interface{}   `json:"connectorConfig"`
	Columns              []CDPActivationColumn    `json:"columns"`
	Valid                bool                     `json:"valid"`
	Executions           []CDPActivationExecution `json:"executions"`
	Configuration        map[string]interface{}   `json:"configuration"`
	Status               string                   `json:"status"`
	CreatedAt            TDTime                   `json:"createdAt"`
	UpdatedAt            TDTime                   `json:"updatedAt"`
	// Legacy fields for compatibility
	Audience_ID string `json:"audience_id,omitempty"`
}

// CDPActivationExecution represents an execution of an activation
type CDPActivationExecution struct {
	ID                string  `json:"id,omitempty"`
	SyndicationID     string  `json:"syndicationId"`
	WorkflowID        string  `json:"workflowId"`
	WorkflowSessionID string  `json:"workflowSessionId"`
	WorkflowAttemptID string  `json:"workflowAttemptId"`
	CreatedAt         TDTime  `json:"createdAt"`
	FinishedAt        *TDTime `json:"finishedAt"`
	Status            string  `json:"status"`
	// Legacy fields for compatibility
	ActivationID    string `json:"activation_id,omitempty"`
	RecordsExported int64  `json:"records_exported,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
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

// CDPAudienceAttributeListResponse represents the response from the audience attributes list API
type CDPAudienceAttributeListResponse struct {
	Attributes []interface{} `json:"attributes"` // Can be CDPAudienceAttribute or CDPAudienceMasterAttribute
}

// CDPPaginationMetadata represents pagination information returned by the API
type CDPPaginationMetadata struct {
	HasNext          bool   `json:"hasNext,omitempty"`
	NextPage         string `json:"nextPage,omitempty"`
	ApproximateCount int64  `json:"approximateCount,omitempty"`
}

// CDPPaginatedResponse represents a paginated API response with data and pagination metadata
type CDPPaginatedResponse struct {
	Data       []interface{}          `json:"data"`
	Pagination *CDPPaginationMetadata `json:"pagination,omitempty"`
}

// CDPJSONAPIResource represents a JSON:API resource
type CDPJSONAPIResource struct {
	ID            string                 `json:"id"`
	Type          string                 `json:"type"`
	Attributes    map[string]interface{} `json:"attributes"`
	Relationships map[string]interface{} `json:"relationships,omitempty"`
}

// CDPJSONAPIRequest represents a JSON:API request
type CDPJSONAPIRequest struct {
	Data CDPJSONAPIResource `json:"data"`
}

// CDPJSONAPIResponse represents a JSON:API response
type CDPJSONAPIResponse struct {
	Data     interface{}   `json:"data"`
	Included []interface{} `json:"included,omitempty"`
}

// CDPJSONAPIListResponse represents a JSON:API list response
type CDPJSONAPIListResponse struct {
	Data     []CDPJSONAPIResource `json:"data"`
	Included []interface{}        `json:"included,omitempty"`
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

// CDPAudienceFolderUpdateRequest represents a request to update an audience folder
type CDPAudienceFolderUpdateRequest struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
}

// CDPAudienceUpdateRequest represents a request to update an audience
type CDPAudienceUpdateRequest struct {
	Name                         string                 `json:"name,omitempty"`
	Description                  string                 `json:"description,omitempty"`
	ScheduleType                 string                 `json:"scheduleType,omitempty"`
	ScheduleOption               *string                `json:"scheduleOption,omitempty"`
	Timezone                     string                 `json:"timezone,omitempty"`
	WorkflowHiveOnly             *bool                  `json:"workflowHiveOnly,omitempty"`
	HiveEngineVersion            string                 `json:"hiveEngineVersion,omitempty"`
	HivePoolName                 *string                `json:"hivePoolName,omitempty"`
	PrestoPoolName               *string                `json:"prestoPoolName,omitempty"`
	AllowActivationBehavior      *bool                  `json:"allowActivationBehavior,omitempty"`
	MaxActivationBehaviorRow     *int                   `json:"maxActivationBehaviorRow,omitempty"`
	LLMEnabled                   *bool                  `json:"llmEnabled,omitempty"`
	LLMState                     string                 `json:"llmState,omitempty"`
	EnrichmentWordTaggingEnabled *bool                  `json:"enrichmentWordTaggingEnabled,omitempty"`
	EnrichmentIPEnabled          *bool                  `json:"enrichmentIpEnabled,omitempty"`
	EnrichmentTdJsSdkEnabled     *bool                  `json:"enrichmentTdJsSdkEnabled,omitempty"`
	Master                       *CDPAudienceMaster     `json:"master,omitempty"`
	Attributes                   []CDPAudienceAttribute `json:"attributes,omitempty"`
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

// CDPTokenCreateRequest represents a request to create a token
type CDPTokenCreateRequest struct {
	Name             string   `json:"name"`
	Description      string   `json:"description,omitempty"`
	KeyColumn        string   `json:"keyColumn"`
	AttributeColumns []string `json:"attributeColumns"`
}

// CDPLegacyTokenRequest represents a legacy token request for audience-level operations
type CDPLegacyTokenRequest struct {
	Description      string           `json:"description,omitempty"`
	Token            string           `json:"token,omitempty"`
	KeyColumn        string           `json:"keyColumn"`
	Segments         []map[string]int `json:"segments,omitempty"`
	AttributeColumns []string         `json:"attributeColumns"`
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

// GetEntitiesByFolder retrieves entities that belong to a specific folder using JSON:API format
func (s *CDPService) GetEntitiesByFolder(ctx context.Context, folderID string) (*CDPJSONAPIListResponse, error) {
	u := fmt.Sprintf("entities/by-folder/%s", folderID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
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

// GetEntityFolder retrieves a specific entity folder by ID using JSON:API format
func (s *CDPService) GetEntityFolder(ctx context.Context, folderID string) (*CDPJSONAPIResponse, error) {
	u := fmt.Sprintf("entities/folders/%s", folderID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
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

// CreateToken creates a new token for an audience (legacy)
func (s *CDPService) CreateToken(ctx context.Context, audienceID string, req *CDPLegacyTokenRequest) (*CDPToken, error) {
	u := fmt.Sprintf("audiences/%s/tokens", audienceID)

	request, err := s.client.NewCDPRequest("POST", u, req)
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

// GetToken retrieves a specific token by ID from an audience (legacy)
func (s *CDPService) GetToken(ctx context.Context, audienceID, tokenID string) (*CDPToken, error) {
	u := fmt.Sprintf("audiences/%s/tokens/%s", audienceID, tokenID)

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

// UpdateToken updates an existing token for an audience (legacy)
func (s *CDPService) UpdateToken(ctx context.Context, audienceID, tokenID string, req *CDPLegacyTokenRequest) (*CDPToken, error) {
	u := fmt.Sprintf("audiences/%s/tokens/%s", audienceID, tokenID)

	request, err := s.client.NewCDPRequest("PUT", u, req)
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

// DeleteToken deletes a token from an audience (legacy)
func (s *CDPService) DeleteToken(ctx context.Context, audienceID, tokenID string) error {
	u := fmt.Sprintf("audiences/%s/tokens/%s", audienceID, tokenID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete token: %s", tokenID)
	}

	return nil
}

// CreateEntityToken creates a new entity token
func (s *CDPService) CreateEntityToken(ctx context.Context, req *CDPTokenCreateRequest) (*CDPToken, error) {
	u := "entities/tokens"

	request, err := s.client.NewCDPRequest("POST", u, req)
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

// ==========================================
// Funnel Management API
// ==========================================

// CDPFunnel represents a funnel in CDP
type CDPFunnel struct {
	ID                string           `json:"id"`
	AudienceID        string           `json:"audienceId"`
	SegmentFolderID   string           `json:"segmentFolderId"`
	Name              string           `json:"name"`
	Description       string           `json:"description"`
	Population        *int64           `json:"population"`
	NumSyndications   int64            `json:"numSyndications"`
	NeedToRunWorkflow bool             `json:"needToRunWorkflow"`
	Stages            []CDPFunnelStage `json:"stages,omitempty"`
	CreatedAt         TDTime           `json:"createdAt,omitempty"`
	UpdatedAt         TDTime           `json:"updatedAt,omitempty"`
}

// CDPFunnelStage represents a funnel stage
type CDPFunnelStage struct {
	ID              string `json:"id,omitempty"`
	Name            string `json:"name"`
	NumSyndication  int64  `json:"numSyndication,omitempty"`
	AudienceID      string `json:"audienceId,omitempty"`
	FunnelID        string `json:"funnelId,omitempty"`
	SegmentFolderID string `json:"segmentFolderId,omitempty"`
	SegmentID       string `json:"segmentId,omitempty"`
}

// CDPFunnelCreateRequest represents parameters for creating a funnel
type CDPFunnelCreateRequest struct {
	Name            string           `json:"name"`
	Description     string           `json:"description"`
	SegmentFolderID int64            `json:"segmentFolderId"`
	Stages          []CDPFunnelStage `json:"stages"`
}

// CDPFunnelCloneRequest represents parameters for cloning a funnel
type CDPFunnelCloneRequest struct {
	Name            string `json:"name"`
	Description     string `json:"description"`
	SegmentFolderID int64  `json:"segmentFolderId"`
}

// CDPFunnelStatistic represents funnel population statistics
type CDPFunnelStatistic struct {
	Population *int64                    `json:"population"`
	Stages     []CDPFunnelStageStatistic `json:"stages"`
}

// CDPFunnelStageStatistic represents statistics for a funnel stage
type CDPFunnelStageStatistic struct {
	ID      int64           `json:"id"`
	History [][]interface{} `json:"history"` // Array of [timestamp, count, hasData]
}

// Entity API types for modern funnel management
type CDPFunnelEntityCreateRequest struct {
	ID            string                       `json:"id,omitempty"`
	Type          string                       `json:"type"`
	Attributes    CDPFunnelEntityAttributes    `json:"attributes"`
	Relationships CDPFunnelEntityRelationships `json:"relationships"`
}

type CDPFunnelEntityAttributes struct {
	Name        string                 `json:"name"`
	Description *string                `json:"description"`
	Stages      []CDPFunnelStageEntity `json:"stages"`
}

type CDPFunnelEntityRelationships struct {
	ParentFolder *CDPEntityFolderParentData `json:"parentFolder,omitempty"`
}

type CDPFunnelStageEntity struct {
	ID              string `json:"id,omitempty"`
	SegmentID       string `json:"segmentId,omitempty"`
	Name            string `json:"name"`
	NumSyndications int64  `json:"numSyndications,omitempty"`
}

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

// ==========================================
// Predictive Segments API
// ==========================================

// CDPPredictiveSegment represents a predictive segment in CDP
type CDPPredictiveSegment struct {
	ID                string                        `json:"id"`
	AudienceID        string                        `json:"audienceId"`
	BaseSegmentID     string                        `json:"baseSegmentId"`
	SegmentID         string                        `json:"segmentId"`
	SegmentFolderID   string                        `json:"segmentFolderId"`
	Name              string                        `json:"name"`
	Description       string                        `json:"description"`
	PredictiveName    string                        `json:"predictiveName"`
	PredictiveColumn  string                        `json:"predictiveColumn"`
	ModelType         string                        `json:"modelType"`
	TrainingPeriod    int                           `json:"trainingPeriod"`
	PredictionPeriod  int                           `json:"predictionPeriod"`
	FeatureColumns    []string                      `json:"featureColumns"`
	GradeThresholds   []float64                     `json:"gradeThresholds"` // Array of 3 numbers
	Status            string                        `json:"status"`
	Population        *int64                        `json:"population"`
	Accuracy          *CDPPredictiveSegmentAccuracy `json:"accuracy,omitempty"`
	AreaUnderRocCurve *CDPPredictiveSegmentROC      `json:"areaUnderRocCurve,omitempty"`
	CreatedAt         TDTime                        `json:"createdAt,omitempty"`
	UpdatedAt         TDTime                        `json:"updatedAt,omitempty"`
}

// CDPPredictiveSegmentAccuracy represents model accuracy metrics
type CDPPredictiveSegmentAccuracy struct {
	TrainingAccuracy   float64 `json:"trainingAccuracy"`
	ValidationAccuracy float64 `json:"validationAccuracy"`
}

// CDPPredictiveSegmentROC represents ROC curve metrics
type CDPPredictiveSegmentROC struct {
	TrainingROC   float64 `json:"trainingROC"`
	ValidationROC float64 `json:"validationROC"`
}

// CDPPredictiveSegmentCreateRequest represents parameters for creating a predictive segment
type CDPPredictiveSegmentCreateRequest struct {
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	BaseSegmentID    string    `json:"baseSegmentId"`
	SegmentFolderID  string    `json:"segmentFolderId"`
	PredictiveName   string    `json:"predictiveName"`
	PredictiveColumn string    `json:"predictiveColumn"`
	ModelType        string    `json:"modelType"`
	TrainingPeriod   int       `json:"trainingPeriod"`
	PredictionPeriod int       `json:"predictionPeriod"`
	FeatureColumns   []string  `json:"featureColumns"`
	GradeThresholds  []float64 `json:"gradeThresholds"`
}

// CDPPredictiveSegmentExecution represents a predictive segment execution
type CDPPredictiveSegmentExecution struct {
	ID                  string  `json:"id"`
	PredictiveSegmentID string  `json:"predictiveSegmentId"`
	Status              string  `json:"status"`
	Message             string  `json:"message,omitempty"`
	CreatedAt           TDTime  `json:"createdAt"`
	StartedAt           *TDTime `json:"startedAt,omitempty"`
	FinishedAt          *TDTime `json:"finishedAt,omitempty"`
}

// CDPPredictiveSegmentRule represents a guessed rule for predictive segments
type CDPPredictiveSegmentRule struct {
	Query  string `json:"query"`
	Status string `json:"status"`
}

// CDPPredictiveSegmentGuessRuleResponse represents the response for guess rule async
type CDPPredictiveSegmentGuessRuleResponse struct {
	Status string                    `json:"status"`
	Rule   *CDPPredictiveSegmentRule `json:"rule,omitempty"`
}

// Entity API types for modern predictive segment management
type CDPPredictiveSegmentEntityCreateRequest struct {
	ID            string                                  `json:"id,omitempty"`
	Type          string                                  `json:"type"`
	Attributes    CDPPredictiveSegmentEntityAttributes    `json:"attributes"`
	Relationships CDPPredictiveSegmentEntityRelationships `json:"relationships"`
}

type CDPPredictiveSegmentEntityAttributes struct {
	Name             string    `json:"name"`
	Description      *string   `json:"description"`
	PredictiveName   string    `json:"predictiveName"`
	PredictiveColumn string    `json:"predictiveColumn"`
	ModelType        string    `json:"modelType"`
	TrainingPeriod   int       `json:"trainingPeriod"`
	PredictionPeriod int       `json:"predictionPeriod"`
	FeatureColumns   []string  `json:"featureColumns"`
	GradeThresholds  []float64 `json:"gradeThresholds"`
}

type CDPPredictiveSegmentEntityRelationships struct {
	BaseSegment  *CDPEntityFolderParentData `json:"baseSegment,omitempty"`
	ParentFolder *CDPEntityFolderParentData `json:"parentFolder,omitempty"`
}

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
	u := fmt.Sprintf("entities/predictive_segments/%s/model/score", predictiveSegmentID)

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
