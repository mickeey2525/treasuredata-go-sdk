package treasuredata

import "time"

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

// CDPRelationshipData represents a JSON:API relationship data structure
type CDPRelationshipData struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// CDPResponseMeta represents metadata in API responses
type CDPResponseMeta struct {
	TotalCount int64                  `json:"total_count,omitempty"`
	Count      int64                  `json:"count,omitempty"`
	Limit      int                    `json:"limit,omitempty"`
	Offset     int                    `json:"offset,omitempty"`
	HasMore    bool                   `json:"has_more,omitempty"`
	NextCursor string                 `json:"next_cursor,omitempty"`
	Pagination map[string]interface{} `json:"pagination,omitempty"`
}

// CDPResponseLinks represents links in API responses
type CDPResponseLinks struct {
	Self  string `json:"self,omitempty"`
	Next  string `json:"next,omitempty"`
	Prev  string `json:"prev,omitempty"`
	First string `json:"first,omitempty"`
	Last  string `json:"last,omitempty"`
}

// CDPActivationTemplate represents an activation template
type CDPActivationTemplate struct {
	ID         string                           `json:"id"`
	Type       string                           `json:"type"`
	Attributes *CDPActivationTemplateAttributes `json:"attributes,omitempty"`
}

// CDPActivationTemplateAttributes contains activation template attributes
type CDPActivationTemplateAttributes struct {
	Name                 string                 `json:"name"`
	Description          string                 `json:"description,omitempty"`
	ActivationType       string                 `json:"activation_type"`
	ConfigurationSchema  map[string]interface{} `json:"configuration_schema,omitempty"`
	DefaultConfiguration map[string]interface{} `json:"default_configuration,omitempty"`
	IsAvailable          bool                   `json:"is_available,omitempty"`
	CreatedAt            time.Time              `json:"created_at"`
	UpdatedAt            time.Time              `json:"updated_at"`
}

// CDPActivationTemplateRequest represents a request for activation template operations
type CDPActivationTemplateRequest struct {
	Data CDPActivationTemplate `json:"data"`
}

// CDPActivationTemplateResponse represents a single activation template response
type CDPActivationTemplateResponse struct {
	Data     CDPActivationTemplate `json:"data"`
	Included []interface{}         `json:"included,omitempty"`
	Meta     *CDPResponseMeta      `json:"meta,omitempty"`
}

// CDPActivationTemplateListResponse represents a list of activation templates
type CDPActivationTemplateListResponse struct {
	Data     []CDPActivationTemplate `json:"data"`
	Included []interface{}           `json:"included,omitempty"`
	Meta     *CDPResponseMeta        `json:"meta,omitempty"`
	Links    *CDPResponseLinks       `json:"links,omitempty"`
}

// CDPParentSegment represents a parent segment in CDP
type CDPParentSegment struct {
	ID         string                      `json:"id"`
	Type       string                      `json:"type"`
	Attributes *CDPParentSegmentAttributes `json:"attributes,omitempty"`
}

// CDPParentSegmentAttributes contains parent segment attributes
type CDPParentSegmentAttributes struct {
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// CDPParentSegmentListResponse represents a list of parent segments
type CDPParentSegmentListResponse struct {
	Data     []CDPParentSegment `json:"data"`
	Included []interface{}      `json:"included,omitempty"`
	Meta     *CDPResponseMeta   `json:"meta,omitempty"`
	Links    *CDPResponseLinks  `json:"links,omitempty"`
}

// CDPParentSegmentResponse represents a single parent segment response
type CDPParentSegmentResponse struct {
	Data     CDPParentSegment `json:"data"`
	Included []interface{}    `json:"included,omitempty"`
	Meta     *CDPResponseMeta `json:"meta,omitempty"`
}
