package treasuredata

import (
	"context"
	"fmt"
	"time"
)

// PermissionsService handles communication with the access control related methods of the Treasure Data API.
type PermissionsService struct {
	client *Client
}

// AccessControlPolicy represents a permission policy
type AccessControlPolicy struct {
	ID          int    `json:"id"`
	AccountID   int    `json:"account_id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	UserCount   int    `json:"user_count,omitempty"`
}

// AccessControlPolicyGroup represents a policy group for organizing policies
type AccessControlPolicyGroup struct {
	ID           int       `json:"id,omitempty"`
	AccountID    int       `json:"account_id,omitempty"`
	Name         string    `json:"name"`
	TaggableName string    `json:"taggable_name,omitempty"`
	Description  *string   `json:"description"`
	PolicyCount  int       `json:"policy_count,omitempty"`
	CreatedAt    time.Time `json:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
}

// AccessControlPolicyGroupPolicies represents the policies in a policy group
type AccessControlPolicyGroupPolicies struct {
	PolicyIDs []int `json:"policy_ids"`
}

// AccessControlPermission represents a granular permission
type AccessControlPermission struct {
	ID               int    `json:"id"`
	PermissionSetID  int    `json:"permission_set_id"`
	ResourceType     string `json:"resource_type"`
	FilterExpression string `json:"filter_expression"`
	FilterOperator   string `json:"filter_operator"`
	FilterValue      string `json:"filter_value"`
	CanCreate        bool   `json:"can_create"`
	CanRead          bool   `json:"can_read"`
	CanUpdate        bool   `json:"can_update"`
	CanDelete        bool   `json:"can_delete"`
	CanExecute       bool   `json:"can_execute"`
	CustomOperation  string `json:"custom_operation"`
}

// AccessControlColumnPermission represents column-level permissions
type AccessControlColumnPermission struct {
	Tags    []string `json:"tags"`
	Except  *bool    `json:"except"`
	Masking *string  `json:"masking,omitempty"`
}

// AccessControlUser represents a user with access control information
type AccessControlUser struct {
	UserID      int                      `json:"user_id"`
	AccountID   int                      `json:"account_id"`
	Permissions AccessControlPermissions `json:"permissions"`
	Policies    []AccessControlPolicy    `json:"policies,omitempty"`
}

// AccessControlUserReference represents a user reference
type AccessControlUserReference struct {
	UserID    int    `json:"user_id"`
	AccountID int    `json:"account_id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
}

// AccessControlPermissions represents the comprehensive permissions structure
type AccessControlPermissions struct {
	WorkflowProject             []WorkflowProjectPermission         `json:"WorkflowProject,omitempty"`
	WorkflowProjectLevel        []WorkflowProjectLevelPermission    `json:"WorkflowProjectLevel,omitempty"`
	WorkflowRestrictedOperators []WorkflowRestrictedPermission      `json:"WorkflowRestrictedOperators,omitempty"`
	Segmentation                []SegmentationPermission            `json:"Segmentation,omitempty"`
	MasterSegmentConfigs        []MasterSegmentConfigsPermission    `json:"MasterSegmentConfigs,omitempty"`
	MasterSegmentConfig         []MasterSegmentConfigPermission     `json:"MasterSegmentConfig,omitempty"`
	MasterSegmentColumn         []MasterSegmentColumnPermission     `json:"MasterSegmentColumn,omitempty"`
	MasterSegmentAllColumns     []MasterSegmentAllColumnsPermission `json:"MasterSegmentAllColumns,omitempty"`
	CookieConsent               []CookieConsentPermission           `json:"CookieConsent,omitempty"`
	SegmentAllFolders           []SegmentAllFoldersPermission       `json:"SegmentAllFolders,omitempty"`
	SegmentFolder               []SegmentFolderPermission           `json:"SegmentFolder,omitempty"`
	Profiles                    []ProfilesPermission                `json:"Profiles,omitempty"`
	ProfilesApiToken            []ProfilesApiTokenPermission        `json:"ProfilesApiToken,omitempty"`
	ActivationTemplate          []ActivationTemplatePermission      `json:"ActivationTemplate,omitempty"`
	Authentications             []AuthenticationsPermission         `json:"Authentications,omitempty"`
	Sources                     []SourcesPermission                 `json:"Sources,omitempty"`
	Destinations                []DestinationsPermission            `json:"Destinations,omitempty"`
	Databases                   []DatabasesPermission               `json:"Databases,omitempty"`
	UniversalConsent            []UniversalConsentPermission        `json:"UniversalConsent,omitempty"`
	TrafficControls             []TrafficControlsPermission         `json:"TrafficControls,omitempty"`
	TrafficControl              []TrafficControlPermission          `json:"TrafficControl,omitempty"`
	Journeys                    []JourneysPermission                `json:"Journeys,omitempty"`
	Journey                     []JourneyPermission                 `json:"Journey,omitempty"`
	LlmProject                  []LlmProjectPermission              `json:"LlmProject,omitempty"`
}

// Permission types for different resources
type WorkflowProjectPermission struct {
	Operation string `json:"operation"` // view, run, edit
}

type WorkflowProjectLevelPermission struct {
	Operation string `json:"operation"` // view, run, edit
	Name      string `json:"name"`
}

type WorkflowRestrictedPermission struct {
	Operation string `json:"operation"` // edit
}

type SegmentationPermission struct {
	Operation string `json:"operation"` // full
}

type MasterSegmentConfigsPermission struct {
	Operation string `json:"operation"` // view, edit, owner_manage
}

type MasterSegmentConfigPermission struct {
	Operation string `json:"operation"` // view, edit
	ID        string `json:"id"`
}

type MasterSegmentColumnPermission struct {
	Operation         string `json:"operation"` // view_clear, view_pii, blocked
	ColumnIdentifiers string `json:"column_identifiers,omitempty"`
}

type MasterSegmentAllColumnsPermission struct {
	Operation  string `json:"operation"` // view_clear, view_pii, blocked_only_for_migration_purpose
	AudienceID string `json:"audience_id"`
}

type CookieConsentPermission struct {
	Operation string `json:"operation"` // view, edit, full
}

type SegmentAllFoldersPermission struct {
	Operation  string `json:"operation"` // view, edit
	AudienceID string `json:"audience_id"`
}

type SegmentFolderPermission struct {
	Operation string `json:"operation"` // view, edit
	ID        string `json:"id"`
}

type ProfilesPermission struct {
	Operation  string `json:"operation"` // view
	AudienceID string `json:"audience_id"`
}

type ProfilesApiTokenPermission struct {
	Operation  string `json:"operation"` // full
	AudienceID string `json:"audience_id"`
}

type ActivationTemplatePermission struct {
	Operation string `json:"operation"` // view, full, template_access
}

type AuthenticationsPermission struct {
	Operation string `json:"operation"` // use_limited, use, full, owner_manage
	IDs       string `json:"ids,omitempty"`
}

type SourcesPermission struct {
	Operation string `json:"operation"` // restricted, full
}

type DestinationsPermission struct {
	Operation string `json:"operation"` // restricted, full
}

type DatabasesPermission struct {
	Operation string `json:"operation"` // query, edit, import, manage, owner_manage, download
	IDs       string `json:"ids,omitempty"`
}

type UniversalConsentPermission struct {
	Operation string `json:"operation"` // full
}

type TrafficControlsPermission struct {
	Operation string `json:"operation"` // full, view
}

type TrafficControlPermission struct {
	Operation  string `json:"operation"` // full, view
	AudienceID string `json:"audience_id"`
}

type JourneysPermission struct {
	Operation string `json:"operation"` // full, edit, view
}

type JourneyPermission struct {
	Operation  string `json:"operation"` // full, edit, view
	AudienceID string `json:"audience_id"`
}

type LlmProjectPermission struct {
	Operation string `json:"operation"` // full, edit, chat, publish_internal_integration, publish_external_integration
	ProjectID string `json:"project_id,omitempty"`
}

// === Request/Response structures ===

// CreateAccessControlPolicyRequest represents options for creating a policy
type CreateAccessControlPolicyRequest struct {
	Policy struct {
		Name        string `json:"name"`
		Description string `json:"description,omitempty"`
	} `json:"policy"`
}

// UpdateAccessControlPolicyRequest represents options for updating a policy
type UpdateAccessControlPolicyRequest struct {
	Policy struct {
		Name        string `json:"name,omitempty"`
		Description string `json:"description,omitempty"`
	} `json:"policy"`
}

// CreateAccessControlPolicyGroupRequest represents options for creating a policy group
type CreateAccessControlPolicyGroupRequest struct {
	Name string `json:"name"`
}

// UpdateAccessControlPolicyGroupRequest represents options for updating a policy group
type UpdateAccessControlPolicyGroupRequest struct {
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
}

// UpdateAccessControlPolicyGroupPoliciesRequest represents options for updating policy group policies
type UpdateAccessControlPolicyGroupPoliciesRequest struct {
	PolicyIDs []int `json:"policy_ids"`
}

// UpdateAccessControlPoliciesRequest represents options for updating user policies
type UpdateAccessControlPoliciesRequest struct {
	PolicyIDs []string `json:"policy_ids"`
}

// UpdateAccessControlPolicyUsersRequest represents options for updating policy users
type UpdateAccessControlPolicyUsersRequest struct {
	UserIDs []int `json:"user_ids"`
}

// ListPoliciesOptions represents options for listing policies
type ListPoliciesOptions struct {
	ColumnPermissionTag string `url:"column_permission_tag,omitempty"`
}

// === Policy Management ===

// ListPolicies returns all policies
func (s *PermissionsService) ListPolicies(ctx context.Context, opts *ListPoliciesOptions) ([]AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/policies", apiVersion)

	if opts != nil {
		var err error
		u, err = addOptions(u, opts)
		if err != nil {
			return nil, err
		}
	}

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var policies []AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policies)
	if err != nil {
		return nil, err
	}

	return policies, nil
}

// GetPolicy returns a specific policy by ID
func (s *PermissionsService) GetPolicy(ctx context.Context, policyID int) (*AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d", apiVersion, policyID)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var policy AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policy)
	if err != nil {
		return nil, err
	}

	return &policy, nil
}

// CreatePolicy creates a new policy
func (s *PermissionsService) CreatePolicy(ctx context.Context, name, description string) (*AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/policies", apiVersion)

	body := CreateAccessControlPolicyRequest{}
	body.Policy.Name = name
	if description != "" {
		body.Policy.Description = description
	}

	req, err := s.client.NewRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var policy AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policy)
	if err != nil {
		return nil, err
	}

	return &policy, nil
}

// UpdatePolicy updates a policy
func (s *PermissionsService) UpdatePolicy(ctx context.Context, policyID int, name, description string) (*AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d", apiVersion, policyID)

	body := UpdateAccessControlPolicyRequest{}
	if name != "" {
		body.Policy.Name = name
	}
	if description != "" {
		body.Policy.Description = description
	}

	req, err := s.client.NewRequest("PATCH", u, body)
	if err != nil {
		return nil, err
	}

	var policy AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policy)
	if err != nil {
		return nil, err
	}

	return &policy, nil
}

// DeletePolicy deletes a policy
func (s *PermissionsService) DeletePolicy(ctx context.Context, policyID int) (*AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d", apiVersion, policyID)

	req, err := s.client.NewRequest("DELETE", u, nil)
	if err != nil {
		return nil, err
	}

	var policy AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policy)
	if err != nil {
		return nil, err
	}

	return &policy, nil
}

// === User Policy Management ===

// ListUserPolicies lists policies for a specific user
func (s *PermissionsService) ListUserPolicies(ctx context.Context, userID int) ([]AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/users/%d/policies", apiVersion, userID)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var policies []AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policies)
	if err != nil {
		return nil, err
	}

	return policies, nil
}

// UpdateUserPolicies updates policies for a user
func (s *PermissionsService) UpdateUserPolicies(ctx context.Context, userID int, policyIDs []string) ([]AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/users/%d/policies", apiVersion, userID)

	body := UpdateAccessControlPoliciesRequest{
		PolicyIDs: policyIDs,
	}

	req, err := s.client.NewRequest("PATCH", u, body)
	if err != nil {
		return nil, err
	}

	var policies []AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policies)
	if err != nil {
		return nil, err
	}

	return policies, nil
}

// AttachUserToPolicy attaches a user to a policy
func (s *PermissionsService) AttachUserToPolicy(ctx context.Context, userID, policyID int) (*AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/users/%d/policies/%d", apiVersion, userID, policyID)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var policy AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policy)
	if err != nil {
		return nil, err
	}

	return &policy, nil
}

// DetachUserFromPolicy detaches a user from a policy
func (s *PermissionsService) DetachUserFromPolicy(ctx context.Context, userID, policyID int) (*AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/users/%d/policies/%d", apiVersion, userID, policyID)

	req, err := s.client.NewRequest("DELETE", u, nil)
	if err != nil {
		return nil, err
	}

	var policy AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policy)
	if err != nil {
		return nil, err
	}

	return &policy, nil
}

// === Policy Groups Management ===

// ListPolicyGroups returns all policy groups
func (s *PermissionsService) ListPolicyGroups(ctx context.Context) ([]AccessControlPolicyGroup, error) {
	u := fmt.Sprintf("%s/access_control/policy_groups", apiVersion)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var groups []AccessControlPolicyGroup
	_, err = s.client.Do(ctx, req, &groups)
	if err != nil {
		return nil, err
	}

	return groups, nil
}

// GetPolicyGroup returns a specific policy group by ID or taggable name
func (s *PermissionsService) GetPolicyGroup(ctx context.Context, groupIDOrName string) (*AccessControlPolicyGroup, error) {
	u := fmt.Sprintf("%s/access_control/policy_groups/%s", apiVersion, groupIDOrName)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var group AccessControlPolicyGroup
	_, err = s.client.Do(ctx, req, &group)
	if err != nil {
		return nil, err
	}

	return &group, nil
}

// CreatePolicyGroup creates a new policy group
func (s *PermissionsService) CreatePolicyGroup(ctx context.Context, name string) (*AccessControlPolicyGroup, error) {
	u := fmt.Sprintf("%s/access_control/policy_groups", apiVersion)

	body := CreateAccessControlPolicyGroupRequest{
		Name: name,
	}

	req, err := s.client.NewRequest("POST", u, body)
	if err != nil {
		return nil, err
	}

	var group AccessControlPolicyGroup
	_, err = s.client.Do(ctx, req, &group)
	if err != nil {
		return nil, err
	}

	return &group, nil
}

// UpdatePolicyGroup updates a policy group
func (s *PermissionsService) UpdatePolicyGroup(ctx context.Context, groupIDOrName, name string, description *string) (*AccessControlPolicyGroup, error) {
	u := fmt.Sprintf("%s/access_control/policy_groups/%s", apiVersion, groupIDOrName)

	body := UpdateAccessControlPolicyGroupRequest{
		Name:        name,
		Description: description,
	}

	req, err := s.client.NewRequest("PATCH", u, body)
	if err != nil {
		return nil, err
	}

	var group AccessControlPolicyGroup
	_, err = s.client.Do(ctx, req, &group)
	if err != nil {
		return nil, err
	}

	return &group, nil
}

// DeletePolicyGroup deletes a policy group
func (s *PermissionsService) DeletePolicyGroup(ctx context.Context, groupIDOrName string) error {
	u := fmt.Sprintf("%s/access_control/policy_groups/%s", apiVersion, groupIDOrName)

	req, err := s.client.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// ListPolicyGroupPolicies lists all policies in a policy group
func (s *PermissionsService) ListPolicyGroupPolicies(ctx context.Context, groupIDOrName string) (*AccessControlPolicyGroupPolicies, error) {
	u := fmt.Sprintf("%s/access_control/policy_groups/%s/policies", apiVersion, groupIDOrName)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var policies AccessControlPolicyGroupPolicies
	_, err = s.client.Do(ctx, req, &policies)
	if err != nil {
		return nil, err
	}

	return &policies, nil
}

// UpdatePolicyGroupPolicies updates the policies attached to a policy group
func (s *PermissionsService) UpdatePolicyGroupPolicies(ctx context.Context, groupIDOrName string, policyIDs []int) (*AccessControlPolicyGroupPolicies, error) {
	u := fmt.Sprintf("%s/access_control/policy_groups/%s/policies", apiVersion, groupIDOrName)

	body := UpdateAccessControlPolicyGroupPoliciesRequest{
		PolicyIDs: policyIDs,
	}

	req, err := s.client.NewRequest("PATCH", u, body)
	if err != nil {
		return nil, err
	}

	var policies AccessControlPolicyGroupPolicies
	_, err = s.client.Do(ctx, req, &policies)
	if err != nil {
		return nil, err
	}

	return &policies, nil
}

// === Policy Permissions Management ===

// GetPolicyPermissions shows policy permissions by policy ID
func (s *PermissionsService) GetPolicyPermissions(ctx context.Context, policyID int) (*AccessControlPermissions, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d/permissions", apiVersion, policyID)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var permissions AccessControlPermissions
	_, err = s.client.Do(ctx, req, &permissions)
	if err != nil {
		return nil, err
	}

	return &permissions, nil
}

// UpdatePolicyPermissions updates policy permissions
func (s *PermissionsService) UpdatePolicyPermissions(ctx context.Context, policyID int, permissions *AccessControlPermissions) (*AccessControlPermissions, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d/permissions", apiVersion, policyID)

	req, err := s.client.NewRequest("PATCH", u, permissions)
	if err != nil {
		return nil, err
	}

	var updatedPermissions AccessControlPermissions
	_, err = s.client.Do(ctx, req, &updatedPermissions)
	if err != nil {
		return nil, err
	}

	return &updatedPermissions, nil
}

// GetColumnPermissions shows column permissions by policy ID
func (s *PermissionsService) GetColumnPermissions(ctx context.Context, policyID int) ([]AccessControlColumnPermission, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d/column_permissions", apiVersion, policyID)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var permissions []AccessControlColumnPermission
	_, err = s.client.Do(ctx, req, &permissions)
	if err != nil {
		return nil, err
	}

	return permissions, nil
}

// UpdateColumnPermissions updates column permissions by policy ID
func (s *PermissionsService) UpdateColumnPermissions(ctx context.Context, policyID int, permissions []AccessControlColumnPermission) ([]AccessControlColumnPermission, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d/column_permissions", apiVersion, policyID)

	body := map[string]any{
		"column_permissions": permissions,
	}

	req, err := s.client.NewRequest("PATCH", u, body)
	if err != nil {
		return nil, err
	}

	var updatedPermissions []AccessControlColumnPermission
	_, err = s.client.Do(ctx, req, &updatedPermissions)
	if err != nil {
		return nil, err
	}

	return updatedPermissions, nil
}

// === Access Control Users ===

// ListAccessControlUsers retrieves a list of users and their permissions
func (s *PermissionsService) ListAccessControlUsers(ctx context.Context) ([]AccessControlUser, error) {
	u := fmt.Sprintf("%s/access_control/users", apiVersion)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var users []AccessControlUser
	_, err = s.client.Do(ctx, req, &users)
	if err != nil {
		return nil, err
	}

	return users, nil
}

// GetAccessControlUser retrieves the specified user
func (s *PermissionsService) GetAccessControlUser(ctx context.Context, userID int) (*AccessControlUser, error) {
	u := fmt.Sprintf("%s/access_control/users/%d", apiVersion, userID)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var user AccessControlUser
	_, err = s.client.Do(ctx, req, &user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

// GetPolicyUsers retrieves a list of users attached to a policy
func (s *PermissionsService) GetPolicyUsers(ctx context.Context, policyID int) ([]AccessControlUserReference, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d/users", apiVersion, policyID)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var users []AccessControlUserReference
	_, err = s.client.Do(ctx, req, &users)
	if err != nil {
		return nil, err
	}

	return users, nil
}

// UpdatePolicyUsers updates users attached to a policy
func (s *PermissionsService) UpdatePolicyUsers(ctx context.Context, policyID int, userIDs []int) ([]AccessControlUser, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d/users", apiVersion, policyID)

	body := UpdateAccessControlPolicyUsersRequest{
		UserIDs: userIDs,
	}

	req, err := s.client.NewRequest("PATCH", u, body)
	if err != nil {
		return nil, err
	}

	var users []AccessControlUser
	_, err = s.client.Do(ctx, req, &users)
	if err != nil {
		return nil, err
	}

	return users, nil
}

// AttachPolicyToUser attaches a policy to a user
func (s *PermissionsService) AttachPolicyToUser(ctx context.Context, policyID, userID int) (*AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d/users/%d", apiVersion, policyID, userID)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var policy AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policy)
	if err != nil {
		return nil, err
	}

	return &policy, nil
}

// DetachPolicyFromUser detaches a user from a policy
func (s *PermissionsService) DetachPolicyFromUser(ctx context.Context, policyID, userID int) (*AccessControlPolicy, error) {
	u := fmt.Sprintf("%s/access_control/policies/%d/users/%d", apiVersion, policyID, userID)

	req, err := s.client.NewRequest("DELETE", u, nil)
	if err != nil {
		return nil, err
	}

	var policy AccessControlPolicy
	_, err = s.client.Do(ctx, req, &policy)
	if err != nil {
		return nil, err
	}

	return &policy, nil
}
