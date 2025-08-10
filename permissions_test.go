package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestPermissionsService_ListPolicies(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/policies", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/access_control/policies")

		fmt.Fprint(w, `[
			{
				"id": 1,
				"account_id": 100,
				"name": "Analysts Policy",
				"description": "Policy for data analysts",
				"user_count": 5
			},
			{
				"id": 2,
				"account_id": 100,
				"name": "Restricted Access",
				"description": "Limited access policy",
				"user_count": 2
			}
		]`)
	})

	ctx := context.Background()
	policies, err := client.Permissions.ListPolicies(ctx, nil)
	if err != nil {
		t.Errorf("Permissions.ListPolicies returned error: %v", err)
	}

	want := []AccessControlPolicy{
		{
			ID:          1,
			AccountID:   100,
			Name:        "Analysts Policy",
			Description: "Policy for data analysts",
			UserCount:   5,
		},
		{
			ID:          2,
			AccountID:   100,
			Name:        "Restricted Access",
			Description: "Limited access policy",
			UserCount:   2,
		},
	}

	if !reflect.DeepEqual(policies, want) {
		t.Errorf("Permissions.ListPolicies returned %+v, want %+v", policies, want)
	}
}

func TestPermissionsService_GetPolicy(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/policies/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/access_control/policies/1")

		fmt.Fprint(w, `{
			"id": 1,
			"account_id": 100,
			"name": "Analysts Policy",
			"description": "Policy for data analysts",
			"user_count": 5
		}`)
	})

	ctx := context.Background()
	policy, err := client.Permissions.GetPolicy(ctx, 1)
	if err != nil {
		t.Errorf("Permissions.GetPolicy returned error: %v", err)
	}

	want := &AccessControlPolicy{
		ID:          1,
		AccountID:   100,
		Name:        "Analysts Policy",
		Description: "Policy for data analysts",
		UserCount:   5,
	}

	if !reflect.DeepEqual(policy, want) {
		t.Errorf("Permissions.GetPolicy returned %+v, want %+v", policy, want)
	}
}

func TestPermissionsService_GetPolicy_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/policies/999", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Policy not found"}`)
	})

	ctx := context.Background()
	_, err := client.Permissions.GetPolicy(ctx, 999)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestPermissionsService_CreatePolicy(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/policies", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/access_control/policies")

		var body CreateAccessControlPolicyRequest
		json.NewDecoder(r.Body).Decode(&body)

		if body.Policy.Name != "New Policy" {
			t.Errorf("Request body name = %v, want 'New Policy'", body.Policy.Name)
		}
		if body.Policy.Description != "A new test policy" {
			t.Errorf("Request body description = %v, want 'A new test policy'", body.Policy.Description)
		}

		fmt.Fprint(w, `{
			"id": 3,
			"account_id": 100,
			"name": "New Policy",
			"description": "A new test policy",
			"user_count": 0
		}`)
	})

	ctx := context.Background()
	policy, err := client.Permissions.CreatePolicy(ctx, "New Policy", "A new test policy")
	if err != nil {
		t.Errorf("Permissions.CreatePolicy returned error: %v", err)
	}

	want := &AccessControlPolicy{
		ID:          3,
		AccountID:   100,
		Name:        "New Policy",
		Description: "A new test policy",
		UserCount:   0,
	}

	if !reflect.DeepEqual(policy, want) {
		t.Errorf("Permissions.CreatePolicy returned %+v, want %+v", policy, want)
	}
}

func TestPermissionsService_UpdatePolicy(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/policies/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PATCH")
		testURL(t, r, "/v3/access_control/policies/1")

		var body UpdateAccessControlPolicyRequest
		json.NewDecoder(r.Body).Decode(&body)

		if body.Policy.Name != "Updated Policy" {
			t.Errorf("Request body name = %v, want 'Updated Policy'", body.Policy.Name)
		}

		fmt.Fprint(w, `{
			"id": 1,
			"account_id": 100,
			"name": "Updated Policy",
			"description": "Updated description",
			"user_count": 3
		}`)
	})

	ctx := context.Background()
	policy, err := client.Permissions.UpdatePolicy(ctx, 1, "Updated Policy", "Updated description")
	if err != nil {
		t.Errorf("Permissions.UpdatePolicy returned error: %v", err)
	}

	want := &AccessControlPolicy{
		ID:          1,
		AccountID:   100,
		Name:        "Updated Policy",
		Description: "Updated description",
		UserCount:   3,
	}

	if !reflect.DeepEqual(policy, want) {
		t.Errorf("Permissions.UpdatePolicy returned %+v, want %+v", policy, want)
	}
}

func TestPermissionsService_DeletePolicy(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/policies/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testURL(t, r, "/v3/access_control/policies/1")

		fmt.Fprint(w, `{
			"id": 1,
			"account_id": 100,
			"name": "Deleted Policy",
			"description": "This policy was deleted"
		}`)
	})

	ctx := context.Background()
	policy, err := client.Permissions.DeletePolicy(ctx, 1)
	if err != nil {
		t.Errorf("Permissions.DeletePolicy returned error: %v", err)
	}

	if policy.ID != 1 {
		t.Errorf("Expected deleted policy ID 1, got %d", policy.ID)
	}
}

func TestPermissionsService_ListPolicyGroups(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/policy_groups", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/access_control/policy_groups")

		fmt.Fprint(w, `[
			{
				"id": 10,
				"account_id": 100,
				"name": "Analytics Team",
				"description": "Group for analytics team",
				"policy_count": 3,
				"created_at": "2021-01-01T00:00:00Z",
				"updated_at": "2021-01-02T00:00:00Z"
			},
			{
				"id": 11,
				"account_id": 100,
				"name": "Restricted Users",
				"description": "Group for restricted access",
				"policy_count": 1,
				"created_at": "2021-01-01T00:00:00Z",
				"updated_at": "2021-01-01T00:00:00Z"
			}
		]`)
	})

	ctx := context.Background()
	groups, err := client.Permissions.ListPolicyGroups(ctx)
	if err != nil {
		t.Errorf("Permissions.ListPolicyGroups returned error: %v", err)
	}

	createdAt, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	updatedAt1, _ := time.Parse(time.RFC3339, "2021-01-02T00:00:00Z")
	updatedAt2, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")

	desc1 := "Group for analytics team"
	desc2 := "Group for restricted access"

	want := []AccessControlPolicyGroup{
		{
			ID:          10,
			AccountID:   100,
			Name:        "Analytics Team",
			Description: &desc1,
			PolicyCount: 3,
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt1,
		},
		{
			ID:          11,
			AccountID:   100,
			Name:        "Restricted Users",
			Description: &desc2,
			PolicyCount: 1,
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt2,
		},
	}

	if !reflect.DeepEqual(groups, want) {
		t.Errorf("Permissions.ListPolicyGroups returned %+v, want %+v", groups, want)
	}
}

func TestPermissionsService_CreatePolicyGroup(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/policy_groups", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/access_control/policy_groups")

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)

		if body["name"] != "New Group" {
			t.Errorf("Request body name = %v, want 'New Group'", body["name"])
		}

		fmt.Fprint(w, `{
			"id": 12,
			"account_id": 100,
			"name": "New Group",
			"description": null,
			"policy_count": 0,
			"created_at": "2021-01-03T00:00:00Z",
			"updated_at": "2021-01-03T00:00:00Z"
		}`)
	})

	ctx := context.Background()
	group, err := client.Permissions.CreatePolicyGroup(ctx, "New Group")
	if err != nil {
		t.Errorf("Permissions.CreatePolicyGroup returned error: %v", err)
	}

	createdAt, _ := time.Parse(time.RFC3339, "2021-01-03T00:00:00Z")
	updatedAt, _ := time.Parse(time.RFC3339, "2021-01-03T00:00:00Z")

	want := &AccessControlPolicyGroup{
		ID:          12,
		AccountID:   100,
		Name:        "New Group",
		Description: nil,
		PolicyCount: 0,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}

	if !reflect.DeepEqual(group, want) {
		t.Errorf("Permissions.CreatePolicyGroup returned %+v, want %+v", group, want)
	}
}

func TestPermissionsService_ListAccessControlUsers(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/users", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/access_control/users")

		fmt.Fprint(w, `[
			{
				"user_id": 1001,
				"account_id": 100,
				"permissions": {},
				"policies": [
					{
						"id": 1,
						"name": "Basic Access"
					}
				]
			},
			{
				"user_id": 1002,
				"account_id": 100,
				"permissions": {},
				"policies": []
			}
		]`)
	})

	ctx := context.Background()
	users, err := client.Permissions.ListAccessControlUsers(ctx)
	if err != nil {
		t.Errorf("Permissions.ListAccessControlUsers returned error: %v", err)
	}

	want := []AccessControlUser{
		{
			UserID:      1001,
			AccountID:   100,
			Permissions: AccessControlPermissions{},
			Policies: []AccessControlPolicy{
				{ID: 1, Name: "Basic Access"},
			},
		},
		{
			UserID:      1002,
			AccountID:   100,
			Permissions: AccessControlPermissions{},
			Policies:    []AccessControlPolicy{},
		},
	}

	if !reflect.DeepEqual(users, want) {
		t.Errorf("Permissions.ListAccessControlUsers returned %+v, want %+v", users, want)
	}
}

func TestPermissionsService_AttachUserToPolicy(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/users/1001/policies/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/access_control/users/1001/policies/1")

		fmt.Fprint(w, `{
			"id": 1,
			"account_id": 100,
			"name": "Analytics Policy",
			"description": "Policy attached to user",
			"user_count": 6
		}`)
	})

	ctx := context.Background()
	policy, err := client.Permissions.AttachUserToPolicy(ctx, 1001, 1)
	if err != nil {
		t.Errorf("Permissions.AttachUserToPolicy returned error: %v", err)
	}

	want := &AccessControlPolicy{
		ID:          1,
		AccountID:   100,
		Name:        "Analytics Policy",
		Description: "Policy attached to user",
		UserCount:   6,
	}

	if !reflect.DeepEqual(policy, want) {
		t.Errorf("Permissions.AttachUserToPolicy returned %+v, want %+v", policy, want)
	}
}

func TestPermissionsService_DetachUserFromPolicy(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/access_control/users/1001/policies/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testURL(t, r, "/v3/access_control/users/1001/policies/1")

		fmt.Fprint(w, `{
			"id": 1,
			"account_id": 100,
			"name": "Analytics Policy",
			"description": "Policy detached from user",
			"user_count": 4
		}`)
	})

	ctx := context.Background()
	policy, err := client.Permissions.DetachUserFromPolicy(ctx, 1001, 1)
	if err != nil {
		t.Errorf("Permissions.DetachUserFromPolicy returned error: %v", err)
	}

	want := &AccessControlPolicy{
		ID:          1,
		AccountID:   100,
		Name:        "Analytics Policy",
		Description: "Policy detached from user",
		UserCount:   4,
	}

	if !reflect.DeepEqual(policy, want) {
		t.Errorf("Permissions.DetachUserFromPolicy returned %+v, want %+v", policy, want)
	}
}

// Example tests demonstrating common permission operations

func ExamplePermissionsService_ListPolicies() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// List all access control policies
	policies, err := client.Permissions.ListPolicies(ctx, nil)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, policy := range policies {
		fmt.Printf("Policy: %s (ID: %d) - %d users\n", policy.Name, policy.ID, policy.UserCount)
	}
}

func ExamplePermissionsService_CreatePolicy() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Create a new access control policy
	policy, err := client.Permissions.CreatePolicy(ctx, "Data Scientists", "Policy for data science team")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Created policy: %s (ID: %d)\n", policy.Name, policy.ID)
}

func ExamplePermissionsService_AttachUserToPolicy() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Attach a user to a policy
	policy, err := client.Permissions.AttachUserToPolicy(ctx, 1001, 5)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("User attached to policy: %s\n", policy.Name)
}
