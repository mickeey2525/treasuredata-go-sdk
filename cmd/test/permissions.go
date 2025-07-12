package main

import (
	"context"
	"fmt"
	"log"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func runPermissionsTests(apiKey string) {
	fmt.Println("=== Access Control and Permissions Tests ===")

	client, err := td.NewClient(apiKey)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Test 1: Policy Management
	fmt.Println("1. Testing Policy Management...")
	testPolicyManagement(ctx, client)

	// Test 2: Policy Groups
	fmt.Println("\n2. Testing Policy Groups...")
	testPolicyGroups(ctx, client)

	// Test 3: Access Control Users
	fmt.Println("\n3. Testing Access Control Users...")
	testAccessControlUsers(ctx, client)

	// Test 4: Policy Permissions
	fmt.Println("\n4. Testing Policy Permissions...")
	testPolicyPermissions(ctx, client)
}

func testPolicyManagement(ctx context.Context, client *td.Client) {
	// List existing policies
	fmt.Println("   📋 Listing existing policies...")
	policies, err := client.Permissions.ListPolicies(ctx, nil)
	if err != nil {
		fmt.Printf("   ❌ Failed to list policies: %v\n", err)
		return
	}
	fmt.Printf("   ✅ Found %d existing policies\n", len(policies))

	// Create a test policy
	fmt.Println("   ➕ Creating test policy...")
	policy, err := client.Permissions.CreatePolicy(ctx, "SDK Test Policy", "Test policy created by Go SDK")
	if err != nil {
		fmt.Printf("   ❌ Failed to create policy: %v\n", err)
		return
	}
	fmt.Printf("   ✅ Created policy: %s (ID: %d)\n", policy.Name, policy.ID)

	// Get the created policy
	fmt.Println("   🔍 Retrieving created policy...")
	retrievedPolicy, err := client.Permissions.GetPolicy(ctx, policy.ID)
	if err != nil {
		fmt.Printf("   ❌ Failed to get policy: %v\n", err)
	} else {
		fmt.Printf("   ✅ Retrieved policy: %s\n", retrievedPolicy.Name)
	}

	// Update the policy
	fmt.Println("   ✏️  Updating policy...")
	updatedPolicy, err := client.Permissions.UpdatePolicy(ctx, policy.ID, "", "Updated test policy description")
	if err != nil {
		fmt.Printf("   ❌ Failed to update policy: %v\n", err)
	} else {
		fmt.Printf("   ✅ Updated policy description: %s\n", updatedPolicy.Description)
	}

	// Clean up - delete the test policy
	fmt.Println("   🗑️  Cleaning up test policy...")
	deletedPolicy, err := client.Permissions.DeletePolicy(ctx, policy.ID)
	if err != nil {
		fmt.Printf("   ⚠️  Failed to delete test policy: %v\n", err)
	} else {
		fmt.Printf("   ✅ Deleted test policy: %s\n", deletedPolicy.Name)
	}
}

func testPolicyGroups(ctx context.Context, client *td.Client) {
	// List existing policy groups
	fmt.Println("   📋 Listing existing policy groups...")
	groups, err := client.Permissions.ListPolicyGroups(ctx)
	if err != nil {
		fmt.Printf("   ❌ Failed to list policy groups: %v\n", err)
		return
	}
	fmt.Printf("   ✅ Found %d existing policy groups\n", len(groups))

	// Create a test policy group
	fmt.Println("   ➕ Creating test policy group...")
	group, err := client.Permissions.CreatePolicyGroup(ctx, "SDK Test Group")
	if err != nil {
		fmt.Printf("   ❌ Failed to create policy group: %v\n", err)
		return
	}
	fmt.Printf("   ✅ Created policy group: %s (ID: %d)\n", group.Name, group.ID)

	// Get the created policy group (use string representation of ID)
	fmt.Println("   🔍 Retrieving created policy group...")
	groupIDStr := fmt.Sprintf("%d", group.ID)
	retrievedGroup, err := client.Permissions.GetPolicyGroup(ctx, groupIDStr)
	if err != nil {
		fmt.Printf("   ❌ Failed to get policy group: %v\n", err)
	} else {
		fmt.Printf("   ✅ Retrieved policy group: %s\n", retrievedGroup.Name)
	}

	// Update the policy group
	fmt.Println("   ✏️  Updating policy group...")
	description := "Updated test policy group description"
	updatedGroup, err := client.Permissions.UpdatePolicyGroup(ctx, groupIDStr, group.Name, &description)
	if err != nil {
		fmt.Printf("   ❌ Failed to update policy group: %v\n", err)
	} else {
		if updatedGroup.Description != nil {
			fmt.Printf("   ✅ Updated policy group description: %s\n", *updatedGroup.Description)
		} else {
			fmt.Printf("   ✅ Updated policy group\n")
		}
	}

	// Test managing policies in group (if we have policies)
	policies, err := client.Permissions.ListPolicies(ctx, nil)
	if err == nil && len(policies) > 0 {
		fmt.Println("   🔗 Testing policy group associations...")

		// Update policy group policies
		policyIDs := []int{policies[0].ID}
		updatedPolicies, err := client.Permissions.UpdatePolicyGroupPolicies(ctx, groupIDStr, policyIDs)
		if err != nil {
			fmt.Printf("   ⚠️  Failed to update group policies: %v\n", err)
		} else {
			fmt.Printf("   ✅ Updated group policies: %d policies\n", len(updatedPolicies.PolicyIDs))

			// List policies in group
			groupPolicies, err := client.Permissions.ListPolicyGroupPolicies(ctx, groupIDStr)
			if err != nil {
				fmt.Printf("   ❌ Failed to list group policies: %v\n", err)
			} else {
				fmt.Printf("   ✅ Group now has %d policies\n", len(groupPolicies.PolicyIDs))
			}

			// Clear policies from group
			emptyPolicies, err := client.Permissions.UpdatePolicyGroupPolicies(ctx, groupIDStr, []int{})
			if err != nil {
				fmt.Printf("   ⚠️  Failed to clear group policies: %v\n", err)
			} else {
				fmt.Printf("   ✅ Cleared group policies: %d policies remaining\n", len(emptyPolicies.PolicyIDs))
			}
		}
	}

	// Clean up - delete the test policy group
	fmt.Println("   🗑️  Cleaning up test policy group...")
	err = client.Permissions.DeletePolicyGroup(ctx, groupIDStr)
	if err != nil {
		fmt.Printf("   ⚠️  Failed to delete test policy group: %v\n", err)
	} else {
		fmt.Printf("   ✅ Deleted test policy group\n")
	}
}

func testAccessControlUsers(ctx context.Context, client *td.Client) {
	// List access control users
	fmt.Println("   📋 Listing access control users...")
	users, err := client.Permissions.ListAccessControlUsers(ctx)
	if err != nil {
		fmt.Printf("   ❌ Failed to list access control users: %v\n", err)
		return
	}
	fmt.Printf("   ✅ Found %d access control users\n", len(users))

	// If we have users, test getting individual user details
	if len(users) > 0 {
		fmt.Println("   🔍 Getting details for first user...")
		user, err := client.Permissions.GetAccessControlUser(ctx, users[0].UserID)
		if err != nil {
			fmt.Printf("   ❌ Failed to get user details: %v\n", err)
		} else {
			fmt.Printf("   ✅ Retrieved user %d with %d policies\n", user.UserID, len(user.Policies))

			// Show some permission information
			if len(user.Permissions.Databases) > 0 {
				fmt.Printf("   📊 User has database permissions: %v\n", user.Permissions.Databases[0].Operation)
			}
		}

		// Test user policies
		fmt.Println("   📋 Listing user policies...")
		userPolicies, err := client.Permissions.ListUserPolicies(ctx, users[0].UserID)
		if err != nil {
			fmt.Printf("   ❌ Failed to list user policies: %v\n", err)
		} else {
			fmt.Printf("   ✅ User has %d policies\n", len(userPolicies))
		}
	}

	// Test policy users (if we have policies)
	policies, err := client.Permissions.ListPolicies(ctx, nil)
	if err == nil && len(policies) > 0 {
		fmt.Println("   👥 Testing policy users...")
		policyUsers, err := client.Permissions.GetPolicyUsers(ctx, policies[0].ID)
		if err != nil {
			fmt.Printf("   ❌ Failed to get policy users: %v\n", err)
		} else {
			fmt.Printf("   ✅ Policy %d has %d users\n", policies[0].ID, len(policyUsers))
			if len(policyUsers) > 0 {
				fmt.Printf("   👤 First user: %s (%s)\n", policyUsers[0].Name, policyUsers[0].Email)
			}
		}
	}
}

func testPolicyPermissions(ctx context.Context, client *td.Client) {
	// Get policies to test permissions
	policies, err := client.Permissions.ListPolicies(ctx, nil)
	if err != nil {
		fmt.Printf("   ❌ Failed to list policies: %v\n", err)
		return
	}

	if len(policies) == 0 {
		fmt.Println("   ⚠️  No policies found for permissions testing")
		return
	}

	// Test getting policy permissions
	fmt.Printf("   📋 Getting permissions for policy %d...\n", policies[0].ID)
	permissions, err := client.Permissions.GetPolicyPermissions(ctx, policies[0].ID)
	if err != nil {
		fmt.Printf("   ❌ Failed to get policy permissions: %v\n", err)
	} else {
		fmt.Printf("   ✅ Retrieved policy permissions\n")

		// Show some permission information
		if len(permissions.Databases) > 0 {
			fmt.Printf("   📊 Database permissions: %d operations\n", len(permissions.Databases))
		}
		if len(permissions.WorkflowProject) > 0 {
			fmt.Printf("   🔧 Workflow permissions: %d operations\n", len(permissions.WorkflowProject))
		}
		if len(permissions.Authentications) > 0 {
			fmt.Printf("   🔐 Authentication permissions: %d operations\n", len(permissions.Authentications))
		}

		// Test updating policy permissions (create a simple permission set)
		fmt.Println("   ✏️  Testing permission updates...")
		testPermissions := &td.AccessControlPermissions{
			Databases: []td.DatabasesPermission{
				{Operation: "query", IDs: "1,2"},
			},
		}

		updatedPermissions, err := client.Permissions.UpdatePolicyPermissions(ctx, policies[0].ID, testPermissions)
		if err != nil {
			fmt.Printf("   ⚠️  Failed to update policy permissions: %v\n", err)
		} else {
			fmt.Printf("   ✅ Updated policy permissions\n")
			if len(updatedPermissions.Databases) > 0 {
				fmt.Printf("   📊 Updated database permissions: %s\n", updatedPermissions.Databases[0].Operation)
			}
		}
	}

	// Test column permissions
	fmt.Printf("   📊 Testing column permissions for policy %d...\n", policies[0].ID)
	columnPermissions, err := client.Permissions.GetColumnPermissions(ctx, policies[0].ID)
	if err != nil {
		fmt.Printf("   ⚠️  Failed to get column permissions: %v\n", err)
	} else {
		fmt.Printf("   ✅ Retrieved %d column permissions\n", len(columnPermissions))

		// Test updating column permissions
		if len(columnPermissions) == 0 {
			fmt.Println("   ➕ Creating test column permissions...")
			testColumnPermissions := []td.AccessControlColumnPermission{
				{
					Tags:   []string{"pii", "sensitive"},
					Except: nil,
				},
			}

			updatedColumnPermissions, err := client.Permissions.UpdateColumnPermissions(ctx, policies[0].ID, testColumnPermissions)
			if err != nil {
				fmt.Printf("   ⚠️  Failed to update column permissions: %v\n", err)
			} else {
				fmt.Printf("   ✅ Updated column permissions: %d items\n", len(updatedColumnPermissions))
			}
		}
	}
}
