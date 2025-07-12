package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func TestPrintAccessControlUsersTable(t *testing.T) {
	tests := []struct {
		name           string
		users          []td.AccessControlUser
		userDetailsMap map[int]td.User
		expectedOutput []string
	}{
		{
			name: "Users with details",
			users: []td.AccessControlUser{
				{UserID: 1001, AccountID: 1, Policies: []td.AccessControlPolicy{{ID: 1}, {ID: 2}}},
				{UserID: 1002, AccountID: 1, Policies: []td.AccessControlPolicy{{ID: 3}}},
			},
			userDetailsMap: map[int]td.User{
				1001: {ID: 1001, Email: "john@example.com", Name: "John Doe"},
				1002: {ID: 1002, Email: "jane@example.com", Name: "Jane Smith"},
			},
			expectedOutput: []string{
				"USER_ID", "EMAIL", "NAME", "ACCOUNT_ID", "POLICIES",
				"1001", "john@example.com", "John Doe", "1", "2",
				"1002", "jane@example.com", "Jane Smith", "1", "1",
			},
		},
		{
			name: "Users without details",
			users: []td.AccessControlUser{
				{UserID: 2001, AccountID: 2, Policies: []td.AccessControlPolicy{}},
				{UserID: 2002, AccountID: 2, Policies: []td.AccessControlPolicy{{ID: 4}}},
			},
			userDetailsMap: map[int]td.User{},
			expectedOutput: []string{
				"USER_ID", "EMAIL", "NAME", "ACCOUNT_ID", "POLICIES",
				"2001", "", "", "2", "0",
				"2002", "", "", "2", "1",
			},
		},
		{
			name: "Mixed - some users with details, some without",
			users: []td.AccessControlUser{
				{UserID: 3001, AccountID: 3, Policies: []td.AccessControlPolicy{{ID: 5}}},
				{UserID: 3002, AccountID: 3, Policies: []td.AccessControlPolicy{}},
			},
			userDetailsMap: map[int]td.User{
				3001: {ID: 3001, Email: "test@example.com", Name: "Test User"},
			},
			expectedOutput: []string{
				"USER_ID", "EMAIL", "NAME", "ACCOUNT_ID", "POLICIES",
				"3001", "test@example.com", "Test User", "3", "1",
				"3002", "", "", "3", "0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Call the function
			printAccessControlUsersTable(tt.users, tt.userDetailsMap)

			// Restore stdout and read output
			w.Close()
			os.Stdout = oldStdout
			output, _ := io.ReadAll(r)
			outputStr := string(output)

			// Verify all expected strings are in the output
			for _, expected := range tt.expectedOutput {
				if !strings.Contains(outputStr, expected) {
					t.Errorf("Expected output to contain %q, but got:\n%s", expected, outputStr)
				}
			}
		})
	}
}

func TestPrintAccessControlUsersCSV(t *testing.T) {
	tests := []struct {
		name           string
		users          []td.AccessControlUser
		userDetailsMap map[int]td.User
		expectedLines  []string
	}{
		{
			name: "CSV format with user details",
			users: []td.AccessControlUser{
				{UserID: 1001, AccountID: 1, Policies: []td.AccessControlPolicy{{ID: 1}, {ID: 2}}},
				{UserID: 1002, AccountID: 1, Policies: []td.AccessControlPolicy{}},
			},
			userDetailsMap: map[int]td.User{
				1001: {ID: 1001, Email: "john@example.com", Name: "John Doe"},
				1002: {ID: 1002, Email: "jane@example.com", Name: "Jane Smith"},
			},
			expectedLines: []string{
				"user_id,email,name,account_id,policy_count",
				"1001,john@example.com,John Doe,1,2",
				"1002,jane@example.com,Jane Smith,1,0",
			},
		},
		{
			name: "CSV format without user details",
			users: []td.AccessControlUser{
				{UserID: 2001, AccountID: 2, Policies: []td.AccessControlPolicy{{ID: 3}}},
			},
			userDetailsMap: map[int]td.User{},
			expectedLines: []string{
				"user_id,email,name,account_id,policy_count",
				"2001,,,2,1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Call the function
			printAccessControlUsersCSV(tt.users, tt.userDetailsMap)

			// Restore stdout and read output
			w.Close()
			os.Stdout = oldStdout
			output, _ := io.ReadAll(r)
			outputStr := strings.TrimSpace(string(output))

			// Split output into lines
			outputLines := strings.Split(outputStr, "\n")

			// Verify we have the expected number of lines
			if len(outputLines) != len(tt.expectedLines) {
				t.Fatalf("Expected %d lines, got %d\nOutput:\n%s", len(tt.expectedLines), len(outputLines), outputStr)
			}

			// Verify each line matches
			for i, expectedLine := range tt.expectedLines {
				if outputLines[i] != expectedLine {
					t.Errorf("Line %d mismatch:\nExpected: %s\nGot:      %s", i+1, expectedLine, outputLines[i])
				}
			}
		})
	}
}

// MockClient is a mock implementation for testing
type MockClient struct {
	Permissions *MockPermissionsService
	Users       *MockUsersService
}

type MockPermissionsService struct {
	ListAccessControlUsersFunc func(ctx context.Context) ([]td.AccessControlUser, error)
}

func (m *MockPermissionsService) ListAccessControlUsers(ctx context.Context) ([]td.AccessControlUser, error) {
	if m.ListAccessControlUsersFunc != nil {
		return m.ListAccessControlUsersFunc(ctx)
	}
	return nil, nil
}

type MockUsersService struct {
	ListFunc func(ctx context.Context) ([]td.User, error)
}

func (m *MockUsersService) List(ctx context.Context) ([]td.User, error) {
	if m.ListFunc != nil {
		return m.ListFunc(ctx)
	}
	return nil, nil
}

func TestHandleAccessControlUserList(t *testing.T) {
	// This test demonstrates how the integration would work
	// In a real scenario, you would need to refactor handleAccessControlUserList
	// to accept interfaces instead of concrete types for better testability

	t.Run("Integration test example", func(t *testing.T) {
		// Expected behavior:
		// 1. Calls client.Permissions.ListAccessControlUsers()
		// 2. Calls client.Users.List() to get user details
		// 3. Maps user IDs to user details
		// 4. Prints the combined information

		// Create test data
		accessControlUsers := []td.AccessControlUser{
			{UserID: 1001, AccountID: 1, Policies: []td.AccessControlPolicy{{ID: 1}}},
			{UserID: 1002, AccountID: 1, Policies: []td.AccessControlPolicy{{ID: 2}, {ID: 3}}},
		}

		allUsers := []td.User{
			{ID: 1001, Email: "user1@example.com", Name: "User One"},
			{ID: 1002, Email: "user2@example.com", Name: "User Two"},
			{ID: 1003, Email: "user3@example.com", Name: "User Three"}, // Extra user not in access control
		}

		// Verify the output would contain both user IDs and their details
		userDetailsMap := make(map[int]td.User)
		for _, user := range allUsers {
			userDetailsMap[user.ID] = user
		}

		// Capture stdout
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		// Test table output
		printAccessControlUsersTable(accessControlUsers, userDetailsMap)

		// Restore stdout and read output
		w.Close()
		os.Stdout = oldStdout
		output, _ := io.ReadAll(r)
		outputStr := string(output)

		// Verify output contains user details
		if !strings.Contains(outputStr, "user1@example.com") {
			t.Error("Expected output to contain user1@example.com")
		}
		if !strings.Contains(outputStr, "User One") {
			t.Error("Expected output to contain User One")
		}
		if !strings.Contains(outputStr, "user2@example.com") {
			t.Error("Expected output to contain user2@example.com")
		}
		if !strings.Contains(outputStr, "User Two") {
			t.Error("Expected output to contain User Two")
		}
	})
}

func TestPrintAccessControlUsersJSON(t *testing.T) {
	tests := []struct {
		name           string
		users          []td.AccessControlUser
		userDetailsMap map[int]td.User
		expectedFields []string
	}{
		{
			name: "JSON format with user details",
			users: []td.AccessControlUser{
				{
					UserID:      1001,
					AccountID:   1,
					Policies:    []td.AccessControlPolicy{{ID: 1}, {ID: 2}},
					Permissions: td.AccessControlPermissions{},
				},
			},
			userDetailsMap: map[int]td.User{
				1001: {ID: 1001, Email: "john@example.com", Name: "John Doe"},
			},
			expectedFields: []string{
				`"user_id": 1001`,
				`"account_id": 1`,
				`"email": "john@example.com"`,
				`"name": "John Doe"`,
				`"policies"`,
			},
		},
		{
			name: "JSON format without matching user details",
			users: []td.AccessControlUser{
				{
					UserID:      2001,
					AccountID:   2,
					Policies:    []td.AccessControlPolicy{},
					Permissions: td.AccessControlPermissions{},
				},
			},
			userDetailsMap: map[int]td.User{
				9999: {ID: 9999, Email: "other@example.com", Name: "Other User"},
			},
			expectedFields: []string{
				`"user_id": 2001`,
				`"account_id": 2`,
				`"permissions"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Call the function
			printAccessControlUsersJSON(tt.users, tt.userDetailsMap)

			// Restore stdout and read output
			w.Close()
			os.Stdout = oldStdout
			output, _ := io.ReadAll(r)
			outputStr := string(output)

			// Verify expected fields are in the output
			for _, expected := range tt.expectedFields {
				if !strings.Contains(outputStr, expected) {
					t.Errorf("Expected output to contain %q, but got:\n%s", expected, outputStr)
				}
			}
		})
	}
}

func TestPrintAccessControlUsersTableWithoutDetails(t *testing.T) {
	users := []td.AccessControlUser{
		{UserID: 1001, AccountID: 1, Policies: []td.AccessControlPolicy{{ID: 1}}},
		{UserID: 1002, AccountID: 1, Policies: []td.AccessControlPolicy{}},
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function with nil userDetailsMap
	printAccessControlUsersTable(users, nil)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify the basic format is used (without EMAIL and NAME columns)
	expectedStrings := []string{
		"USER_ID", "ACCOUNT_ID", "POLICIES",
		"1001", "1", "1",
		"1002", "1", "0",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(outputStr, expected) {
			t.Errorf("Expected output to contain %q, but got:\n%s", expected, outputStr)
		}
	}

	// Verify EMAIL and NAME headers are NOT present
	if strings.Contains(outputStr, "EMAIL") || strings.Contains(outputStr, "NAME") {
		t.Errorf("Expected output to NOT contain EMAIL or NAME headers, but got:\n%s", outputStr)
	}
}

func TestPrintAccessControlUsersCSVWithoutDetails(t *testing.T) {
	users := []td.AccessControlUser{
		{UserID: 1001, AccountID: 1, Policies: []td.AccessControlPolicy{{ID: 1}}},
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function with nil userDetailsMap
	printAccessControlUsersCSV(users, nil)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := strings.TrimSpace(string(output))

	expectedLines := []string{
		"user_id,account_id,policy_count",
		"1001,1,1",
	}

	outputLines := strings.Split(outputStr, "\n")
	if len(outputLines) != len(expectedLines) {
		t.Fatalf("Expected %d lines, got %d\nOutput:\n%s", len(expectedLines), len(outputLines), outputStr)
	}

	for i, expectedLine := range expectedLines {
		if outputLines[i] != expectedLine {
			t.Errorf("Line %d mismatch:\nExpected: %s\nGot:      %s", i+1, expectedLine, outputLines[i])
		}
	}
}
