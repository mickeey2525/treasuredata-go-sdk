package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)


func TestCDPJourneyAttributes_TimestampParsing(t *testing.T) {
	// Test data with camelCase timestamp fields from real API response
	jsonData := `{
		"name": "Test Journey",
		"audienceId": "499270",
		"state": "draft",
		"createdAt": "2025-01-10T17:05:37.259Z",
		"updatedAt": "2025-01-10T17:05:37.259Z",
		"launchedAt": null,
		"paused": false,
		"pausedAt": null,
		"allowReentry": false,
		"reentryMode": "reentry_unless_goal_achieved",
		"journeyBundleId": "42449",
		"journeyBundleName": "Test Journey Bundle",
		"versionNumber": 1
	}`

	var attrs CDPJourneyAttributes
	err := json.Unmarshal([]byte(jsonData), &attrs)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// Verify timestamps are parsed correctly
	expectedCreatedAt := time.Date(2025, 1, 10, 17, 5, 37, 259000000, time.UTC)
	expectedUpdatedAt := time.Date(2025, 1, 10, 17, 5, 37, 259000000, time.UTC)

	if !attrs.CreatedAt.Equal(expectedCreatedAt) {
		t.Errorf("CreatedAt parsing failed. Got: %v, Want: %v", attrs.CreatedAt, expectedCreatedAt)
	}

	if !attrs.UpdatedAt.Equal(expectedUpdatedAt) {
		t.Errorf("UpdatedAt parsing failed. Got: %v, Want: %v", attrs.UpdatedAt, expectedUpdatedAt)
	}

	// Verify other fields
	if attrs.Name != "Test Journey" {
		t.Errorf("Name parsing failed. Got: %s, Want: %s", attrs.Name, "Test Journey")
	}

	if attrs.AudienceID != "499270" {
		t.Errorf("AudienceID parsing failed. Got: %s, Want: %s", attrs.AudienceID, "499270")
	}

	if attrs.State != "draft" {
		t.Errorf("State parsing failed. Got: %s, Want: %s", attrs.State, "draft")
	}

	if attrs.JourneyBundleID != "42449" {
		t.Errorf("JourneyBundleID parsing failed. Got: %s, Want: %s", attrs.JourneyBundleID, "42449")
	}

	// Verify nullable fields
	if attrs.LaunchedAt != nil {
		t.Errorf("LaunchedAt should be nil. Got: %v", attrs.LaunchedAt)
	}

	if attrs.PausedAt != nil {
		t.Errorf("PausedAt should be nil. Got: %v", attrs.PausedAt)
	}
}

func TestCDPJourneyAttributes_MarshallTimestamps(t *testing.T) {
	// Create journey attributes with known timestamps
	createdAt := time.Date(2025, 1, 10, 17, 5, 37, 259000000, time.UTC)
	updatedAt := time.Date(2025, 1, 10, 18, 30, 45, 123000000, time.UTC)

	attrs := CDPJourneyAttributes{
		Name:         "Test Journey",
		AudienceID:   "499270",
		State:        "draft",
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
		Paused:       false,
		AllowReentry: true,
		ReentryMode:  "reentry_unless_goal_achieved",
	}

	data, err := json.Marshal(attrs)
	if err != nil {
		t.Fatalf("Failed to marshal attributes: %v", err)
	}

	// Parse it back
	var parsed CDPJourneyAttributes
	err = json.Unmarshal(data, &parsed)
	if err != nil {
		t.Fatalf("Failed to unmarshal marshalled data: %v", err)
	}

	// Verify round-trip preservation
	if !parsed.CreatedAt.Equal(createdAt) {
		t.Errorf("CreatedAt round-trip failed. Got: %v, Want: %v", parsed.CreatedAt, createdAt)
	}

	if !parsed.UpdatedAt.Equal(updatedAt) {
		t.Errorf("UpdatedAt round-trip failed. Got: %v, Want: %v", parsed.UpdatedAt, updatedAt)
	}

	// Verify JSON contains camelCase field names
	jsonStr := string(data)
	if !contains(jsonStr, `"createdAt"`) {
		t.Errorf("JSON should contain camelCase 'createdAt', got: %s", jsonStr)
	}

	if !contains(jsonStr, `"updatedAt"`) {
		t.Errorf("JSON should contain camelCase 'updatedAt', got: %s", jsonStr)
	}

	if !contains(jsonStr, `"audienceId"`) {
		t.Errorf("JSON should contain camelCase 'audienceId', got: %s", jsonStr)
	}
}

func TestCDPService_GetJourney(t *testing.T) {
	client, mux, teardown := setupCDP()
	defer teardown()

	journeyID := "114485"

	mux.HandleFunc("/entities/journeys/"+journeyID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/entities/journeys/"+journeyID)

		// Real API response structure
		fmt.Fprint(w, `{
			"data": {
				"id": "114485",
				"type": "journey",
				"attributes": {
					"name": "Test Journey",
					"audienceId": "499270",
					"description": "A test journey",
					"state": "draft",
					"createdAt": "2025-01-10T17:05:37.259Z",
					"updatedAt": "2025-01-10T17:05:37.259Z",
					"launchedAt": null,
					"paused": false,
					"pausedAt": null,
					"allowReentry": false,
					"reentryMode": "reentry_unless_goal_achieved",
					"journeyBundleId": "42449",
					"journeyBundleName": "Test Journey Bundle",
					"versionNumber": 1,
					"journeyStages": [
						{
							"id": "135421",
							"name": "Stage 1",
							"description": "First stage",
							"position": 0,
							"is_goal": false
						}
					]
				},
				"relationships": {
					"createdBy": {
						"data": {
							"id": "3616",
							"type": "user"
						}
					}
				}
			},
			"included": [
				{
					"id": "3616",
					"type": "user",
					"attributes": {
						"tdUserId": "31912",
						"name": "Test User"
					}
				}
			]
		}`)
	})

	ctx := context.Background()
	resp, err := client.CDP.GetJourney(ctx, journeyID)
	if err != nil {
		t.Errorf("CDP.GetJourney returned error: %v", err)
	}

	// Verify response structure
	if resp.Data.ID != journeyID {
		t.Errorf("Journey ID = %s, want %s", resp.Data.ID, journeyID)
	}

	if resp.Data.Type != "journey" {
		t.Errorf("Journey Type = %s, want %s", resp.Data.Type, "journey")
	}

	// Verify attributes
	attrs := resp.Data.Attributes
	if attrs.Name != "Test Journey" {
		t.Errorf("Journey Name = %s, want %s", attrs.Name, "Test Journey")
	}

	if attrs.AudienceID != "499270" {
		t.Errorf("Journey AudienceID = %s, want %s", attrs.AudienceID, "499270")
	}

	if attrs.State != "draft" {
		t.Errorf("Journey State = %s, want %s", attrs.State, "draft")
	}

	// Verify timestamps
	expectedTime := time.Date(2025, 1, 10, 17, 5, 37, 259000000, time.UTC)
	if !attrs.CreatedAt.Equal(expectedTime) {
		t.Errorf("Journey CreatedAt = %v, want %v", attrs.CreatedAt, expectedTime)
	}

	if !attrs.UpdatedAt.Equal(expectedTime) {
		t.Errorf("Journey UpdatedAt = %v, want %v", attrs.UpdatedAt, expectedTime)
	}

	// Verify journey stages
	if len(attrs.JourneyStages) != 1 {
		t.Errorf("Journey stages count = %d, want %d", len(attrs.JourneyStages), 1)
	}

	stage := attrs.JourneyStages[0]
	if stage.ID != "135421" {
		t.Errorf("Journey stage ID = %s, want %s", stage.ID, "135421")
	}

	if stage.Name != "Stage 1" {
		t.Errorf("Journey stage Name = %s, want %s", stage.Name, "Stage 1")
	}

	// Verify included data
	if len(resp.Included) != 1 {
		t.Errorf("Included data count = %d, want %d", len(resp.Included), 1)
	}
}

func TestCDPService_ListJourneys(t *testing.T) {
	client, mux, teardown := setupCDP()
	defer teardown()

	folderID := "776082"

	mux.HandleFunc("/entities/journeys", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")

		// Check query parameters
		if got := r.URL.Query().Get("folder_id"); got != folderID {
			t.Errorf("folder_id parameter = %s, want %s", got, folderID)
		}

		fmt.Fprint(w, `{
			"data": [
				{
					"id": "114485",
					"type": "journey",
					"attributes": {
						"name": "Journey 1",
						"audienceId": "499270",
						"state": "draft",
						"createdAt": "2025-01-10T17:05:37.259Z",
						"updatedAt": "2025-01-10T17:05:37.259Z",
						"paused": false,
						"allowReentry": false
					}
				},
				{
					"id": "114486",
					"type": "journey",
					"attributes": {
						"name": "Journey 2",
						"audienceId": "499271",
						"state": "launched",
						"createdAt": "2025-01-11T10:15:22.123Z",
						"updatedAt": "2025-01-11T10:15:22.123Z",
						"paused": true,
						"allowReentry": true
					}
				}
			],
			"meta": {
				"total": 2
			}
		}`)
	})

	ctx := context.Background()
	resp, err := client.CDP.ListJourneys(ctx, folderID)
	if err != nil {
		t.Errorf("CDP.ListJourneys returned error: %v", err)
	}

	// Verify response
	if len(resp.Data) != 2 {
		t.Errorf("Journey count = %d, want %d", len(resp.Data), 2)
	}

	// Verify first journey
	journey1 := resp.Data[0]
	if journey1.ID != "114485" {
		t.Errorf("Journey 1 ID = %s, want %s", journey1.ID, "114485")
	}

	if journey1.Attributes.Name != "Journey 1" {
		t.Errorf("Journey 1 Name = %s, want %s", journey1.Attributes.Name, "Journey 1")
	}

	if journey1.Attributes.State != "draft" {
		t.Errorf("Journey 1 State = %s, want %s", journey1.Attributes.State, "draft")
	}

	// Verify second journey
	journey2 := resp.Data[1]
	if journey2.ID != "114486" {
		t.Errorf("Journey 2 ID = %s, want %s", journey2.ID, "114486")
	}

	if journey2.Attributes.State != "launched" {
		t.Errorf("Journey 2 State = %s, want %s", journey2.Attributes.State, "launched")
	}

	if !journey2.Attributes.Paused {
		t.Error("Journey 2 should be paused")
	}

	if !journey2.Attributes.AllowReentry {
		t.Error("Journey 2 should allow reentry")
	}
}

func TestCDPService_CreateJourney(t *testing.T) {
	client, mux, teardown := setupCDP()
	defer teardown()

	mux.HandleFunc("/entities/journeys", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/entities/journeys")

		// Verify request body structure
		var request CDPJourneyRequest
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			t.Errorf("Failed to decode request body: %v", err)
		}

		if request.Data.Attributes.Name != "New Journey" {
			t.Errorf("Request journey name = %s, want %s", request.Data.Attributes.Name, "New Journey")
		}

		// Return created journey
		fmt.Fprint(w, `{
			"data": {
				"id": "114487",
				"type": "journey",
				"attributes": {
					"name": "New Journey",
					"audienceId": "499270",
					"state": "draft",
					"createdAt": "2025-01-10T18:00:00.000Z",
					"updatedAt": "2025-01-10T18:00:00.000Z",
					"paused": false,
					"allowReentry": false
				}
			}
		}`)
	})

	ctx := context.Background()
	request := &CDPJourneyRequest{
		Data: CDPJourney{
			Type: "journey",
			Attributes: &CDPJourneyAttributes{
				Name:         "New Journey",
				AudienceID:   "499270",
				AllowReentry: false,
			},
		},
	}

	resp, err := client.CDP.CreateJourney(ctx, request)
	if err != nil {
		t.Errorf("CDP.CreateJourney returned error: %v", err)
	}

	if resp.Data.ID != "114487" {
		t.Errorf("Created journey ID = %s, want %s", resp.Data.ID, "114487")
	}

	if resp.Data.Attributes.Name != "New Journey" {
		t.Errorf("Created journey name = %s, want %s", resp.Data.Attributes.Name, "New Journey")
	}
}

func TestCDPService_PauseJourney(t *testing.T) {
	client, mux, teardown := setupCDP()
	defer teardown()

	journeyID := "114485"

	mux.HandleFunc("/entities/journeys/"+journeyID+"/pause", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PATCH")

		fmt.Fprint(w, `{
			"data": {
				"id": "114485",
				"type": "journey",
				"attributes": {
					"name": "Test Journey",
					"state": "launched",
					"paused": true,
					"pausedAt": "2025-01-10T18:30:00.000Z",
					"createdAt": "2025-01-10T17:05:37.259Z",
					"updatedAt": "2025-01-10T18:30:00.000Z"
				}
			}
		}`)
	})

	ctx := context.Background()
	resp, err := client.CDP.PauseJourney(ctx, journeyID)
	if err != nil {
		t.Errorf("CDP.PauseJourney returned error: %v", err)
	}

	if !resp.Data.Attributes.Paused {
		t.Error("Journey should be paused after pause operation")
	}

	if resp.Data.Attributes.PausedAt == nil {
		t.Error("PausedAt should be set after pause operation")
	}

	expectedPausedAt := time.Date(2025, 1, 10, 18, 30, 0, 0, time.UTC)
	if !resp.Data.Attributes.PausedAt.Equal(expectedPausedAt) {
		t.Errorf("PausedAt = %v, want %v", resp.Data.Attributes.PausedAt, expectedPausedAt)
	}
}

func TestCDPJourneyActivationAttr_TimestampParsing(t *testing.T) {
	jsonData := `{
		"name": "Test Activation",
		"journey_stage_id": "stage-123",
		"activation_template_id": "template-456",
		"status": "active",
		"createdAt": "2025-01-10T17:05:37.259Z",
		"updatedAt": "2025-01-10T17:05:37.259Z"
	}`

	var attrs CDPJourneyActivationAttr
	err := json.Unmarshal([]byte(jsonData), &attrs)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	expectedTime := time.Date(2025, 1, 10, 17, 5, 37, 259000000, time.UTC)

	if !attrs.CreatedAt.Equal(expectedTime) {
		t.Errorf("Activation CreatedAt = %v, want %v", attrs.CreatedAt, expectedTime)
	}

	if !attrs.UpdatedAt.Equal(expectedTime) {
		t.Errorf("Activation UpdatedAt = %v, want %v", attrs.UpdatedAt, expectedTime)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			func() bool {
				for i := 0; i <= len(s)-len(substr); i++ {
					if s[i:i+len(substr)] == substr {
						return true
					}
				}
				return false
			}()))
}

// Test error handling
func TestCDPService_GetJourney_NotFound(t *testing.T) {
	client, mux, teardown := setupCDP()
	defer teardown()

	journeyID := "nonexistent"

	mux.HandleFunc("/entities/journeys/"+journeyID, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{
			"errors": [{
				"status": "404",
				"title": "Journey not found",
				"detail": "Journey with ID nonexistent was not found"
			}]
		}`)
	})

	ctx := context.Background()
	_, err := client.CDP.GetJourney(ctx, journeyID)
	if err == nil {
		t.Error("Expected error for non-existent journey, got nil")
	}

	// Check if it's an ErrorResponse
	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code %d, got %d", http.StatusNotFound, tdErr.Response.StatusCode)
		}
	}
}

func TestCDPService_DeleteJourney(t *testing.T) {
	client, mux, teardown := setupCDP()
	defer teardown()

	journeyID := "114485"

	mux.HandleFunc("/entities/journeys/"+journeyID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		w.WriteHeader(http.StatusNoContent)
	})

	ctx := context.Background()
	err := client.CDP.DeleteJourney(ctx, journeyID)
	if err != nil {
		t.Errorf("CDP.DeleteJourney returned unexpected error: %v", err)
	}
}

// Benchmark timestamp parsing
func BenchmarkCDPJourneyAttributes_TimestampParsing(b *testing.B) {
	jsonData := `{
		"name": "Benchmark Journey",
		"audienceId": "499270",
		"state": "draft",
		"createdAt": "2025-01-10T17:05:37.259Z",
		"updatedAt": "2025-01-10T17:05:37.259Z",
		"paused": false,
		"allowReentry": false
	}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var attrs CDPJourneyAttributes
		json.Unmarshal([]byte(jsonData), &attrs)
	}
}
