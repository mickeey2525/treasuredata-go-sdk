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

func TestWorkflowService_GetWorkflowSchedule(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/schedule", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1/schedule")

		fmt.Fprint(w, `{
			"id": "10",
			"workflow_id": "1",
			"cron": "0 * * * *",
			"timezone": "UTC",
			"delay": 0,
			"next_time": 1609462800,
			"next_schedule_time": 1609462800,
			"created_at": 1609459200,
			"updated_at": 1609459200
		}`)
	})

	ctx := context.Background()
	schedule, err := client.Workflow.GetWorkflowSchedule(ctx, "1")
	if err != nil {
		t.Errorf("Workflows.GetWorkflowSchedule returned error: %v", err)
	}

	nextTime := TDTime{time.Unix(1609462800, 0)}
	nextScheduleTime := TDTime{time.Unix(1609462800, 0)}
	want := &WorkflowSchedule{
		ID:               "10",
		WorkflowID:       "1",
		Cron:             "0 * * * *",
		Timezone:         "UTC",
		Delay:            0,
		NextTime:         &nextTime,
		NextScheduleTime: &nextScheduleTime,
		CreatedAt:        TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:        TDTime{time.Unix(1609459200, 0)},
	}

	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("Workflows.GetWorkflowSchedule returned %+v, want %+v", schedule, want)
	}
}

func TestWorkflowService_EnableWorkflowSchedule(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/schedule/enable", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/api/workflows/1/schedule/enable")

		fmt.Fprint(w, `{
			"id": "10",
			"workflow_id": "1",
			"cron": "0 * * * *",
			"timezone": "UTC",
			"delay": 0,
			"created_at": 1609459200,
			"updated_at": 1609545600
		}`)
	})

	ctx := context.Background()
	schedule, err := client.Workflow.EnableWorkflowSchedule(ctx, "1")
	if err != nil {
		t.Errorf("Workflows.EnableWorkflowSchedule returned error: %v", err)
	}

	want := &WorkflowSchedule{
		ID:         "10",
		WorkflowID: "1",
		Cron:       "0 * * * *",
		Timezone:   "UTC",
		Delay:      0,
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609545600, 0)},
	}

	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("Workflows.EnableWorkflowSchedule returned %+v, want %+v", schedule, want)
	}
}

func TestWorkflowService_DisableWorkflowSchedule(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/schedule/disable", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/api/workflows/1/schedule/disable")

		fmt.Fprint(w, `{
			"id": "10",
			"workflow_id": "1",
			"cron": "0 * * * *",
			"timezone": "UTC",
			"delay": 0,
			"disabled_at": 1609545600,
			"created_at": 1609459200,
			"updated_at": 1609545600
		}`)
	})

	ctx := context.Background()
	schedule, err := client.Workflow.DisableWorkflowSchedule(ctx, "1")
	if err != nil {
		t.Errorf("Workflows.DisableWorkflowSchedule returned error: %v", err)
	}

	disabledAt := TDTime{time.Unix(1609545600, 0)}
	want := &WorkflowSchedule{
		ID:         "10",
		WorkflowID: "1",
		Cron:       "0 * * * *",
		Timezone:   "UTC",
		Delay:      0,
		DisabledAt: &disabledAt,
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609545600, 0)},
	}

	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("Workflows.DisableWorkflowSchedule returned %+v, want %+v", schedule, want)
	}
}

func TestWorkflowService_UpdateWorkflowSchedule(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/schedule", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/api/workflows/1/schedule")

		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)

		if body["cron"] != "30 * * * *" {
			t.Errorf("Request body cron = %v, want %v", body["cron"], "30 * * * *")
		}
		if body["timezone"] != "America/New_York" {
			t.Errorf("Request body timezone = %v, want %v", body["timezone"], "America/New_York")
		}
		if body["delay"] != float64(300) { // JSON numbers are float64
			t.Errorf("Request body delay = %v, want %v", body["delay"], 300)
		}

		fmt.Fprint(w, `{
			"id": "10",
			"workflow_id": "1",
			"cron": "30 * * * *",
			"timezone": "America/New_York",
			"delay": 300,
			"created_at": 1609459200,
			"updated_at": 1609545600
		}`)
	})

	ctx := context.Background()
	schedule, err := client.Workflow.UpdateWorkflowSchedule(ctx, "1", "30 * * * *", "America/New_York", 300)
	if err != nil {
		t.Errorf("Workflows.UpdateWorkflowSchedule returned error: %v", err)
	}

	want := &WorkflowSchedule{
		ID:         "10",
		WorkflowID: "1",
		Cron:       "30 * * * *",
		Timezone:   "America/New_York",
		Delay:      300,
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609545600, 0)},
	}

	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("Workflows.UpdateWorkflowSchedule returned %+v, want %+v", schedule, want)
	}
}

func ExampleWorkflowService_UpdateWorkflowSchedule() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Update workflow schedule to run daily at 2 AM UTC
	schedule, err := client.Workflow.UpdateWorkflowSchedule(ctx, "123", "0 2 * * *", "UTC", 0)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Updated schedule: %s (Next run: %v)\n", schedule.Cron, schedule.NextTime)
}

func TestValidateCronExpression(t *testing.T) {
	testCases := []struct {
		name    string
		cron    string
		wantErr bool
	}{
		{"valid standard cron", "0 * * * *", false},
		{"valid standard cron with range", "0-59 * * * *", false},
		{"valid standard cron with list", "0,15,30,45 * * * *", false},
		{"valid standard cron with step", "*/15 * * * *", false},
		{"valid extended cron with seconds", "0 0 * * * *", false},
		{"valid special string @daily", "@daily", false},
		{"valid special string @hourly", "@hourly", false},
		{"valid special string @monthly", "@monthly", false},
		{"invalid number of fields (too few)", "* * * *", true},
		{"invalid number of fields (too many)", "* * * * * * *", true},
		{"invalid character", "a * * * *", true},
		{"empty field", "* *  * *", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCronExpression(tc.cron)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateCronExpression() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
