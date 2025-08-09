package treasuredata

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// validateCronExpression validates a cron expression
func validateCronExpression(cron string) error {
	// Basic cron expression validation
	// Standard cron format: minute hour day month weekday
	// Extended format with seconds: second minute hour day month weekday
	// Special strings: @yearly, @annually, @monthly, @weekly, @daily, @midnight, @hourly

	// Check for special strings first
	specialStrings := []string{"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"}
	for _, special := range specialStrings {
		if cron == special {
			return nil
		}
	}

	// Split cron expression into fields
	fields := strings.Fields(cron)
	if len(fields) < 5 || len(fields) > 6 {
		return fmt.Errorf("cron expression must have 5 or 6 fields, got %d", len(fields))
	}

	// Basic validation for each field
	for i, field := range fields {
		if field == "" {
			return fmt.Errorf("field %d cannot be empty", i+1)
		}
		// Allow common cron characters
		if !regexp.MustCompile(`^[\*0-9,\-\/]+$`).MatchString(field) {
			return fmt.Errorf("field %d contains invalid characters: %s", i+1, field)
		}
	}

	return nil
}

// WorkflowSchedule represents a workflow schedule
type WorkflowSchedule struct {
	ID               string  `json:"id"`
	WorkflowID       string  `json:"workflow_id"`
	Cron             string  `json:"cron"`
	Timezone         string  `json:"timezone"`
	Delay            int     `json:"delay"`
	NextTime         *TDTime `json:"next_time"`
	NextScheduleTime *TDTime `json:"next_schedule_time"`
	DisabledAt       *TDTime `json:"disabled_at"`
	CreatedAt        TDTime  `json:"created_at"`
	UpdatedAt        TDTime  `json:"updated_at"`
}

// GetWorkflowSchedule retrieves the schedule for a workflow
func (s *WorkflowService) GetWorkflowSchedule(ctx context.Context, workflowID string) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("api/workflows/%s/schedule", workflowID)

	req, err := s.client.NewWorkflowRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var schedule WorkflowSchedule
	_, err = s.client.Do(ctx, req, &schedule)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

// EnableWorkflowSchedule enables the schedule for a workflow
func (s *WorkflowService) EnableWorkflowSchedule(ctx context.Context, workflowID string) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("api/workflows/%s/schedule/enable", workflowID)

	req, err := s.client.NewWorkflowRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var schedule WorkflowSchedule
	_, err = s.client.Do(ctx, req, &schedule)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

// DisableWorkflowSchedule disables the schedule for a workflow
func (s *WorkflowService) DisableWorkflowSchedule(ctx context.Context, workflowID string) (*WorkflowSchedule, error) {
	u := fmt.Sprintf("api/workflows/%s/schedule/disable", workflowID)

	req, err := s.client.NewWorkflowRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var schedule WorkflowSchedule
	_, err = s.client.Do(ctx, req, &schedule)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}

// UpdateWorkflowSchedule updates the schedule for a workflow
func (s *WorkflowService) UpdateWorkflowSchedule(ctx context.Context, workflowID string, cron, timezone string, delay int) (*WorkflowSchedule, error) {
	// Validate input
	if workflowID == "" {
		return nil, NewValidationError("workflowID", workflowID, "cannot be empty")
	}
	if cron == "" {
		return nil, NewValidationError("cron", cron, "cannot be empty")
	}
	if err := validateCronExpression(cron); err != nil {
		return nil, NewValidationError("cron", cron, err.Error())
	}
	if timezone == "" {
		return nil, NewValidationError("timezone", timezone, "cannot be empty")
	}
	if delay < 0 {
		return nil, NewValidationError("delay", delay, "cannot be negative")
	}

	u := fmt.Sprintf("api/workflows/%s/schedule", workflowID)

	body := map[string]interface{}{
		"cron":     cron,
		"timezone": timezone,
		"delay":    delay,
	}

	req, err := s.client.NewWorkflowRequest("PUT", u, body)
	if err != nil {
		return nil, err
	}

	var schedule WorkflowSchedule
	_, err = s.client.Do(ctx, req, &schedule)
	if err != nil {
		return nil, err
	}

	return &schedule, nil
}
