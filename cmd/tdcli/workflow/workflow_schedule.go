package workflow

import (
	"context"
	"fmt"
	"log"
	"strconv"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Workflow schedule handlers
func HandleWorkflowScheduleGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	schedule, err := client.Workflow.GetWorkflowSchedule(ctx, workflowID)
	if err != nil {
		HandleError(err, "Failed to get workflow schedule", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(schedule)
	case "csv":
		fmt.Println("id,workflow_id,cron,timezone,delay,next_time,disabled_at")
		nextTime := ""
		if schedule.NextTime != nil {
			nextTime = schedule.NextTime.Format("2006-01-02 15:04:05")
		}
		disabledAt := ""
		if schedule.DisabledAt != nil {
			disabledAt = schedule.DisabledAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%s,%s,%s,%s,%d,%s,%s\n",
			schedule.ID, schedule.WorkflowID, schedule.Cron, schedule.Timezone,
			schedule.Delay, nextTime, disabledAt)
	default:
		fmt.Printf("ID: %s\n", schedule.ID)
		fmt.Printf("Workflow ID: %s\n", schedule.WorkflowID)
		fmt.Printf("Cron: %s\n", schedule.Cron)
		fmt.Printf("Timezone: %s\n", schedule.Timezone)
		fmt.Printf("Delay: %d seconds\n", schedule.Delay)
		if schedule.NextTime != nil {
			fmt.Printf("Next Time: %s\n", schedule.NextTime.Format("2006-01-02 15:04:05"))
		}
		if schedule.DisabledAt != nil {
			fmt.Printf("Disabled At: %s\n", schedule.DisabledAt.Format("2006-01-02 15:04:05"))
		} else {
			fmt.Printf("Status: Enabled\n")
		}
	}
}

func HandleWorkflowScheduleEnable(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	schedule, err := client.Workflow.EnableWorkflowSchedule(ctx, workflowID)
	if err != nil {
		HandleError(err, "Failed to enable workflow schedule", flags.Verbose)
	}

	fmt.Printf("Workflow schedule enabled successfully\n")
	if schedule.NextTime != nil {
		fmt.Printf("Next run: %s\n", schedule.NextTime.Format("2006-01-02 15:04:05"))
	}
}

func HandleWorkflowScheduleDisable(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	_, err := client.Workflow.DisableWorkflowSchedule(ctx, workflowID)
	if err != nil {
		HandleError(err, "Failed to disable workflow schedule", flags.Verbose)
	}

	fmt.Printf("Workflow schedule disabled successfully\n")
}

func HandleWorkflowScheduleUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 4 {
		log.Fatal("Workflow ID, cron expression, timezone, and delay required")
	}

	workflowID := args[0]

	delay, err := strconv.Atoi(args[3])
	if err != nil {
		log.Fatalf("Invalid delay: %s", args[3])
	}

	schedule, err := client.Workflow.UpdateWorkflowSchedule(ctx, workflowID, args[1], args[2], delay)
	if err != nil {
		HandleError(err, "Failed to update workflow schedule", flags.Verbose)
	}

	fmt.Printf("Workflow schedule updated successfully\n")
	fmt.Printf("Cron: %s\n", schedule.Cron)
	fmt.Printf("Timezone: %s\n", schedule.Timezone)
	fmt.Printf("Delay: %d seconds\n", schedule.Delay)
}
