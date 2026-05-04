package workflow

import (
	"context"
	"fmt"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleWorkflowAttemptLog gets the log for a workflow attempt.
func HandleWorkflowAttemptLog(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Workflow ID and attempt ID required")
	}

	logContent, err := client.Workflow.GetWorkflowAttemptLog(ctx, args[0], args[1])
	if err != nil {
		return wrapError(err, "failed to get workflow attempt log", flags.Verbose)
	}

	fmt.Print(logContent)
	return nil
}

// HandleWorkflowTaskLog gets the log for a task within a workflow attempt.
func HandleWorkflowTaskLog(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return usageError("Workflow ID, attempt ID, and task ID required")
	}

	logContent, err := client.Workflow.GetWorkflowTaskLog(ctx, args[0], args[1], args[2])
	if err != nil {
		return wrapError(err, "failed to get workflow task log", flags.Verbose)
	}

	fmt.Print(logContent)
	return nil
}
