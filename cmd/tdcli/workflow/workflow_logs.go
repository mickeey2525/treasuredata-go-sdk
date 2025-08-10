package workflow

import (
	"context"
	"fmt"
	"log"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Workflow log handlers
func HandleWorkflowAttemptLog(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	logContent, err := client.Workflow.GetWorkflowAttemptLog(ctx, workflowID, attemptID)
	if err != nil {
		HandleError(err, "Failed to get workflow attempt log", flags.Verbose)
	}

	fmt.Print(logContent)
}

func HandleWorkflowTaskLog(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Workflow ID, attempt ID, and task ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	logContent, err := client.Workflow.GetWorkflowTaskLog(ctx, workflowID, attemptID, args[2])
	if err != nil {
		HandleError(err, "Failed to get workflow task log", flags.Verbose)
	}

	fmt.Print(logContent)
}
