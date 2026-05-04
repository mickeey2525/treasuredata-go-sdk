package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleWorkflowAttemptList lists workflow attempts
func HandleWorkflowAttemptList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Workflow ID required")
	}

	opts := &td.WorkflowAttemptListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.Workflow.ListWorkflowAttempts(ctx, args[0], opts)
	if err != nil {
		return wrapError(err, "failed to list workflow attempts", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		return PrintJSON(resp)
	case "csv":
		fmt.Println("id,index,status,created_at,finished_at")
		for _, attempt := range resp.Attempts {
			finishedAt := ""
			if attempt.FinishedAt != nil {
				finishedAt = attempt.FinishedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Printf("%s,%d,%s,%s,%s\n",
				attempt.ID, attempt.Index, attempt.Status,
				attempt.CreatedAt.Format("2006-01-02 15:04:05"),
				finishedAt)
		}
	default:
		if len(resp.Attempts) == 0 {
			fmt.Println("No attempts found")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tINDEX\tSTATUS\tCREATED\tFINISHED")
		for _, attempt := range resp.Attempts {
			finishedAt := "-"
			if attempt.FinishedAt != nil {
				finishedAt = attempt.FinishedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\n",
				attempt.ID, attempt.Index, attempt.Status,
				attempt.CreatedAt.Format("2006-01-02 15:04:05"),
				finishedAt)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d attempts\n", len(resp.Attempts))
	}
	return nil
}

// HandleWorkflowAttemptGet retrieves a workflow attempt
func HandleWorkflowAttemptGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Workflow ID and attempt ID required")
	}

	attempt, err := client.Workflow.GetWorkflowAttempt(ctx, args[0], args[1])
	if err != nil {
		return wrapError(err, "failed to get workflow attempt", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		return PrintJSON(attempt)
	case "csv":
		fmt.Println("id,index,status,created_at,finished_at,done,success")
		finishedAt := ""
		if attempt.FinishedAt != nil {
			finishedAt = attempt.FinishedAt.Format("2006-01-02 15:04:05")
		}
		success := ""
		if attempt.Success != nil {
			success = fmt.Sprintf("%t", *attempt.Success)
		}
		fmt.Printf("%s,%d,%s,%s,%s,%t,%s\n",
			attempt.ID, attempt.Index, attempt.Status,
			attempt.CreatedAt.Format("2006-01-02 15:04:05"),
			finishedAt, attempt.Done, success)
	default:
		fmt.Printf("ID: %s\n", attempt.ID)
		fmt.Printf("Index: %d\n", attempt.Index)
		fmt.Printf("Workflow ID: %s\n", attempt.WorkflowID)
		fmt.Printf("Status: %s\n", attempt.Status)
		fmt.Printf("Created: %s\n", attempt.CreatedAt.Format("2006-01-02 15:04:05"))
		if attempt.FinishedAt != nil {
			fmt.Printf("Finished: %s\n", attempt.FinishedAt.Format("2006-01-02 15:04:05"))
		}
		fmt.Printf("Done: %t\n", attempt.Done)
		if attempt.Success != nil {
			fmt.Printf("Success: %t\n", *attempt.Success)
		}
		if attempt.SessionID != nil {
			fmt.Printf("Session ID: %s\n", *attempt.SessionID)
		}
		if len(attempt.Params) > 0 {
			fmt.Printf("\nParameters:\n")
			paramsJSON, _ := json.MarshalIndent(attempt.Params, "  ", "  ")
			fmt.Printf("  %s\n", paramsJSON)
		}
	}
	return nil
}

// HandleWorkflowAttemptKill kills a workflow attempt
func HandleWorkflowAttemptKill(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Workflow ID and attempt ID required")
	}

	if err := client.Workflow.KillWorkflowAttempt(ctx, args[0], args[1]); err != nil {
		return wrapError(err, "failed to kill workflow attempt", flags.Verbose)
	}

	fmt.Printf("Workflow attempt %s killed successfully\n", args[1])
	return nil
}

// HandleWorkflowAttemptRetry retries a workflow attempt
func HandleWorkflowAttemptRetry(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Workflow ID and attempt ID required")
	}

	var params map[string]interface{}
	if len(args) > 2 {
		if err := json.Unmarshal([]byte(args[2]), &params); err != nil {
			return usageError(fmt.Sprintf("Invalid parameters JSON: %v", err))
		}
	}

	attempt, err := client.Workflow.RetryWorkflowAttempt(ctx, args[0], args[1], params)
	if err != nil {
		return wrapError(err, "failed to retry workflow attempt", flags.Verbose)
	}

	fmt.Printf("Workflow attempt retried successfully\n")
	fmt.Printf("New Attempt ID: %s\n", attempt.ID)
	fmt.Printf("Status: %s\n", attempt.Status)
	return nil
}
