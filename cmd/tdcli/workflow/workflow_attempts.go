package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Workflow attempt handlers
func HandleWorkflowAttemptList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	opts := &td.WorkflowAttemptListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.Workflow.ListWorkflowAttempts(ctx, workflowID, opts)
	if err != nil {
		HandleError(err, "Failed to list workflow attempts", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(resp)
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
			return
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
}

func HandleWorkflowAttemptGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	attempt, err := client.Workflow.GetWorkflowAttempt(ctx, workflowID, attemptID)
	if err != nil {
		HandleError(err, "Failed to get workflow attempt", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(attempt)
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
}

func HandleWorkflowAttemptKill(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	err := client.Workflow.KillWorkflowAttempt(ctx, workflowID, attemptID)
	if err != nil {
		HandleError(err, "Failed to kill workflow attempt", flags.Verbose)
	}

	fmt.Printf("Workflow attempt %s killed successfully\n", attemptID)
}

func HandleWorkflowAttemptRetry(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	var params map[string]interface{}
	if len(args) > 2 {
		err := json.Unmarshal([]byte(args[2]), &params)
		if err != nil {
			log.Fatalf("Invalid parameters JSON: %v", err)
		}
	}

	attempt, err := client.Workflow.RetryWorkflowAttempt(ctx, workflowID, attemptID, params)
	if err != nil {
		HandleError(err, "Failed to retry workflow attempt", flags.Verbose)
	}

	fmt.Printf("Workflow attempt retried successfully\n")
	fmt.Printf("New Attempt ID: %s\n", attempt.ID)
	fmt.Printf("Status: %s\n", attempt.Status)
}
