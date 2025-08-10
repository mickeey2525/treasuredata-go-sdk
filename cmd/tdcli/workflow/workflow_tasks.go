package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Workflow task handlers
func HandleWorkflowTaskList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	resp, err := client.Workflow.ListWorkflowTasks(ctx, workflowID, attemptID)
	if err != nil {
		HandleError(err, "Failed to list workflow tasks", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(resp)
	case "csv":
		fmt.Println("id,full_name,state,is_group,started_at,updated_at")
		for _, task := range resp.Tasks {
			startedAt := ""
			if task.StartedAt != nil {
				startedAt = task.StartedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Printf("%s,%s,%s,%t,%s,%s\n",
				task.ID, task.FullName, task.State, task.IsGroup,
				startedAt, task.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Tasks) == 0 {
			fmt.Println("No tasks found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tSTATE\tGROUP\tSTARTED")
		for _, task := range resp.Tasks {
			startedAt := "-"
			if task.StartedAt != nil {
				startedAt = task.StartedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%t\t%s\n",
				task.ID, task.FullName, task.State, task.IsGroup, startedAt)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d tasks\n", len(resp.Tasks))
	}
}

func HandleWorkflowTaskGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Workflow ID, attempt ID, and task ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	task, err := client.Workflow.GetWorkflowTask(ctx, workflowID, attemptID, args[2])
	if err != nil {
		HandleError(err, "Failed to get workflow task", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(task)
	case "csv":
		fmt.Println("id,full_name,state,is_group,started_at,updated_at")
		startedAt := ""
		if task.StartedAt != nil {
			startedAt = task.StartedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%s,%s,%s,%t,%s,%s\n",
			task.ID, task.FullName, task.State, task.IsGroup,
			startedAt, task.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", task.ID)
		fmt.Printf("Full Name: %s\n", task.FullName)
		fmt.Printf("State: %s\n", task.State)
		fmt.Printf("Is Group: %t\n", task.IsGroup)
		if task.ParentID != nil {
			fmt.Printf("Parent ID: %s\n", *task.ParentID)
		}
		if task.StartedAt != nil {
			fmt.Printf("Started: %s\n", task.StartedAt.Format("2006-01-02 15:04:05"))
		}
		fmt.Printf("Updated: %s\n", task.UpdatedAt.Format("2006-01-02 15:04:05"))
		if len(task.UpstreamsID) > 0 {
			fmt.Printf("Upstreams: %s\n", strings.Join(task.UpstreamsID, ", "))
		}
		if len(task.Config) > 0 {
			fmt.Printf("\nConfig:\n")
			configJSON, _ := json.MarshalIndent(task.Config, "  ", "  ")
			fmt.Printf("  %s\n", configJSON)
		}
	}
}
