package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleWorkflowTaskList lists tasks for a workflow attempt.
func HandleWorkflowTaskList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Workflow ID and attempt ID required")
	}

	resp, err := client.Workflow.ListWorkflowTasks(ctx, args[0], args[1])
	if err != nil {
		return wrapError(err, "failed to list workflow tasks", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		return PrintJSON(resp)
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
			return nil
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
	return nil
}

// HandleWorkflowTaskGet retrieves a single workflow task.
func HandleWorkflowTaskGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return usageError("Workflow ID, attempt ID, and task ID required")
	}

	task, err := client.Workflow.GetWorkflowTask(ctx, args[0], args[1], args[2])
	if err != nil {
		return wrapError(err, "failed to get workflow task", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		return PrintJSON(task)
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
	return nil
}
