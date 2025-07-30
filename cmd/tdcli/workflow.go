package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// formatTimeJST formats time in JST timezone for display
func formatTimeJST(t time.Time) string {
	jst := time.FixedZone("JST", 9*3600) // JST is UTC+9
	return t.In(jst).Format("2006-01-02 15:04:05")
}

// Workflow handlers
func handleWorkflowList(ctx context.Context, client *td.Client, flags Flags) {
	opts := &td.WorkflowListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.Workflow.ListWorkflows(ctx, opts)
	if err != nil {
		handleError(err, "Failed to list workflows", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,project,status,created_at,updated_at")
		for _, workflow := range resp.Workflows {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				workflow.ID, workflow.Name, workflow.Project.Name, workflow.Status,
				workflow.CreatedAt.Format("2006-01-02 15:04:05"),
				workflow.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Workflows) == 0 {
			fmt.Println("No workflows found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPROJECT\tSTATUS\tCREATED")
		for _, workflow := range resp.Workflows {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				workflow.ID, workflow.Name, workflow.Project.Name, workflow.Status,
				workflow.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d workflows\n", len(resp.Workflows))
	}
}

func handleWorkflowGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	workflow, err := client.Workflow.GetWorkflow(ctx, workflowID)
	if err != nil {
		handleError(err, "Failed to get workflow", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(workflow)
	case "csv":
		fmt.Println("id,name,project,status,revision,timezone,created_at,updated_at")
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s\n",
			workflow.ID, workflow.Name, workflow.Project.Name, workflow.Status,
			workflow.Revision, workflow.Timezone,
			workflow.CreatedAt.Format("2006-01-02 15:04:05"),
			workflow.UpdatedAt.Format("2006-01-02 15:04:05"))
	default:
		fmt.Printf("ID: %s\n", workflow.ID)
		fmt.Printf("Name: %s\n", workflow.Name)
		fmt.Printf("Project: %s (%s)\n", workflow.Project.Name, workflow.Project.ID)
		fmt.Printf("Status: %s\n", workflow.Status)
		fmt.Printf("Revision: %s\n", workflow.Revision)
		fmt.Printf("Timezone: %s\n", workflow.Timezone)
		fmt.Printf("Created: %s\n", workflow.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", workflow.UpdatedAt.Format("2006-01-02 15:04:05"))
		if workflow.LastAttempt != nil {
			fmt.Printf("Last Attempt: %d\n", *workflow.LastAttempt)
		}
		if workflow.NextSchedule != nil {
			fmt.Printf("Next Schedule: %s\n", workflow.NextSchedule.Format("2006-01-02 15:04:05"))
		}
		if len(workflow.Config) > 0 {
			fmt.Printf("\nConfig:\n%+v\n", workflow.Config)
		}
	}
}

func handleWorkflowCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Name, project, and config required")
	}

	workflow, err := client.Workflow.CreateWorkflow(ctx, args[0], args[1], args[2])
	if err != nil {
		handleError(err, "Failed to create workflow", flags.Verbose)
	}

	fmt.Printf("Workflow created successfully\n")
	fmt.Printf("ID: %s\n", workflow.ID)
	fmt.Printf("Name: %s\n", workflow.Name)
}

func handleWorkflowUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and updates (key=value) required")
	}

	workflowID := args[0]

	updates := make(map[string]string)
	// Parse key=value pairs
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		updates[parts[0]] = parts[1]
	}

	workflow, err := client.Workflow.UpdateWorkflow(ctx, workflowID, updates)
	if err != nil {
		handleError(err, "Failed to update workflow", flags.Verbose)
	}

	fmt.Printf("Workflow %s updated successfully\n", workflow.ID)
}

func handleWorkflowDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	err := client.Workflow.DeleteWorkflow(ctx, workflowID)
	if err != nil {
		handleError(err, "Failed to delete workflow", flags.Verbose)
	}

	fmt.Printf("Workflow %s deleted successfully\n", workflowID)
}

func handleWorkflowStart(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	var params map[string]interface{}
	if len(args) > 1 {
		err := json.Unmarshal([]byte(args[1]), &params)
		if err != nil {
			log.Fatalf("Invalid parameters JSON: %v", err)
		}
	}

	attempt, err := client.Workflow.StartWorkflow(ctx, workflowID, params)
	if err != nil {
		handleError(err, "Failed to start workflow", flags.Verbose)
	}

	fmt.Printf("Workflow started successfully\n")
	fmt.Printf("Attempt ID: %s\n", attempt.ID)
	fmt.Printf("Status: %s\n", attempt.Status)
}

// Workflow attempt handlers
func handleWorkflowAttemptList(ctx context.Context, client *td.Client, args []string, flags Flags) {
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
		handleError(err, "Failed to list workflow attempts", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
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

func handleWorkflowAttemptGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	attempt, err := client.Workflow.GetWorkflowAttempt(ctx, workflowID, attemptID)
	if err != nil {
		handleError(err, "Failed to get workflow attempt", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(attempt)
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

func handleWorkflowAttemptKill(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	err := client.Workflow.KillWorkflowAttempt(ctx, workflowID, attemptID)
	if err != nil {
		handleError(err, "Failed to kill workflow attempt", flags.Verbose)
	}

	fmt.Printf("Workflow attempt %s killed successfully\n", attemptID)
}

func handleWorkflowAttemptRetry(ctx context.Context, client *td.Client, args []string, flags Flags) {
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
		handleError(err, "Failed to retry workflow attempt", flags.Verbose)
	}

	fmt.Printf("Workflow attempt retried successfully\n")
	fmt.Printf("New Attempt ID: %s\n", attempt.ID)
	fmt.Printf("Status: %s\n", attempt.Status)
}

// Workflow schedule handlers
func handleWorkflowScheduleGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	schedule, err := client.Workflow.GetWorkflowSchedule(ctx, workflowID)
	if err != nil {
		handleError(err, "Failed to get workflow schedule", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(schedule)
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

func handleWorkflowScheduleEnable(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	schedule, err := client.Workflow.EnableWorkflowSchedule(ctx, workflowID)
	if err != nil {
		handleError(err, "Failed to enable workflow schedule", flags.Verbose)
	}

	fmt.Printf("Workflow schedule enabled successfully\n")
	if schedule.NextTime != nil {
		fmt.Printf("Next run: %s\n", schedule.NextTime.Format("2006-01-02 15:04:05"))
	}
}

func handleWorkflowScheduleDisable(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	_, err := client.Workflow.DisableWorkflowSchedule(ctx, workflowID)
	if err != nil {
		handleError(err, "Failed to disable workflow schedule", flags.Verbose)
	}

	fmt.Printf("Workflow schedule disabled successfully\n")
}

func handleWorkflowScheduleUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
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
		handleError(err, "Failed to update workflow schedule", flags.Verbose)
	}

	fmt.Printf("Workflow schedule updated successfully\n")
	fmt.Printf("Cron: %s\n", schedule.Cron)
	fmt.Printf("Timezone: %s\n", schedule.Timezone)
	fmt.Printf("Delay: %d seconds\n", schedule.Delay)
}

// Workflow task handlers
func handleWorkflowTaskList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	resp, err := client.Workflow.ListWorkflowTasks(ctx, workflowID, attemptID)
	if err != nil {
		handleError(err, "Failed to list workflow tasks", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
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

func handleWorkflowTaskGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Workflow ID, attempt ID, and task ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	task, err := client.Workflow.GetWorkflowTask(ctx, workflowID, attemptID, args[2])
	if err != nil {
		handleError(err, "Failed to get workflow task", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(task)
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

// Workflow log handlers
func handleWorkflowAttemptLog(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and attempt ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	logContent, err := client.Workflow.GetWorkflowAttemptLog(ctx, workflowID, attemptID)
	if err != nil {
		handleError(err, "Failed to get workflow attempt log", flags.Verbose)
	}

	fmt.Print(logContent)
}

func handleWorkflowTaskLog(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Workflow ID, attempt ID, and task ID required")
	}

	workflowID := args[0]

	attemptID := args[1]

	logContent, err := client.Workflow.GetWorkflowTaskLog(ctx, workflowID, attemptID, args[2])
	if err != nil {
		handleError(err, "Failed to get workflow task log", flags.Verbose)
	}

	fmt.Print(logContent)
}

// Workflow project handlers
func handleWorkflowProjectList(ctx context.Context, client *td.Client, flags Flags) {
	resp, err := client.Workflow.ListProjects(ctx)
	if err != nil {
		handleError(err, "Failed to list workflow projects", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,revision,archive_type,created_at,updated_at")
		for _, project := range resp.Projects {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				project.ID, project.Name, project.Revision, project.ArchiveType,
				formatTimeJST(project.CreatedAt.Time),
				formatTimeJST(project.UpdatedAt.Time))
		}
	default:
		if len(resp.Projects) == 0 {
			fmt.Println("No projects found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tREVISION\tTYPE\tCREATED")
		for _, project := range resp.Projects {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				project.ID, project.Name, project.Revision, project.ArchiveType,
				formatTimeJST(project.CreatedAt.Time))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d projects\n", len(resp.Projects))
	}
}

func handleWorkflowProjectGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project ID required")
	}

	projectID := args[0]

	project, err := client.Workflow.GetProject(ctx, projectID)
	if err != nil {
		handleError(err, "Failed to get workflow project", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(project)
	case "csv":
		fmt.Println("id,name,revision,archive_type,archive_md5,created_at,updated_at,deleted_at")
		deletedAt := ""
		if project.DeletedAt != nil {
			deletedAt = formatTimeJST(project.DeletedAt.Time)
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s\n",
			project.ID, project.Name, project.Revision, project.ArchiveType,
			project.ArchiveMD5,
			formatTimeJST(project.CreatedAt.Time),
			formatTimeJST(project.UpdatedAt.Time),
			deletedAt)
	default:
		fmt.Printf("ID: %s\n", project.ID)
		fmt.Printf("Name: %s\n", project.Name)
		fmt.Printf("Revision: %s\n", project.Revision)
		fmt.Printf("Archive Type: %s\n", project.ArchiveType)
		fmt.Printf("Archive MD5: %s\n", project.ArchiveMD5)
		fmt.Printf("Created: %s\n", formatTimeJST(project.CreatedAt.Time))
		fmt.Printf("Updated: %s\n", formatTimeJST(project.UpdatedAt.Time))
		if project.DeletedAt != nil {
			fmt.Printf("Deleted: %s\n", formatTimeJST(project.DeletedAt.Time))
		}
	}
}

func handleWorkflowProjectCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Project name and path (directory or archive file) required")
	}

	path := args[1]

	// Check if the path is a directory or file
	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Fatalf("Failed to access path %s: %v", path, err)
	}

	var project *td.WorkflowProject

	if fileInfo.IsDir() {
		// Create project from directory
		fmt.Printf("Creating project from directory: %s\n", path)
		project, err = client.Workflow.CreateProjectFromDirectory(ctx, args[0], path)
	} else {
		// Create project from archive file
		fmt.Printf("Creating project from archive file: %s\n", path)
		archiveData, readErr := os.ReadFile(path)
		if readErr != nil {
			log.Fatalf("Failed to read archive file: %v", readErr)
		}
		project, err = client.Workflow.CreateProject(ctx, args[0], archiveData)
	}

	if err != nil {
		handleError(err, "Failed to create workflow project", flags.Verbose)
	}

	fmt.Printf("Project created successfully\n")
	fmt.Printf("ID: %s\n", project.ID)
	fmt.Printf("Name: %s\n", project.Name)
	fmt.Printf("Revision: %s\n", project.Revision)
}

func handleWorkflowProjectWorkflows(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project ID required")
	}

	projectID := args[0]

	resp, err := client.Workflow.ListProjectWorkflows(ctx, projectID)
	if err != nil {
		handleError(err, "Failed to list project workflows", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("id,name,project,status,created_at,updated_at")
		for _, workflow := range resp.Workflows {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				workflow.ID, workflow.Name, workflow.Project.Name, workflow.Status,
				workflow.CreatedAt.Format("2006-01-02 15:04:05"),
				workflow.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Workflows) == 0 {
			fmt.Println("No workflows found in this project")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tSTATUS\tCREATED")
		for _, workflow := range resp.Workflows {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				workflow.ID, workflow.Name, workflow.Status,
				workflow.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d workflows\n", len(resp.Workflows))
	}
}

func handleWorkflowProjectSecretsList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project ID required")
	}

	projectID := args[0]

	resp, err := client.Workflow.GetProjectSecrets(ctx, projectID)
	if err != nil {
		handleError(err, "Failed to list project secrets", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(resp)
	case "csv":
		fmt.Println("key,value")
		for key, value := range resp.Secrets {
			fmt.Printf("%s,%s\n", key, value)
		}
	default:
		if len(resp.Secrets) == 0 {
			fmt.Println("No secrets found in this project")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "KEY\tVALUE")
		for key, value := range resp.Secrets {
			fmt.Fprintf(w, "%s\t%s\n", key, value)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d secrets\n", len(resp.Secrets))
	}
}

func handleWorkflowProjectSecretsSet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Project ID, secret key, and secret value required")
	}

	projectID := args[0]

	err := client.Workflow.SetProjectSecret(ctx, projectID, args[1], args[2])
	if err != nil {
		handleError(err, "Failed to set project secret", flags.Verbose)
	}

	fmt.Printf("Secret '%s' set successfully for project %s\n", args[1], projectID)
}

func handleWorkflowProjectSecretsDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Project ID and secret key required")
	}

	projectID := args[0]

	err := client.Workflow.DeleteProjectSecret(ctx, projectID, args[1])
	if err != nil {
		handleError(err, "Failed to delete project secret", flags.Verbose)
	}

	fmt.Printf("Secret '%s' deleted successfully from project %s\n", args[1], projectID)
}

// Workflow hooks handlers
func handleWorkflowHooksShow(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project directory path required")
	}

	dirPath := args[0]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("No hooks configuration found at %s\n", configPath)
		fmt.Println("Run 'tdcli workflow projects hooks init' to create a hooks configuration file.")
		return
	}

	// Read and display config
	configData, err := os.ReadFile(configPath)
	if err != nil {
		handleError(err, "Failed to read hooks configuration", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		fmt.Print(string(configData))
	default:
		var config td.WorkflowHooksConfig
		if err := json.Unmarshal(configData, &config); err != nil {
			handleError(err, "Failed to parse hooks configuration", flags.Verbose)
		}

		if len(config.PreUploadHooks) == 0 {
			fmt.Println("No pre-upload hooks configured")
			return
		}

		fmt.Printf("Pre-upload hooks (%d):\n", len(config.PreUploadHooks))
		for i, hook := range config.PreUploadHooks {
			fmt.Printf("\n%d. %s\n", i+1, hook.Name)
			fmt.Printf("   Command: %s\n", strings.Join(hook.Command, " "))
			if hook.Timeout > 0 {
				fmt.Printf("   Timeout: %d seconds\n", hook.Timeout)
			}
			fmt.Printf("   Fail on error: %t\n", hook.FailOnError)
			if hook.WorkingDir != "" {
				fmt.Printf("   Working directory: %s\n", hook.WorkingDir)
			}
		}
	}
}

func handleWorkflowHooksInit(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project directory path required")
	}

	dirPath := args[0]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Check if config file already exists
	if _, err := os.Stat(configPath); err == nil {
		fmt.Printf("Hooks configuration file already exists at %s\n", configPath)
		return
	}

	// Create default configuration
	config := td.WorkflowHooksConfig{
		PreUploadHooks: []td.WorkflowHook{
			{
				Name:        "lint",
				Command:     []string{"echo", "Add your linting command here"},
				Timeout:     60,
				FailOnError: true,
				WorkingDir:  "",
			},
		},
	}

	// Marshal to JSON with indentation
	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		handleError(err, "Failed to create hooks configuration", flags.Verbose)
	}

	// Write to file
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		handleError(err, "Failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Created hooks configuration file at %s\n", configPath)
	fmt.Println("Edit this file to configure your pre-upload hooks.")
}

func handleWorkflowHooksAdd(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 6 {
		log.Fatal("Path, name, timeout, fail_on_error, working_dir, and command required")
	}

	dirPath := args[0]
	name := args[1]
	timeout, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatalf("Invalid timeout: %s", args[2])
	}
	failOnError, err := strconv.ParseBool(args[3])
	if err != nil {
		log.Fatalf("Invalid fail_on_error: %s", args[3])
	}
	workingDir := args[4]
	command := args[5:]

	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Load existing config or create new one
	var config td.WorkflowHooksConfig
	if configData, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(configData, &config); err != nil {
			handleError(err, "Failed to parse existing hooks configuration", flags.Verbose)
		}
	}

	// Check if hook with same name already exists
	for _, hook := range config.PreUploadHooks {
		if hook.Name == name {
			log.Fatalf("Hook with name '%s' already exists", name)
		}
	}

	// Add new hook
	newHook := td.WorkflowHook{
		Name:        name,
		Command:     command,
		Timeout:     timeout,
		FailOnError: failOnError,
		WorkingDir:  workingDir,
	}

	config.PreUploadHooks = append(config.PreUploadHooks, newHook)

	// Write updated config
	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		handleError(err, "Failed to serialize hooks configuration", flags.Verbose)
	}

	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		handleError(err, "Failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Added hook '%s' to %s\n", name, configPath)
}

func handleWorkflowHooksRemove(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Project directory path and hook name required")
	}

	dirPath := args[0]
	hookName := args[1]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Load existing config
	configData, err := os.ReadFile(configPath)
	if err != nil {
		handleError(err, "Failed to read hooks configuration", flags.Verbose)
	}

	var config td.WorkflowHooksConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		handleError(err, "Failed to parse hooks configuration", flags.Verbose)
	}

	// Find and remove hook
	found := false
	var updatedHooks []td.WorkflowHook
	for _, hook := range config.PreUploadHooks {
		if hook.Name != hookName {
			updatedHooks = append(updatedHooks, hook)
		} else {
			found = true
		}
	}

	if !found {
		log.Fatalf("Hook '%s' not found", hookName)
	}

	config.PreUploadHooks = updatedHooks

	// Write updated config
	updatedConfigData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		handleError(err, "Failed to serialize hooks configuration", flags.Verbose)
	}

	if err := os.WriteFile(configPath, updatedConfigData, 0644); err != nil {
		handleError(err, "Failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Removed hook '%s' from %s\n", hookName, configPath)
}

func handleWorkflowHooksTest(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project directory path required")
	}

	dirPath := args[0]
	
	fmt.Printf("Testing pre-upload hooks in %s...\n", dirPath)
	
	// Load hooks configuration
	configPath := filepath.Join(dirPath, ".td-hooks.json")
	
	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Println("No hooks configuration found")
		return
	}
	
	// Read and parse config file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		handleError(err, "Failed to read hooks configuration", flags.Verbose)
	}
	
	var config td.WorkflowHooksConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		handleError(err, "Failed to parse hooks configuration", flags.Verbose)
	}
	
	if len(config.PreUploadHooks) == 0 {
		fmt.Println("No pre-upload hooks configured")
		return
	}
	
	fmt.Printf("Found %d pre-upload hook(s)\n", len(config.PreUploadHooks))
	
	// We can't directly call the package-private functions, so we'll just load and display the config
	for i, hook := range config.PreUploadHooks {
		fmt.Printf("%d. Hook '%s': %s\n", i+1, hook.Name, strings.Join(hook.Command, " "))
	}
	
	fmt.Println("Use 'tdcli workflow projects push' to test hooks execution during actual upload")
}
