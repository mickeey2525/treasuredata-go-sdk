package treasuredata_test

import (
	"context"
	"fmt"
	"log"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func ExampleNewClient() {
	// Create a simple client
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	// Create a client with region
	client, err = td.NewClient("YOUR_API_KEY", td.WithRegion("jp"))
	if err != nil {
		log.Fatal(err)
	}

	// Create a client with custom endpoint
	client, err = td.NewClient("YOUR_API_KEY", td.WithEndpoint("https://api.treasuredata.co.jp"))
	if err != nil {
		log.Fatal(err)
	}

	_ = client
}

func ExampleDatabasesService_List() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	databases, err := client.Databases.List(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, db := range databases {
		fmt.Printf("Database: %s (Count: %d)\n", db.Name, db.Count)
	}
}

func ExampleTablesService_Create() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	resp, err := client.Tables.Create(ctx, "my_database", "new_table", "log")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Created table: %s.%s (type: %s)\n", resp.Database, resp.Table, resp.Type)
}

func ExampleQueriesService_Issue() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	opts := &td.IssueQueryOptions{
		Query:      "SELECT COUNT(*) FROM access_logs WHERE time > TD_TIME_RANGE(NOW(), '-1h')",
		Priority:   0,
		RetryLimit: 1,
		DomainKey:  "unique-query-key-123", // For idempotency
	}

	resp, err := client.Queries.Issue(ctx, td.QueryTypePresto, "my_database", opts)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Job ID: %s\n", resp.JobID)

	// Wait for job completion
	for {
		status, err := client.Jobs.Status(ctx, resp.JobID)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Status: %s\n", status.Status)

		if status.Status == "success" || status.Status == "error" {
			break
		}

		time.Sleep(5 * time.Second)
	}
}

func ExampleResultsService_GetResultJSONL() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	scanner, err := client.Results.GetResultJSONL(ctx, "12345")
	if err != nil {
		log.Fatal(err)
	}
	defer scanner.Close()

	for scanner.Scan() {
		var record map[string]interface{}
		if err := scanner.Decode(&record); err != nil {
			log.Printf("Error decoding: %v", err)
			continue
		}

		// Process each record
		fmt.Printf("Record: %v\n", record)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func ExampleBulkImportService() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	sessionName := fmt.Sprintf("import_%d", time.Now().Unix())

	// Create bulk import session
	err = client.BulkImport.Create(ctx, sessionName, "my_database", "my_table")
	if err != nil {
		log.Fatal(err)
	}

	// Upload data (in practice, this would be your actual data)
	// data := prepareYourData()
	// err = client.BulkImport.UploadPart(ctx, sessionName, "part1", data)
	// if err != nil {
	//     log.Fatal(err)
	// }

	// Freeze and commit
	err = client.BulkImport.Freeze(ctx, sessionName)
	if err != nil {
		log.Fatal(err)
	}

	err = client.BulkImport.Commit(ctx, sessionName)
	if err != nil {
		log.Fatal(err)
	}

	// Perform the import
	job, err := client.BulkImport.Perform(ctx, sessionName)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Import job ID: %s\n", job.JobID)
}

func ExampleCDPService_CreateSegment() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	segment, err := client.CDP.CreateSegment(ctx,
		"audience123", // Audience ID
		"High Value Customers",
		"Customers with lifetime value > $1000",
		"SELECT * FROM customers WHERE lifetime_value > 1000",
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Segment created: %s (ID: %s)\n", segment.Name, segment.ID)
}

func ExampleCDPService_CreateAudience() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	audience, err := client.CDP.CreateAudience(ctx,
		"Marketing Campaign Audience",
		"Target audience for Q4 marketing campaign",
		"marketing_db",
		"customers",
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Audience created: %s (ID: %s)\n", audience.Name, audience.ID)
}

func ExampleCDPService_CreateActivation() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	config := map[string]interface{}{
		"destination": "google-ads",
		"account_id":  "123456789",
		"sync_mode":   "incremental",
	}

	activation, err := client.CDP.CreateActivation(ctx,
		"Google Ads Sync",
		"google-ads",
		"audience-123",
		config,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Activation created: %s (ID: %s, Status: %s)\n",
		activation.Name, activation.ID, activation.Status)
}

func ExampleWorkflowService_ListWorkflows() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	opts := &td.WorkflowListOptions{
		Limit:  10,
		Offset: 0,
	}

	resp, err := client.Workflow.ListWorkflows(ctx, opts)
	if err != nil {
		log.Fatal(err)
	}

	for _, workflow := range resp.Workflows {
		fmt.Printf("Workflow: %s (ID: %d, Status: %s)\n",
			workflow.Name, workflow.ID, workflow.Status)
	}
}

func ExampleWorkflowService_GetWorkflow() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	workflow, err := client.Workflow.GetWorkflow(ctx, 12345)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Workflow: %s\n", workflow.Name)
	fmt.Printf("Project: %s\n", workflow.Project)
	fmt.Printf("Status: %s\n", workflow.Status)
}

func ExampleWorkflowService_StartWorkflow() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	params := map[string]interface{}{
		"param1": "value1",
		"param2": 123,
	}

	attempt, err := client.Workflow.StartWorkflow(ctx, 12345, params)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Workflow started: Attempt ID %d\n", attempt.ID)
	fmt.Printf("Status: %s\n", attempt.Status)
}

func ExampleWorkflowService_ListWorkflowAttempts() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	opts := &td.WorkflowAttemptListOptions{
		Limit:  10,
		Status: "running",
	}

	resp, err := client.Workflow.ListWorkflowAttempts(ctx, 12345, opts)
	if err != nil {
		log.Fatal(err)
	}

	for _, attempt := range resp.Attempts {
		fmt.Printf("Attempt %d: %s\n", attempt.Index, attempt.Status)
	}
}

func ExampleWorkflowService_CreateWorkflow() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	config := `
timezone: UTC
schedule:
  daily>: 02:00:00

+task1:
  td>: queries/sample.sql
  database: sample_db
`

	workflow, err := client.Workflow.CreateWorkflow(ctx,
		"my-workflow",
		"my-project",
		config)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Workflow created: %s (ID: %d)\n", workflow.Name, workflow.ID)
}

func ExampleWorkflowService_GetWorkflowSchedule() {
	client, err := td.NewClient("YOUR_API_KEY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	schedule, err := client.Workflow.GetWorkflowSchedule(ctx, 12345)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Schedule: %s\n", schedule.Cron)
	fmt.Printf("Timezone: %s\n", schedule.Timezone)
	if schedule.NextTime != nil {
		fmt.Printf("Next run: %s\n", schedule.NextTime.Format("2006-01-02 15:04:05"))
	}
}
