package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleLLMActionList(ctx context.Context, client *td.Client, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	resp, err := client.LLM.ListActions(ctx)
	if err != nil {
		return wrapErr(err, "failed to list LLM actions", flags.Verbose)
	}

	if flags.Format == "json" {
		printJSON(resp)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tTYPE\tINTEGRATION ID\tPROMPT ID\tUI TAGS")
	for _, action := range resp.Data {
		tagStr := strings.Join(action.UITags, ", ")
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", action.ID, action.Type, action.IntegrationID, action.PromptID, tagStr)
	}
	w.Flush()
	return nil
}

func handleLLMActionGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	if len(args) < 1 {
		return errors.New("usage: tdcli llm actions get <action-id>")
	}

	resp, err := client.LLM.GetAction(ctx, args[0])
	if err != nil {
		return wrapErr(err, "failed to get LLM action", flags.Verbose)
	}

	if flags.Format == "json" {
		printJSON(resp)
		return nil
	}

	action := resp.Data
	fmt.Printf("ID:          %s\n", action.ID)
	fmt.Printf("Type:        %s\n", action.Type)
	fmt.Printf("Integration: %s\n", action.IntegrationID)
	fmt.Printf("Prompt:      %s\n", action.PromptID)
	fmt.Printf("Webhook URL: %s\n", action.WebhookTextURL)
	if len(action.UITags) > 0 {
		fmt.Printf("UI Tags:     %s\n", strings.Join(action.UITags, ", "))
	}
	return nil
}

func handleLLMActionExecute(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	if len(args) < 2 {
		return errors.New("usage: tdcli llm actions execute <action-id> <json-input>")
	}

	actionID := args[0]
	inputStr := args[1]

	var input map[string]interface{}
	if err := json.Unmarshal([]byte(inputStr), &input); err != nil {
		return wrapErr(err, "failed to parse input JSON", flags.Verbose)
	}

	resp, err := client.LLM.ExecuteAction(ctx, actionID, input)
	if err != nil {
		return wrapErr(err, "failed to execute LLM action", flags.Verbose)
	}

	printJSON(resp)
	return nil
}

func handleLLMIntegrationList(ctx context.Context, client *td.Client, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	resp, err := client.LLM.ListIntegrations(ctx)
	if err != nil {
		return wrapErr(err, "failed to list LLM integrations", flags.Verbose)
	}

	if flags.Format == "json" {
		printJSON(resp)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS")
	for _, integration := range resp.Data {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", integration.ID, integration.Name, integration.Type, integration.Status)
	}
	w.Flush()
	return nil
}

func handleLLMPromptList(ctx context.Context, client *td.Client, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	resp, err := client.LLM.ListPrompts(ctx)
	if err != nil {
		return wrapErr(err, "failed to list LLM prompts", flags.Verbose)
	}

	if flags.Format == "json" {
		printJSON(resp)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tSTATUS\tVARIABLES")
	for _, prompt := range resp.Data {
		varsStr := strings.Join(prompt.Variables, ", ")
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", prompt.ID, prompt.Name, prompt.Status, varsStr)
	}
	w.Flush()
	return nil
}

func handleLLMProjectList(ctx context.Context, client *td.Client, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	resp, err := client.LLM.ListProjects(ctx)
	if err != nil {
		return wrapErr(err, "failed to list LLM projects", flags.Verbose)
	}

	if flags.Format == "json" {
		printJSON(resp)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tSTATUS")
	for _, project := range resp.Data {
		fmt.Fprintf(w, "%s\t%s\t%s\n", project.ID, project.Name, project.Status)
	}
	w.Flush()
	return nil
}

func handleLLMProjectGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	if len(args) < 1 {
		return errors.New("usage: tdcli llm projects get <project-id>")
	}

	project, err := client.LLM.GetProject(ctx, args[0])
	if err != nil {
		return wrapErr(err, "failed to get LLM project", flags.Verbose)
	}

	if flags.Format == "json" {
		printJSON(project)
		return nil
	}

	fmt.Printf("ID:          %s\n", project.ID)
	fmt.Printf("Name:        %s\n", project.Name)
	fmt.Printf("Status:      %s\n", project.Status)
	if project.Description != "" {
		fmt.Printf("Description: %s\n", project.Description)
	}
	return nil
}
