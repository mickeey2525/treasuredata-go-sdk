package main

import (
	"context"
	"encoding/json"
	"fmt"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handlePostbackSend(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		fmt.Println("Usage: tdcli postback send <database> <table> <json-data>")
		fmt.Println("Example: tdcli postback send mydb mytable '{\"email\":\"test@example.com\"}'")
		return
	}

	database := args[0]
	table := args[1]
	dataStr := args[2]

	var event map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &event); err != nil {
		handleError(err, "Failed to parse event JSON", flags.Verbose)
		return
	}

	if err := client.Postback.SendEvent(ctx, database, table, event); err != nil {
		handleError(err, "Failed to send postback event", flags.Verbose)
		return
	}

	fmt.Println("Event sent successfully")
}
