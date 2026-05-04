package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handlePostbackSend(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if client == nil {
		return errors.New("client is not initialized")
	}
	if len(args) < 3 {
		return errors.New("usage: tdcli postback send <database> <table> <json-data>\n" +
			"Example: tdcli postback send mydb mytable '{\"email\":\"test@example.com\"}'")
	}

	database := args[0]
	table := args[1]
	dataStr := args[2]

	var event map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &event); err != nil {
		return wrapErr(err, "failed to parse event JSON", flags.Verbose)
	}

	if err := client.Postback.SendEvent(ctx, database, table, event); err != nil {
		return wrapErr(err, "failed to send postback event", flags.Verbose)
	}

	fmt.Println("Event sent successfully")
	return nil
}
