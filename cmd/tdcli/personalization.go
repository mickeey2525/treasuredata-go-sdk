package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handlePersonalizationSend(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		fmt.Println("Usage: tdcli personalization send <database> <table> <json-data> [--token TOKEN]")
		fmt.Println("Example: tdcli personalization send mydb mytable '{\"td_client_id\":\"abc123\"}'")
		return
	}

	database := args[0]
	table := args[1]
	dataStr := args[2]
	token := ""
	if len(args) > 3 {
		token = args[3]
	}

	var event map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &event); err != nil {
		handleError(err, "Failed to parse event JSON", flags.Verbose)
		return
	}

	var resp *td.PersonalizationResponse
	var err error
	if token != "" {
		resp, err = client.Personalization.SendEventWithToken(ctx, database, table, token, event)
	} else {
		resp, err = client.Personalization.SendEvent(ctx, database, table, event)
	}

	if err != nil {
		handleError(err, "Failed to send personalization event", flags.Verbose)
		return
	}

	if resp == nil || len(resp.Offers) == 0 {
		fmt.Println("Event sent successfully. No offers returned.")
		return
	}

	fmt.Println("Event sent successfully. Matching offers:")
	outputPersonalizationResponse(resp, flags)
}

func outputPersonalizationResponse(resp *td.PersonalizationResponse, flags Flags) {
	switch flags.Format {
	case "json":
		outputPersonalizationJSON(resp)
	case "csv":
		outputPersonalizationCSV(resp)
	default:
		outputPersonalizationTable(resp)
	}
}

func outputPersonalizationJSON(resp *td.PersonalizationResponse) {
	printJSON(resp)
}

func outputPersonalizationCSV(resp *td.PersonalizationResponse) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	_ = w.Write([]string{"offer_name", "attribute_key", "attribute_value"})
	for offerName, offer := range resp.Offers {
		for key, value := range offer.Attributes {
			_ = w.Write([]string{offerName, key, fmt.Sprintf("%v", value)})
		}
	}
}

func outputPersonalizationTable(resp *td.PersonalizationResponse) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "OFFER NAME\tATTRIBUTES\tBATCH SEGMENTS")
	for offerName, offer := range resp.Offers {
		attrStr := ""
		for k, v := range offer.Attributes {
			if attrStr != "" {
				attrStr += ", "
			}
			attrStr += fmt.Sprintf("%s=%v", k, v)
		}
		segStr := ""
		for _, seg := range offer.BatchSegments {
			if segStr != "" {
				segStr += ", "
			}
			segStr += seg.ID
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", offerName, attrStr, segStr)
	}
	w.Flush()
}
