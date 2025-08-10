package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestTablesService_List(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/list/test_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/table/list/test_db")

		fmt.Fprint(w, `{
			"database": "test_db",
			"tables": [
				{
					"id": 12345,
					"name": "events",
					"database": "test_db",
					"type": "log",
					"count": 1000000,
					"created_at": 1609459200,
					"updated_at": 1609545600,
					"estimated_storage_size": 1048576,
					"last_log_timestamp": 1609632000,
					"delete_protected": false,
					"schema": "[{\"name\":\"user_id\",\"type\":\"string\"}]",
					"expire_days": 90,
					"include_v": true,
					"counter_updated_at": 1609718400
				},
				{
					"id": 12346,
					"name": "users",
					"database": "test_db",
					"type": "item",
					"count": 50000,
					"created_at": 1609459200,
					"updated_at": 1609545600,
					"estimated_storage_size": 524288,
					"last_log_timestamp": null,
					"delete_protected": true,
					"schema": "",
					"include_v": false
				}
			]
		}`)
	})

	ctx := context.Background()
	tables, err := client.Tables.List(ctx, "test_db")
	if err != nil {
		t.Errorf("Tables.List returned error: %v", err)
	}

	expireDays := 90
	lastLogTimestamp := int64(1609632000)
	counterUpdatedAt := TDTime{time.Unix(1609718400, 0)}

	want := []Table{
		{
			ID:                   12345,
			Name:                 "events",
			Database:             "test_db",
			Type:                 "log",
			Count:                1000000,
			CreatedAt:            TDTime{time.Unix(1609459200, 0)},
			UpdatedAt:            TDTime{time.Unix(1609545600, 0)},
			EstimatedStorageSize: 1048576,
			LastLogTimestamp:     FlexibleInt64{Value: &lastLogTimestamp},
			DeleteProtected:      false,
			Schema:               "[{\"name\":\"user_id\",\"type\":\"string\"}]",
			ExpireDays:           &expireDays,
			IncludeV:             true,
			CounterUpdatedAt:     &counterUpdatedAt,
		},
		{
			ID:                   12346,
			Name:                 "users",
			Database:             "test_db",
			Type:                 "item",
			Count:                50000,
			CreatedAt:            TDTime{time.Unix(1609459200, 0)},
			UpdatedAt:            TDTime{time.Unix(1609545600, 0)},
			EstimatedStorageSize: 524288,
			LastLogTimestamp:     FlexibleInt64{Value: nil},
			DeleteProtected:      true,
			Schema:               "",
			ExpireDays:           nil,
			IncludeV:             false,
			CounterUpdatedAt:     nil,
		},
	}

	// Compare tables length first
	if len(tables) != len(want) {
		t.Errorf("Tables.List returned %d tables, want %d", len(tables), len(want))
		return
	}

	// Compare each table individually with special handling for pointer fields
	for i, table := range tables {
		wantTable := want[i]
		
		// Compare basic fields
		if table.ID != wantTable.ID || table.Name != wantTable.Name || table.Database != wantTable.Database ||
			table.Type != wantTable.Type || table.Count != wantTable.Count || table.DeleteProtected != wantTable.DeleteProtected ||
			table.Schema != wantTable.Schema || table.IncludeV != wantTable.IncludeV ||
			table.EstimatedStorageSize != wantTable.EstimatedStorageSize {
			t.Errorf("Table %d basic fields mismatch: got %+v, want %+v", i, table, wantTable)
		}
		
		// Compare time fields
		if !table.CreatedAt.Equal(wantTable.CreatedAt.Time) || !table.UpdatedAt.Equal(wantTable.UpdatedAt.Time) {
			t.Errorf("Table %d time fields mismatch: got CreatedAt=%v UpdatedAt=%v, want CreatedAt=%v UpdatedAt=%v", 
				i, table.CreatedAt, table.UpdatedAt, wantTable.CreatedAt, wantTable.UpdatedAt)
		}
		
		// Compare LastLogTimestamp (FlexibleInt64)
		if (table.LastLogTimestamp.Value == nil) != (wantTable.LastLogTimestamp.Value == nil) {
			t.Errorf("Table %d LastLogTimestamp nil mismatch: got %v, want %v", i, table.LastLogTimestamp.Value, wantTable.LastLogTimestamp.Value)
		} else if table.LastLogTimestamp.Value != nil && wantTable.LastLogTimestamp.Value != nil &&
			*table.LastLogTimestamp.Value != *wantTable.LastLogTimestamp.Value {
			t.Errorf("Table %d LastLogTimestamp value mismatch: got %d, want %d", i, *table.LastLogTimestamp.Value, *wantTable.LastLogTimestamp.Value)
		}
		
		// Compare ExpireDays
		if (table.ExpireDays == nil) != (wantTable.ExpireDays == nil) {
			t.Errorf("Table %d ExpireDays nil mismatch: got %v, want %v", i, table.ExpireDays, wantTable.ExpireDays)
		} else if table.ExpireDays != nil && wantTable.ExpireDays != nil &&
			*table.ExpireDays != *wantTable.ExpireDays {
			t.Errorf("Table %d ExpireDays value mismatch: got %d, want %d", i, *table.ExpireDays, *wantTable.ExpireDays)
		}
		
		// Compare CounterUpdatedAt
		if (table.CounterUpdatedAt == nil) != (wantTable.CounterUpdatedAt == nil) {
			t.Errorf("Table %d CounterUpdatedAt nil mismatch: got %v, want %v", i, table.CounterUpdatedAt, wantTable.CounterUpdatedAt)
		} else if table.CounterUpdatedAt != nil && wantTable.CounterUpdatedAt != nil &&
			!table.CounterUpdatedAt.Equal(wantTable.CounterUpdatedAt.Time) {
			t.Errorf("Table %d CounterUpdatedAt value mismatch: got %v, want %v", i, *table.CounterUpdatedAt, *wantTable.CounterUpdatedAt)
		}
	}
}

func TestTablesService_List_DatabaseNotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/list/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Database not found"}`)
	})

	ctx := context.Background()
	_, err := client.Tables.List(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestTablesService_Get(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/show/test_db/events", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/table/show/test_db/events")

		fmt.Fprint(w, `{
			"id": 12345,
			"name": "events",
			"database": "test_db",
			"type": "log",
			"count": 1000000,
			"created_at": 1609459200,
			"updated_at": 1609545600,
			"estimated_storage_size": 1048576,
			"last_log_timestamp": "1609632000",
			"delete_protected": false,
			"schema": "[{\"name\":\"user_id\",\"type\":\"string\"}]",
			"expire_days": 90,
			"include_v": true
		}`)
	})

	ctx := context.Background()
	table, err := client.Tables.Get(ctx, "test_db", "events")
	if err != nil {
		t.Errorf("Tables.Get returned error: %v", err)
	}

	expireDays := 90
	lastLogTimestamp := int64(1609632000)
	want := &Table{
		ID:                   12345,
		Name:                 "events",
		Database:             "test_db",
		Type:                 "log",
		Count:                1000000,
		CreatedAt:            TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:            TDTime{time.Unix(1609545600, 0)},
		EstimatedStorageSize: 1048576,
		LastLogTimestamp:     FlexibleInt64{Value: &lastLogTimestamp},
		DeleteProtected:      false,
		Schema:               "[{\"name\":\"user_id\",\"type\":\"string\"}]",
		ExpireDays:           &expireDays,
		IncludeV:             true,
	}

	if !reflect.DeepEqual(table, want) {
		t.Errorf("Tables.Get returned %+v, want %+v", table, want)
	}
}

func TestTablesService_Get_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/show/test_db/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Table not found"}`)
	})

	ctx := context.Background()
	_, err := client.Tables.Get(ctx, "test_db", "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestTablesService_Create(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/create/test_db/new_table/log", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/table/create/test_db/new_table/log")

		fmt.Fprint(w, `{
			"database": "test_db",
			"table": "new_table",
			"type": "log"
		}`)
	})

	ctx := context.Background()
	resp, err := client.Tables.Create(ctx, "test_db", "new_table", "log")
	if err != nil {
		t.Errorf("Tables.Create returned error: %v", err)
	}

	want := &TableCreateResponse{
		Database: "test_db",
		Table:    "new_table",
		Type:     "log",
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Tables.Create returned %+v, want %+v", resp, want)
	}
}

func TestTablesService_Create_DefaultType(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/create/test_db/new_table/log", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/table/create/test_db/new_table/log")

		fmt.Fprint(w, `{
			"database": "test_db",
			"table": "new_table",
			"type": "log"
		}`)
	})

	ctx := context.Background()
	// Test default type (empty string should default to "log")
	_, err := client.Tables.Create(ctx, "test_db", "new_table", "")
	if err != nil {
		t.Errorf("Tables.Create returned error: %v", err)
	}
}

func TestTablesService_Create_AlreadyExists(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/create/test_db/existing_table/log", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"error": "Table already exists"}`)
	})

	ctx := context.Background()
	_, err := client.Tables.Create(ctx, "test_db", "existing_table", "log")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code 409, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestTablesService_Delete(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/delete/test_db/old_table", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/table/delete/test_db/old_table")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"database": "test_db", "table": "old_table"}`)
	})

	ctx := context.Background()
	err := client.Tables.Delete(ctx, "test_db", "old_table")
	if err != nil {
		t.Errorf("Tables.Delete returned error: %v", err)
	}
}

func TestTablesService_Delete_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/delete/test_db/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Table not found"}`)
	})

	ctx := context.Background()
	err := client.Tables.Delete(ctx, "test_db", "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestTablesService_Swap(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/swap/test_db/table1/table2", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/table/swap/test_db/table1/table2")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"database": "test_db"}`)
	})

	ctx := context.Background()
	err := client.Tables.Swap(ctx, "test_db", "table1", "table2")
	if err != nil {
		t.Errorf("Tables.Swap returned error: %v", err)
	}
}

func TestTablesService_Swap_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/swap/test_db/table1/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Table not found"}`)
	})

	ctx := context.Background()
	err := client.Tables.Swap(ctx, "test_db", "table1", "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}
}

func TestTablesService_Rename(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/rename/test_db/old_name/new_name", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/table/rename/test_db/old_name/new_name")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"database": "test_db", "table": "new_name"}`)
	})

	ctx := context.Background()
	err := client.Tables.Rename(ctx, "test_db", "old_name", "new_name")
	if err != nil {
		t.Errorf("Tables.Rename returned error: %v", err)
	}
}

func TestTablesService_Rename_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/rename/test_db/nonexistent/new_name", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Table not found"}`)
	})

	ctx := context.Background()
	err := client.Tables.Rename(ctx, "test_db", "nonexistent", "new_name")
	if err == nil {
		t.Error("Expected error to be returned")
	}
}

func TestTablesService_Update(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/update/test_db/events", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/table/update/test_db/events")

		var body UpdateOptions
		json.NewDecoder(r.Body).Decode(&body)

		expectedSchema := "[{\"name\":\"user_id\",\"type\":\"string\"}]"
		if body.Schema != expectedSchema {
			t.Errorf("Request body schema = %v, want %v", body.Schema, expectedSchema)
		}

		expectedExpire := 365
		if body.ExpireDays == nil || *body.ExpireDays != expectedExpire {
			t.Errorf("Request body expire_days = %v, want %v", body.ExpireDays, &expectedExpire)
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"database": "test_db", "table": "events"}`)
	})

	ctx := context.Background()
	expireDays := 365
	opts := &UpdateOptions{
		Schema:     "[{\"name\":\"user_id\",\"type\":\"string\"}]",
		ExpireDays: &expireDays,
	}

	err := client.Tables.Update(ctx, "test_db", "events", opts)
	if err != nil {
		t.Errorf("Tables.Update returned error: %v", err)
	}
}

func TestTablesService_Update_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/table/update/test_db/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Table not found"}`)
	})

	ctx := context.Background()
	opts := &UpdateOptions{Schema: "[]"}
	err := client.Tables.Update(ctx, "test_db", "nonexistent", opts)
	if err == nil {
		t.Error("Expected error to be returned")
	}
}

// Test FlexibleInt64 JSON marshaling/unmarshaling

func TestFlexibleInt64_UnmarshalJSON_Int(t *testing.T) {
	var f FlexibleInt64
	err := f.UnmarshalJSON([]byte("12345"))
	if err != nil {
		t.Errorf("UnmarshalJSON returned error: %v", err)
	}
	if f.Value == nil || *f.Value != 12345 {
		t.Errorf("Expected 12345, got %v", f.Value)
	}
}

func TestFlexibleInt64_UnmarshalJSON_String(t *testing.T) {
	var f FlexibleInt64
	err := f.UnmarshalJSON([]byte("\"67890\""))
	if err != nil {
		t.Errorf("UnmarshalJSON returned error: %v", err)
	}
	if f.Value == nil || *f.Value != 67890 {
		t.Errorf("Expected 67890, got %v", f.Value)
	}
}

func TestFlexibleInt64_UnmarshalJSON_Null(t *testing.T) {
	var f FlexibleInt64
	err := f.UnmarshalJSON([]byte("null"))
	if err != nil {
		t.Errorf("UnmarshalJSON returned error: %v", err)
	}
	if f.Value != nil {
		t.Errorf("Expected nil, got %v", f.Value)
	}
}

func TestFlexibleInt64_MarshalJSON(t *testing.T) {
	val := int64(12345)
	f := FlexibleInt64{Value: &val}
	data, err := f.MarshalJSON()
	if err != nil {
		t.Errorf("MarshalJSON returned error: %v", err)
	}
	expected := "12345"
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, string(data))
	}
}

func TestFlexibleInt64_MarshalJSON_Null(t *testing.T) {
	f := FlexibleInt64{Value: nil}
	data, err := f.MarshalJSON()
	if err != nil {
		t.Errorf("MarshalJSON returned error: %v", err)
	}
	expected := "null"
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, string(data))
	}
}

// Example tests demonstrating common table operations

func ExampleTablesService_List() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// List all tables in a database
	tables, err := client.Tables.List(ctx, "my_database")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, table := range tables {
		fmt.Printf("Table: %s (Type: %s, Count: %d)\n", table.Name, table.Type, table.Count)
	}
}

func ExampleTablesService_Create() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Create a new log table
	resp, err := client.Tables.Create(ctx, "my_database", "events", "log")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Created table: %s.%s (Type: %s)\n", resp.Database, resp.Table, resp.Type)
}

func ExampleTablesService_Swap() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Swap contents of two tables
	err := client.Tables.Swap(ctx, "my_database", "events_staging", "events")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Tables swapped successfully")
}