package treasuredata

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestDatabasesService_List(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/list", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/database/list")

		fmt.Fprint(w, `{
			"databases": [
				{
					"name": "test_db",
					"created_at": 1609459200,
					"updated_at": 1609459200,
					"count": 1000,
					"organization": "test_org",
					"permission": "full_access",
					"delete_protected": false
				},
				{
					"name": "prod_db",
					"created_at": 1609545600,
					"updated_at": 1609632000,
					"count": 50000,
					"permission": "query_only",
					"delete_protected": true
				}
			]
		}`)
	})

	ctx := context.Background()
	databases, err := client.Databases.List(ctx)
	if err != nil {
		t.Errorf("Databases.List returned error: %v", err)
	}

	orgPtr := "test_org"
	want := []Database{
		{
			Name:            "test_db",
			CreatedAt:       TDTime{time.Unix(1609459200, 0)},
			UpdatedAt:       TDTime{time.Unix(1609459200, 0)},
			Count:           1000,
			Organization:    &orgPtr,
			Permission:      "full_access",
			DeleteProtected: false,
		},
		{
			Name:            "prod_db",
			CreatedAt:       TDTime{time.Unix(1609545600, 0)},
			UpdatedAt:       TDTime{time.Unix(1609632000, 0)},
			Count:           50000,
			Organization:    nil,
			Permission:      "query_only",
			DeleteProtected: true,
		},
	}

	if !reflect.DeepEqual(databases, want) {
		t.Errorf("Databases.List returned %+v, want %+v", databases, want)
	}
}

func TestDatabasesService_List_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/list", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error": "Invalid API key"}`)
	})

	ctx := context.Background()
	_, err := client.Databases.List(ctx)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusUnauthorized {
			t.Errorf("Expected status code 401, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestDatabasesService_Get(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/show/test_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/database/show/test_db")

		fmt.Fprint(w, `{
			"name": "test_db",
			"created_at": 1609459200,
			"updated_at": 1609459200,
			"count": 1000,
			"organization": "test_org",
			"permission": "full_access",
			"delete_protected": false
		}`)
	})

	ctx := context.Background()
	database, err := client.Databases.Get(ctx, "test_db")
	if err != nil {
		t.Errorf("Databases.Get returned error: %v", err)
	}

	orgPtr := "test_org"
	want := &Database{
		Name:            "test_db",
		CreatedAt:       TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:       TDTime{time.Unix(1609459200, 0)},
		Count:           1000,
		Organization:    &orgPtr,
		Permission:      "full_access",
		DeleteProtected: false,
	}

	if !reflect.DeepEqual(database, want) {
		t.Errorf("Databases.Get returned %+v, want %+v", database, want)
	}
}

func TestDatabasesService_Get_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/show/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Database not found"}`)
	})

	ctx := context.Background()
	_, err := client.Databases.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	} else {
		t.Errorf("Expected ErrorResponse, got %T", err)
	}
}

func TestDatabasesService_Create(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/create/new_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/database/create/new_db")

		fmt.Fprint(w, `{
			"name": "new_db",
			"created_at": 1609459200,
			"updated_at": 1609459200,
			"count": 0,
			"permission": "full_access",
			"delete_protected": false
		}`)
	})

	ctx := context.Background()
	database, err := client.Databases.Create(ctx, "new_db")
	if err != nil {
		t.Errorf("Databases.Create returned error: %v", err)
	}

	want := &Database{
		Name:            "new_db",
		CreatedAt:       TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:       TDTime{time.Unix(1609459200, 0)},
		Count:           0,
		Permission:      "full_access",
		DeleteProtected: false,
	}

	if !reflect.DeepEqual(database, want) {
		t.Errorf("Databases.Create returned %+v, want %+v", database, want)
	}
}

func TestDatabasesService_Create_AlreadyExists(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/create/existing_db", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"error": "Database already exists"}`)
	})

	ctx := context.Background()
	_, err := client.Databases.Create(ctx, "existing_db")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code 409, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestDatabasesService_Delete(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/delete/test_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/database/delete/test_db")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"database": "test_db"}`)
	})

	ctx := context.Background()
	err := client.Databases.Delete(ctx, "test_db")
	if err != nil {
		t.Errorf("Databases.Delete returned error: %v", err)
	}
}

func TestDatabasesService_Delete_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/delete/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Database not found"}`)
	})

	ctx := context.Background()
	err := client.Databases.Delete(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestDatabasesService_Delete_Protected(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/database/delete/protected_db", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"error": "Database is delete protected"}`)
	})

	ctx := context.Background()
	err := client.Databases.Delete(ctx, "protected_db")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusForbidden {
			t.Errorf("Expected status code 403, got %d", tdErr.Response.StatusCode)
		}
	}
}

// Example tests demonstrating common database operations

func ExampleDatabasesService_List() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// List all databases
	databases, err := client.Databases.List(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, db := range databases {
		fmt.Printf("Database: %s (Count: %d, Permission: %s)\n", db.Name, db.Count, db.Permission)
	}
}

func ExampleDatabasesService_Get() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Get specific database
	database, err := client.Databases.Get(ctx, "my_database")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Database: %s, Records: %d\n", database.Name, database.Count)
}

func ExampleDatabasesService_Create() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Create a new database
	database, err := client.Databases.Create(ctx, "new_database")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Created database: %s\n", database.Name)
}
