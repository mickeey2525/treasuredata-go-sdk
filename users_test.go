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

func TestUsersService_List(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/list", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/user/list")

		fmt.Fprint(w, `{
			"users": [
				{
					"id": 12345,
					"name": "Alice Smith",
					"email": "alice@example.com",
					"account_id": 100,
					"created_at": 1609459200,
					"updated_at": 1609545600,
					"gravatar_url": "https://www.gravatar.com/avatar/abc123",
					"administrator": true,
					"me": true,
					"restricted": false,
					"email_verified": true
				},
				{
					"id": 12346,
					"name": "Bob Jones",
					"email": "bob@example.com",
					"account_id": 100,
					"created_at": 1609459800,
					"updated_at": 1609632000,
					"gravatar_url": "https://www.gravatar.com/avatar/def456",
					"administrator": false,
					"me": false,
					"restricted": true,
					"email_verified": false
				}
			]
		}`)
	})

	ctx := context.Background()
	users, err := client.Users.List(ctx)
	if err != nil {
		t.Errorf("Users.List returned error: %v", err)
	}

	want := []User{
		{
			ID:            12345,
			Name:          "Alice Smith",
			Email:         "alice@example.com",
			AccountID:     100,
			CreatedAt:     TDTime{time.Unix(1609459200, 0)},
			UpdatedAt:     TDTime{time.Unix(1609545600, 0)},
			GravatarURL:   "https://www.gravatar.com/avatar/abc123",
			Administrator: true,
			Me:            true,
			Restricted:    false,
			EmailVerified: true,
		},
		{
			ID:            12346,
			Name:          "Bob Jones",
			Email:         "bob@example.com",
			AccountID:     100,
			CreatedAt:     TDTime{time.Unix(1609459800, 0)},
			UpdatedAt:     TDTime{time.Unix(1609632000, 0)},
			GravatarURL:   "https://www.gravatar.com/avatar/def456",
			Administrator: false,
			Me:            false,
			Restricted:    true,
			EmailVerified: false,
		},
	}

	if !reflect.DeepEqual(users, want) {
		t.Errorf("Users.List returned %+v, want %+v", users, want)
	}
}

func TestUsersService_List_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/list", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"error": "Access denied - insufficient permissions"}`)
	})

	ctx := context.Background()
	_, err := client.Users.List(ctx)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusForbidden {
			t.Errorf("Expected status code 403, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestUsersService_Get(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/show/alice@example.com", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/user/show/alice@example.com")

		fmt.Fprint(w, `{
			"id": 12345,
			"name": "Alice Smith",
			"email": "alice@example.com",
			"account_id": 100,
			"created_at": 1609459200,
			"updated_at": 1609545600,
			"gravatar_url": "https://www.gravatar.com/avatar/abc123",
			"administrator": true,
			"me": false,
			"restricted": false,
			"email_verified": true
		}`)
	})

	ctx := context.Background()
	user, err := client.Users.Get(ctx, "alice@example.com")
	if err != nil {
		t.Errorf("Users.Get returned error: %v", err)
	}

	want := &User{
		ID:            12345,
		Name:          "Alice Smith",
		Email:         "alice@example.com",
		AccountID:     100,
		CreatedAt:     TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:     TDTime{time.Unix(1609545600, 0)},
		GravatarURL:   "https://www.gravatar.com/avatar/abc123",
		Administrator: true,
		Me:            false,
		Restricted:    false,
		EmailVerified: true,
	}

	if !reflect.DeepEqual(user, want) {
		t.Errorf("Users.Get returned %+v, want %+v", user, want)
	}
}

func TestUsersService_Get_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/show/nonexistent@example.com", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "User not found"}`)
	})

	ctx := context.Background()
	_, err := client.Users.Get(ctx, "nonexistent@example.com")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestUsersService_Create(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/create", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/user/create")

		var body CreateUserOptions
		json.NewDecoder(r.Body).Decode(&body)

		if body.Email != "newuser@example.com" {
			t.Errorf("Request body email = %v, want 'newuser@example.com'", body.Email)
		}
		if body.Password != "secure123" {
			t.Errorf("Request body password = %v, want 'secure123'", body.Password)
		}
		if body.Name != "New User" {
			t.Errorf("Request body name = %v, want 'New User'", body.Name)
		}

		fmt.Fprint(w, `{
			"id": 12347,
			"name": "New User",
			"email": "newuser@example.com",
			"account_id": 100,
			"created_at": 1609632000,
			"updated_at": 1609632000,
			"gravatar_url": "https://www.gravatar.com/avatar/new123",
			"administrator": false,
			"me": false,
			"restricted": false,
			"email_verified": false
		}`)
	})

	ctx := context.Background()
	opts := &CreateUserOptions{
		Email:    "newuser@example.com",
		Password: "secure123",
		Name:     "New User",
	}

	user, err := client.Users.Create(ctx, opts)
	if err != nil {
		t.Errorf("Users.Create returned error: %v", err)
	}

	want := &User{
		ID:            12347,
		Name:          "New User",
		Email:         "newuser@example.com",
		AccountID:     100,
		CreatedAt:     TDTime{time.Unix(1609632000, 0)},
		UpdatedAt:     TDTime{time.Unix(1609632000, 0)},
		GravatarURL:   "https://www.gravatar.com/avatar/new123",
		Administrator: false,
		Me:            false,
		Restricted:    false,
		EmailVerified: false,
	}

	if !reflect.DeepEqual(user, want) {
		t.Errorf("Users.Create returned %+v, want %+v", user, want)
	}
}

func TestUsersService_Create_EmailAlreadyExists(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/create", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"error": "User with this email already exists"}`)
	})

	ctx := context.Background()
	opts := &CreateUserOptions{
		Email:    "existing@example.com",
		Password: "password123",
		Name:     "Existing User",
	}

	_, err := client.Users.Create(ctx, opts)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code 409, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestUsersService_Create_InvalidEmail(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/create", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error": "Invalid email format"}`)
	})

	ctx := context.Background()
	opts := &CreateUserOptions{
		Email:    "invalid-email",
		Password: "password123",
		Name:     "Test User",
	}

	_, err := client.Users.Create(ctx, opts)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status code 400, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestUsersService_Delete(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/delete/user@example.com", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/user/delete/user@example.com")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"message": "User deleted successfully"}`)
	})

	ctx := context.Background()
	err := client.Users.Delete(ctx, "user@example.com")
	if err != nil {
		t.Errorf("Users.Delete returned error: %v", err)
	}
}

func TestUsersService_Delete_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/delete/nonexistent@example.com", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "User not found"}`)
	})

	ctx := context.Background()
	err := client.Users.Delete(ctx, "nonexistent@example.com")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestUsersService_ListAPIKeys(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/apikey/list/alice@example.com", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/user/apikey/list/alice@example.com")

		fmt.Fprint(w, `{
			"apikeys": [
				{
					"key": "abc123def456ghi789",
					"type": "read_only",
					"created_at": 1609459200
				},
				{
					"key": "xyz987uvw654rst321",
					"type": "full_access",
					"created_at": 1609545600
				}
			]
		}`)
	})

	ctx := context.Background()
	apiKeys, err := client.Users.ListAPIKeys(ctx, "alice@example.com")
	if err != nil {
		t.Errorf("Users.ListAPIKeys returned error: %v", err)
	}

	want := []APIKey{
		{
			Key:       "abc123def456ghi789",
			Type:      "read_only",
			CreatedAt: TDTime{time.Unix(1609459200, 0)},
		},
		{
			Key:       "xyz987uvw654rst321",
			Type:      "full_access",
			CreatedAt: TDTime{time.Unix(1609545600, 0)},
		},
	}

	if !reflect.DeepEqual(apiKeys, want) {
		t.Errorf("Users.ListAPIKeys returned %+v, want %+v", apiKeys, want)
	}
}

func TestUsersService_ListAPIKeys_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/apikey/list/unauthorized@example.com", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error": "Unauthorized access"}`)
	})

	ctx := context.Background()
	_, err := client.Users.ListAPIKeys(ctx, "unauthorized@example.com")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusUnauthorized {
			t.Errorf("Expected status code 401, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestUsersService_AddAPIKey(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/apikey/add/alice@example.com", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/user/apikey/add/alice@example.com")

		fmt.Fprint(w, `{
			"key": "new123key456api789",
			"type": "full_access",
			"created_at": 1609632000
		}`)
	})

	ctx := context.Background()
	apiKey, err := client.Users.AddAPIKey(ctx, "alice@example.com")
	if err != nil {
		t.Errorf("Users.AddAPIKey returned error: %v", err)
	}

	want := &APIKey{
		Key:       "new123key456api789",
		Type:      "full_access",
		CreatedAt: TDTime{time.Unix(1609632000, 0)},
	}

	if !reflect.DeepEqual(apiKey, want) {
		t.Errorf("Users.AddAPIKey returned %+v, want %+v", apiKey, want)
	}
}

func TestUsersService_AddAPIKey_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/apikey/add/restricted@example.com", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"error": "Cannot add API key to restricted user"}`)
	})

	ctx := context.Background()
	_, err := client.Users.AddAPIKey(ctx, "restricted@example.com")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusForbidden {
			t.Errorf("Expected status code 403, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestUsersService_RemoveAPIKey(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/apikey/remove/alice@example.com/abc123def456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/user/apikey/remove/alice@example.com/abc123def456")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"message": "API key removed successfully"}`)
	})

	ctx := context.Background()
	err := client.Users.RemoveAPIKey(ctx, "alice@example.com", "abc123def456")
	if err != nil {
		t.Errorf("Users.RemoveAPIKey returned error: %v", err)
	}
}

func TestUsersService_RemoveAPIKey_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/apikey/remove/alice@example.com/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "API key not found"}`)
	})

	ctx := context.Background()
	err := client.Users.RemoveAPIKey(ctx, "alice@example.com", "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestUsersService_Create_MinimalOptions(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/user/create", func(w http.ResponseWriter, r *http.Request) {
		var body CreateUserOptions
		json.NewDecoder(r.Body).Decode(&body)

		// Should handle when Name is omitted
		if body.Email != "minimal@example.com" {
			t.Errorf("Request body email = %v, want 'minimal@example.com'", body.Email)
		}
		if body.Password != "password123" {
			t.Errorf("Request body password = %v, want 'password123'", body.Password)
		}
		if body.Name != "" {
			t.Errorf("Request body name = %v, want empty string", body.Name)
		}

		fmt.Fprint(w, `{
			"id": 12348,
			"name": "",
			"email": "minimal@example.com",
			"account_id": 100,
			"created_at": 1609632000,
			"updated_at": 1609632000,
			"gravatar_url": "",
			"administrator": false,
			"me": false,
			"restricted": false,
			"email_verified": false
		}`)
	})

	ctx := context.Background()
	opts := &CreateUserOptions{
		Email:    "minimal@example.com",
		Password: "password123",
		// Name omitted (should use omitempty)
	}

	_, err := client.Users.Create(ctx, opts)
	if err != nil {
		t.Errorf("Users.Create returned error: %v", err)
	}
}

// Example tests demonstrating common user operations

func ExampleUsersService_List() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// List all users in the account
	users, err := client.Users.List(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, user := range users {
		status := "Regular"
		if user.Administrator {
			status = "Administrator"
		} else if user.Restricted {
			status = "Restricted"
		}

		fmt.Printf("User: %s (%s) - %s\n", user.Name, user.Email, status)
	}
}

func ExampleUsersService_Create() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Create a new user
	opts := &CreateUserOptions{
		Email:    "newuser@company.com",
		Password: "secure-password-123",
		Name:     "John Doe",
	}

	user, err := client.Users.Create(ctx, opts)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Created user: %s (ID: %d)\n", user.Name, user.ID)
}

func ExampleUsersService_ListAPIKeys() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// List API keys for a user
	apiKeys, err := client.Users.ListAPIKeys(ctx, "user@company.com")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Found %d API keys:\n", len(apiKeys))
	for _, key := range apiKeys {
		fmt.Printf("Key: %s... (Type: %s)\n", key.Key[:8], key.Type)
	}
}
