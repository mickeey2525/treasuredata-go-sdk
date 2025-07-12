package treasuredata

import (
	"context"
	"fmt"
)

// UsersService handles communication with the user related methods of the Treasure Data API.
type UsersService struct {
	client *Client
}

// User represents a Treasure Data user
type User struct {
	ID            int    `json:"id"`
	Name          string `json:"name"`
	Email         string `json:"email"`
	AccountID     int    `json:"account_id"`
	CreatedAt     TDTime `json:"created_at"`
	UpdatedAt     TDTime `json:"updated_at"`
	GravatarURL   string `json:"gravatar_url"`
	Administrator bool   `json:"administrator"`
	Me            bool   `json:"me"`
	Restricted    bool   `json:"restricted"`
	EmailVerified bool   `json:"email_verified"`
}

// UserListResponse represents the response from listing users
type UserListResponse struct {
	Users []User `json:"users"`
}

// List returns all users
func (s *UsersService) List(ctx context.Context) ([]User, error) {
	u := fmt.Sprintf("%s/user/list", apiVersion)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp UserListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Users, nil
}

// Get returns a specific user by email
func (s *UsersService) Get(ctx context.Context, email string) (*User, error) {
	u := fmt.Sprintf("%s/user/show/%s", apiVersion, email)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var user User
	_, err = s.client.Do(ctx, req, &user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

// CreateUserOptions represents options for creating a user
type CreateUserOptions struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Name     string `json:"name,omitempty"`
}

// Create creates a new user
func (s *UsersService) Create(ctx context.Context, opts *CreateUserOptions) (*User, error) {
	u := fmt.Sprintf("%s/user/create", apiVersion)

	req, err := s.client.NewRequest("POST", u, opts)
	if err != nil {
		return nil, err
	}

	var user User
	_, err = s.client.Do(ctx, req, &user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

// Delete deletes a user by email
func (s *UsersService) Delete(ctx context.Context, email string) error {
	u := fmt.Sprintf("%s/user/delete/%s", apiVersion, email)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// APIKey represents an API key
type APIKey struct {
	Key       string `json:"key"`
	Type      string `json:"type"`
	CreatedAt TDTime `json:"created_at"`
}

// APIKeyListResponse represents the response from listing API keys
type APIKeyListResponse struct {
	APIKeys []APIKey `json:"apikeys"`
}

// ListAPIKeys lists API keys for a user
func (s *UsersService) ListAPIKeys(ctx context.Context, email string) ([]APIKey, error) {
	u := fmt.Sprintf("%s/user/apikey/list/%s", apiVersion, email)

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var resp APIKeyListResponse
	_, err = s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	return resp.APIKeys, nil
}

// AddAPIKey adds an API key to a user
func (s *UsersService) AddAPIKey(ctx context.Context, email string) (*APIKey, error) {
	u := fmt.Sprintf("%s/user/apikey/add/%s", apiVersion, email)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return nil, err
	}

	var apiKey APIKey
	_, err = s.client.Do(ctx, req, &apiKey)
	if err != nil {
		return nil, err
	}

	return &apiKey, nil
}

// RemoveAPIKey removes an API key from a user
func (s *UsersService) RemoveAPIKey(ctx context.Context, email, key string) error {
	u := fmt.Sprintf("%s/user/apikey/remove/%s/%s", apiVersion, email, key)

	req, err := s.client.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}
