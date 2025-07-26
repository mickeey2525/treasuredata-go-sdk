package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// ListTokens returns a list of tokens
func (s *CDPService) ListTokens(ctx context.Context, audienceID string, opts *CDPTokenListOptions) (*CDPTokenListResponse, error) {
	u := fmt.Sprintf("audiences/%s/tokens", audienceID)
	u, err := addOptions(u, opts)
	if err != nil {
		return nil, err
	}

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var tokens []CDPToken
	_, err = s.client.Do(ctx, req, &tokens)
	if err != nil {
		return nil, err
	}

	return &CDPTokenListResponse{
		Tokens: tokens,
		Total:  int64(len(tokens)),
	}, nil
}

// CreateToken creates a new token for an audience (legacy)
func (s *CDPService) CreateToken(ctx context.Context, audienceID string, req *CDPLegacyTokenRequest) (*CDPToken, error) {
	u := fmt.Sprintf("audiences/%s/tokens", audienceID)

	request, err := s.client.NewCDPRequest("POST", u, req)
	if err != nil {
		return nil, err
	}

	var token CDPToken
	_, err = s.client.Do(ctx, request, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// GetToken retrieves a specific token by ID from an audience (legacy)
func (s *CDPService) GetToken(ctx context.Context, audienceID, tokenID string) (*CDPToken, error) {
	u := fmt.Sprintf("audiences/%s/tokens/%s", audienceID, tokenID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var token CDPToken
	_, err = s.client.Do(ctx, req, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// UpdateToken updates an existing token for an audience (legacy)
func (s *CDPService) UpdateToken(ctx context.Context, audienceID, tokenID string, req *CDPLegacyTokenRequest) (*CDPToken, error) {
	u := fmt.Sprintf("audiences/%s/tokens/%s", audienceID, tokenID)

	request, err := s.client.NewCDPRequest("PUT", u, req)
	if err != nil {
		return nil, err
	}

	var token CDPToken
	_, err = s.client.Do(ctx, request, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// DeleteToken deletes a token from an audience (legacy)
func (s *CDPService) DeleteToken(ctx context.Context, audienceID, tokenID string) error {
	u := fmt.Sprintf("audiences/%s/tokens/%s", audienceID, tokenID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete token: %s", tokenID)
	}

	return nil
}

// Entity Token Operations

// CreateEntityToken creates a new entity token
func (s *CDPService) CreateEntityToken(ctx context.Context, req *CDPTokenCreateRequest) (*CDPToken, error) {
	u := "entities/tokens"

	request, err := s.client.NewCDPRequest("POST", u, req)
	if err != nil {
		return nil, err
	}

	var token CDPToken
	_, err = s.client.Do(ctx, request, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// GetEntityToken retrieves a specific entity token by ID
func (s *CDPService) GetEntityToken(ctx context.Context, tokenID string) (*CDPToken, error) {
	u := fmt.Sprintf("entities/tokens/%s", tokenID)

	req, err := s.client.NewCDPRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	var token CDPToken
	_, err = s.client.Do(ctx, req, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// UpdateEntityToken updates an existing entity token
func (s *CDPService) UpdateEntityToken(ctx context.Context, tokenID string, req *CDPTokenUpdateRequest) (*CDPToken, error) {
	u := fmt.Sprintf("entities/tokens/%s", tokenID)

	request, err := s.client.NewCDPRequest("PATCH", u, req)
	if err != nil {
		return nil, err
	}

	var token CDPToken
	_, err = s.client.Do(ctx, request, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

// DeleteEntityToken deletes an entity token
func (s *CDPService) DeleteEntityToken(ctx context.Context, tokenID string) error {
	u := fmt.Sprintf("entities/tokens/%s", tokenID)

	req, err := s.client.NewCDPRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete entity token: %s", tokenID)
	}

	return nil
}
