package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// PersonalizationService handles communication with the Personalization API.
type PersonalizationService struct {
	client *Client
}

// PersonalizationEvent represents an event sent to the personalization API
type PersonalizationEvent struct {
	TDClientID      string   `json:"td_client_id,omitempty"`
	TDURL           string   `json:"td_url,omitempty"`
	TDPath          string   `json:"td_path,omitempty"`
	Email           string   `json:"email,omitempty"`
	ProductName     string   `json:"product_name,omitempty"`
	ProductCategory string   `json:"product_category,omitempty"`
	ProductList     []string `json:"product_list,omitempty"`
	CategoryList    []string `json:"category_list,omitempty"`
	// Additional attributes can be added dynamically
}

// PersonalizationToken represents a personalization token configuration
type PersonalizationToken struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Type        string   `json:"type"`
	Status      string   `json:"status"`
	Scopes      []string `json:"scopes,omitempty"`
	CreatedAt   TDTime   `json:"created_at"`
	UpdatedAt   TDTime   `json:"updated_at"`
}

// PersonalizationOffer represents an offer returned by the personalization API
type PersonalizationOffer struct {
	Attributes    map[string]interface{}        `json:"attributes,omitempty"`
	BatchSegments []PersonalizationBatchSegment `json:"batch_segments,omitempty"`
}

// PersonalizationBatchSegment represents a batch segment in an offer
type PersonalizationBatchSegment struct {
	ID string `json:"id"`
}

// PersonalizationResponse represents the response from the personalization API
type PersonalizationResponse struct {
	Offers map[string]PersonalizationOffer `json:"offers,omitempty"`
}

// SendEvent sends a personalization event and returns matching offers
func (s *PersonalizationService) SendEvent(ctx context.Context, database, table string, event map[string]interface{}) (*PersonalizationResponse, error) {
	u := fmt.Sprintf("%s/%s", database, table)

	req, err := s.client.NewPersonalizationRequest("POST", u, event)
	if err != nil {
		return nil, err
	}

	var resp PersonalizationResponse
	httpResp, err := s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("personalization API returned status %d", httpResp.StatusCode)
	}

	return &resp, nil
}

// SendEventWithToken sends a personalization event with a specific token
func (s *PersonalizationService) SendEventWithToken(ctx context.Context, database, table, token string, event map[string]interface{}) (*PersonalizationResponse, error) {
	u := fmt.Sprintf("%s/%s", database, table)

	req, err := s.client.NewPersonalizationRequest("POST", u, event)
	if err != nil {
		return nil, err
	}

	// Add WP13n-Token header
	req.Header.Set("WP13n-Token", token)

	var resp PersonalizationResponse
	httpResp, err := s.client.Do(ctx, req, &resp)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("personalization API returned status %d", httpResp.StatusCode)
	}

	return &resp, nil
}
