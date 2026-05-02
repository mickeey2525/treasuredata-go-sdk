package treasuredata

import (
	"context"
	"fmt"
	"net/http"
)

// PostbackService handles communication with the Postback API.
type PostbackService struct {
	client *Client
}

// PostbackEvent represents a single event to be sent via the Postback API
type PostbackEvent struct {
	// Required identifier fields
	TDClientID string `json:"td_client_id,omitempty"`
	Email      string `json:"email,omitempty"`

	// Common event fields
	TDURL         string `json:"td_url,omitempty"`
	TDPath        string `json:"td_path,omitempty"`
	TDHost        string `json:"td_host,omitempty"`
	TDReferrer    string `json:"td_referrer,omitempty"`
	TDTITLE       string `json:"td_title,omitempty"`
	TDDescription string `json:"td_description,omitempty"`
	TDIp          string `json:"td_ip,omitempty"`
	TDUserAgent   string `json:"td_user_agent,omitempty"`

	// Custom fields can be added dynamically via map
}

// PostbackResponse represents the response from the Postback API
type PostbackResponse struct {
	Status string `json:"status,omitempty"`
}

// SendEvent sends a single event to the Postback API
func (s *PostbackService) SendEvent(ctx context.Context, database, table string, event interface{}) error {
	u := fmt.Sprintf("postback/v3/event/%s/%s", database, table)

	req, err := s.client.NewPostbackRequest("POST", u, event)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("postback API returned status %d", resp.StatusCode)
	}

	return nil
}

// SendEvents sends multiple events to the Postback API
func (s *PostbackService) SendEvents(ctx context.Context, database, table string, events []interface{}) error {
	u := fmt.Sprintf("postback/v3/event/%s/%s", database, table)

	req, err := s.client.NewPostbackRequest("POST", u, events)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(ctx, req, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("postback API returned status %d", resp.StatusCode)
	}

	return nil
}
