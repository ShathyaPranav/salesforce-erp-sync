// Package salesforce is a minimal client for the two Salesforce APIs Relay
// uses: the OAuth 2.0 client credentials token endpoint and the REST query
// endpoint (with nextRecordsUrl paging).
//
// It never logs or returns the client secret or the access token.
package salesforce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Opportunity is one row of the ingest query, in Salesforce's JSON shape.
// Nullable fields are pointers so "missing in Salesforce" survives as null.
type Opportunity struct {
	ID             string       `json:"Id"`
	Name           string       `json:"Name"`
	AccountID      *string      `json:"AccountId"`
	Account        *Account     `json:"Account"`
	Amount         *json.Number `json:"Amount"`
	CloseDate      *string      `json:"CloseDate"`
	StageName      string       `json:"StageName"`
	SystemModstamp string       `json:"SystemModstamp"`
}

// Account is the parent relationship selected as Account.Name.
type Account struct {
	Name string `json:"Name"`
}

// AuthError means Salesforce rejected the client credentials. Retrying will
// not help until someone fixes the External Client App or the secret.
type AuthError struct {
	Status int
	Body   string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("salesforce token request failed: HTTP %d %s", e.Status, e.Body)
}

// APIError is any non-2xx response from the REST API.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("salesforce API error: HTTP %d %s", e.Status, e.Body)
}

// Client talks to one org. LoginURL must be the org's My Domain URL: the
// client credentials flow is rejected on login.salesforce.com.
type Client struct {
	LoginURL     string
	APIVersion   string // e.g. "v66.0"
	ClientID     string
	ClientSecret string
	HTTP         *http.Client

	mu    sync.Mutex
	token *token
}

type token struct {
	AccessToken string `json:"access_token"`
	InstanceURL string `json:"instance_url"`
}

type queryPage struct {
	TotalSize      int           `json:"totalSize"`
	Done           bool          `json:"done"`
	NextRecordsURL string        `json:"nextRecordsUrl"`
	Records        []Opportunity `json:"records"`
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// ServerTime returns Salesforce's clock, read from the Date header of the
// versions endpoint (unauthenticated, and it doesn't count against API
// limits). The poller uses it instead of the Lambda's clock, so the poll
// window is always measured on the same clock that stamps SystemModstamp.
func (c *Client) ServerTime(ctx context.Context) (time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.LoginURL, "/")+"/services/data/", nil)
	if err != nil {
		return time.Time{}, err
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("salesforce server time: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return time.Time{}, &APIError{Status: resp.StatusCode}
	}
	t, err := http.ParseTime(resp.Header.Get("Date"))
	if err != nil {
		return time.Time{}, fmt.Errorf("salesforce server time: bad Date header: %w", err)
	}
	return t.UTC(), nil
}

// QueryAll runs a SOQL query and follows nextRecordsUrl until the last page.
// A cached token is reused; on 401 it fetches a new one and retries once.
func (c *Client) QueryAll(ctx context.Context, soql string) ([]Opportunity, error) {
	path := fmt.Sprintf("/services/data/%s/query?q=%s", c.APIVersion, url.QueryEscape(soql))
	var all []Opportunity
	for {
		var page queryPage
		if err := c.getJSON(ctx, path, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Records...)
		if page.Done || page.NextRecordsURL == "" {
			return all, nil
		}
		path = page.NextRecordsURL
	}
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	for attempt := 0; ; attempt++ {
		tok, err := c.getToken(ctx, attempt > 0)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(tok.InstanceURL, "/")+path, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
		req.Header.Set("Accept", "application/json")
		resp, err := c.httpClient().Do(req)
		if err != nil {
			return fmt.Errorf("salesforce query: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("salesforce query: %w", readErr)
		}
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			continue // token expired or revoked: get a new one and retry once
		}
		if resp.StatusCode != http.StatusOK {
			return &APIError{Status: resp.StatusCode, Body: truncate(string(body))}
		}
		return json.Unmarshal(body, out)
	}
}

func (c *Client) getToken(ctx context.Context, refresh bool) (*token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != nil && !refresh {
		return c.token, nil
	}
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.ClientID},
		"client_secret": {c.ClientSecret},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(c.LoginURL, "/")+"/services/oauth2/token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("salesforce token request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("salesforce token request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Salesforce's OAuth error body is {"error", "error_description"}: no secrets in it.
		return nil, &AuthError{Status: resp.StatusCode, Body: truncate(string(body))}
	}
	var tok token
	if err := json.Unmarshal(body, &tok); err != nil {
		return nil, fmt.Errorf("salesforce token response: %w", err)
	}
	if tok.AccessToken == "" || tok.InstanceURL == "" {
		return nil, errors.New("salesforce token response is missing access_token or instance_url")
	}
	c.token = &tok
	return c.token, nil
}

func truncate(s string) string {
	const limit = 500
	if len(s) > limit {
		return s[:limit] + "..."
	}
	return s
}
