package salesforce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseDateTime(t *testing.T) {
	want := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	for _, in := range []string{
		"2026-09-15T09:00:00.000+0000", // what the REST API returns
		"2026-09-15T14:30:00.000+0530", // same instant, other offset
		"2026-09-15T09:00:00Z",         // what Relay writes
		"2026-09-15T09:00:00.000Z",
	} {
		got, err := ParseDateTime(in)
		if err != nil || !got.Equal(want) {
			t.Errorf("ParseDateTime(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseDateTime("15/09/2026"); err == nil {
		t.Error("expected an error for a non-Salesforce datetime")
	}
}

func TestLiteralIsUnquotedUTCWholeSeconds(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	got := Literal(time.Date(2026, 9, 15, 14, 30, 0, 999, ist))
	if got != "2026-09-15T09:00:00Z" {
		t.Errorf("Literal = %q", got)
	}
}

// fakeOrg is a tiny Salesforce stand-in: a token endpoint, a two-page query,
// and the versions endpoint with a fixed Date header.
type fakeOrg struct {
	tokenCalls    atomic.Int32
	queryCalls    atomic.Int32
	rejectNextGET atomic.Bool // answer the next query with 401, like an expired session
	badSecret     bool
}

func (f *fakeOrg) server(t *testing.T) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/services/oauth2/token":
			f.tokenCalls.Add(1)
			if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" {
				http.Error(w, `{"error":"unsupported_grant_type"}`, http.StatusBadRequest)
				return
			}
			if f.badSecret || r.PostForm.Get("client_secret") != "s3cret" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"invalid client credentials"}`))
				return
			}
			_, _ = fmt.Fprintf(w, `{"access_token":"tok-%d","instance_url":%q,"token_type":"Bearer"}`,
				f.tokenCalls.Load(), srv.URL)
		case r.URL.Path == "/services/data/":
			w.Header().Set("Date", "Tue, 22 Sep 2026 10:00:07 GMT")
			_, _ = w.Write([]byte(`[{"version":"66.0"}]`))
		case strings.HasPrefix(r.URL.Path, "/services/data/v66.0/query"):
			f.queryCalls.Add(1)
			if f.rejectNextGET.CompareAndSwap(true, false) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`[{"message":"Session expired or invalid","errorCode":"INVALID_SESSION_ID"}]`))
				return
			}
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer tok-") {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.URL.Path == "/services/data/v66.0/query/01gX-1" {
				_, _ = w.Write([]byte(`{"totalSize":2,"done":true,"records":[
					{"attributes":{"type":"Opportunity"},"Id":"006B","Name":"B","AccountId":null,"Account":null,
					 "Amount":null,"CloseDate":"2026-09-02","StageName":"Closed Won","SystemModstamp":"2026-09-15T09:00:00.000+0000"}]}`))
				return
			}
			if r.URL.Query().Get("q") == "" {
				t.Errorf("query without q parameter: %s", r.URL)
			}
			_, _ = w.Write([]byte(`{"totalSize":2,"done":false,"nextRecordsUrl":"/services/data/v66.0/query/01gX-1","records":[
				{"attributes":{"type":"Opportunity"},"Id":"006A","Name":"A","AccountId":"001A",
				 "Account":{"attributes":{"type":"Account"},"Name":"Acme"},
				 "Amount":125000.0,"CloseDate":"2026-09-01","StageName":"Closed Won","SystemModstamp":"2026-09-15T09:00:00.000+0000"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newClient(url string) *Client {
	return &Client{LoginURL: url, APIVersion: "v66.0", ClientID: "id", ClientSecret: "s3cret"}
}

func TestQueryAllFollowsNextRecordsURL(t *testing.T) {
	org := &fakeOrg{}
	c := newClient(org.server(t).URL)
	rows, err := c.QueryAll(context.Background(), "SELECT Id FROM Opportunity")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ID != "006A" || rows[1].ID != "006B" {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0].Amount == nil || rows[0].Amount.String() != "125000.0" {
		t.Errorf("amount should keep its exact decimal text, got %v", rows[0].Amount)
	}
	if rows[0].Account == nil || rows[0].Account.Name != "Acme" {
		t.Errorf("account = %+v", rows[0].Account)
	}
	if rows[1].Amount != nil || rows[1].Account != nil || rows[1].AccountID != nil {
		t.Errorf("nulls must stay nil: %+v", rows[1])
	}
	if got := org.queryCalls.Load(); got != 2 {
		t.Errorf("query calls = %d, want 2 (one per page)", got)
	}
}

func TestTokenIsCachedAndRefreshedOnceOn401(t *testing.T) {
	org := &fakeOrg{}
	c := newClient(org.server(t).URL)
	ctx := context.Background()
	if _, err := c.QueryAll(ctx, "q"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.QueryAll(ctx, "q"); err != nil {
		t.Fatal(err)
	}
	if got := org.tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want 1 (cached)", got)
	}
	org.rejectNextGET.Store(true) // the session expires
	if _, err := c.QueryAll(ctx, "q"); err != nil {
		t.Fatalf("expected a transparent refresh, got %v", err)
	}
	if got := org.tokenCalls.Load(); got != 2 {
		t.Errorf("token calls = %d, want 2 after one refresh", got)
	}
}

func TestBadCredentialsReturnAuthErrorWithoutTheSecret(t *testing.T) {
	org := &fakeOrg{badSecret: true}
	c := newClient(org.server(t).URL)
	_, err := c.QueryAll(context.Background(), "q")
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("want *AuthError, got %T %v", err, err)
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Error("the error message must never contain the client secret")
	}
}

func TestServerTimeComesFromSalesforceDateHeader(t *testing.T) {
	org := &fakeOrg{}
	c := newClient(org.server(t).URL)
	got, err := c.ServerTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 9, 22, 10, 0, 7, 0, time.UTC); !got.Equal(want) {
		t.Errorf("ServerTime = %v, want %v", got, want)
	}
}
