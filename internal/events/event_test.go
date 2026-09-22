package events

import (
	"encoding/json"
	"testing"
	"time"

	"relay/internal/salesforce"
)

func ptr[T any](v T) *T { return &v }

func TestEventKeyVersionAndShape(t *testing.T) {
	amount := json.Number("125000.0")
	opp := salesforce.Opportunity{
		ID: "006A", Name: "Acme - 50 robot arms", AccountID: ptr("001A"),
		Account: &salesforce.Account{Name: "Acme Robotics"}, Amount: &amount,
		CloseDate: ptr("2026-09-01"), StageName: "Closed Won",
		SystemModstamp: "2026-09-15T09:00:00.000+0000",
	}
	ev, err := FromOpportunity(opp, time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventKey != "006A:2026-09-15T09:00:00.000Z" {
		t.Errorf("event key = %q", ev.EventKey)
	}
	if want := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC).UnixMilli(); ev.Version != want {
		t.Errorf("version = %d, want %d", ev.Version, want)
	}

	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema":"relay.opportunity.v1","event_key":"006A:2026-09-15T09:00:00.000Z","version":1789462800000,` +
		`"opportunity":{"id":"006A","name":"Acme - 50 robot arms","stage_name":"Closed Won","amount":125000.0,` +
		`"close_date":"2026-09-01","account_id":"001A","account_name":"Acme Robotics",` +
		`"system_modstamp":"2026-09-15T09:00:00.000Z"},"published_at":"2026-09-22T10:00:00Z"}`
	if string(body) != want {
		t.Errorf("message body:\n got %s\nwant %s", body, want)
	}
}

func TestNullsArePublishedAsNullsForTheWorkerToJudge(t *testing.T) {
	opp := salesforce.Opportunity{ID: "006B", Name: "Orphan", StageName: "Closed Won",
		SystemModstamp: "2026-09-15T09:40:00.000+0000"}
	ev, err := FromOpportunity(opp, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Opportunity map[string]any `json:"opportunity"`
	}
	body, _ := json.Marshal(ev)
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"amount", "account_id", "account_name", "close_date"} {
		v, present := decoded.Opportunity[field]
		if !present || v != nil {
			t.Errorf("%s = %v (present=%v), want explicit null", field, v, present)
		}
	}
}

func TestSameInstantInAnotherOffsetGivesTheSameKey(t *testing.T) {
	a, _ := FromOpportunity(salesforce.Opportunity{ID: "006A", SystemModstamp: "2026-09-15T09:00:00.000+0000"}, time.Now())
	b, _ := FromOpportunity(salesforce.Opportunity{ID: "006A", SystemModstamp: "2026-09-15T14:30:00.000+0530"}, time.Now())
	if a.EventKey != b.EventKey || a.Version != b.Version {
		t.Errorf("%s/%d vs %s/%d", a.EventKey, a.Version, b.EventKey, b.Version)
	}
}

func TestBadModstampIsAnError(t *testing.T) {
	if _, err := FromOpportunity(salesforce.Opportunity{ID: "006A", SystemModstamp: "yesterday"}, time.Now()); err == nil {
		t.Error("expected an error")
	}
}
