package events

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"relay/internal/salesforce"
)

// The Go builder matches the shared golden file. tests/contract/test_event_contract.py
// checks the Python builder (used by the reconciler) against the same file.
func TestGoBuilderMatchesGoldenFile(t *testing.T) {
	raw, err := os.ReadFile("../../tests/contract/fixtures/event_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Cases []struct {
			PublishedAt   string          `json:"published_at"`
			SalesforceRow json.RawMessage `json:"salesforce_row"`
			Event         map[string]any  `json:"event"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range golden.Cases {
		var row salesforce.Opportunity
		if err := json.Unmarshal(c.SalesforceRow, &row); err != nil {
			t.Fatal(err)
		}
		published, err := time.Parse(time.RFC3339, c.PublishedAt)
		if err != nil {
			t.Fatal(err)
		}
		ev, err := FromOpportunity(row, published)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, c.Event) {
			t.Errorf("%s:\n got %v\nwant %v", row.ID, got, c.Event)
		}
	}
}
