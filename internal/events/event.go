// Package events defines the SQS message the ingest poller publishes and the
// Python worker consumes. docs/event-schema.md describes the same format for
// the Python side.
package events

import (
	"encoding/json"
	"fmt"
	"time"

	"relay/internal/salesforce"
)

// Schema names this message format. Bump it if a field changes meaning.
const Schema = "relay.opportunity.v1"

// modstampLayout is the normalised form used in event keys: UTC, millis, Z.
const modstampLayout = "2006-01-02T15:04:05.000Z"

// OpportunityEvent is one SQS message body.
type OpportunityEvent struct {
	Schema string `json:"schema"`
	// EventKey is OpportunityId:SystemModstamp. It identifies one version of
	// one deal and appears on every log line, so one query traces a deal end to end.
	EventKey string `json:"event_key"`
	// Version is SystemModstamp as epoch milliseconds. The ERP stores it and
	// only accepts a write whose version is newer than the stored one.
	Version     int64       `json:"version"`
	Opportunity Opportunity `json:"opportunity"`
	PublishedAt string      `json:"published_at"`

	modstamp time.Time
}

// Opportunity carries the deal fields the worker maps into the ERP. Nulls are
// kept as nulls: deciding that a missing Amount is an error is the worker's job.
type Opportunity struct {
	ID             string       `json:"id"`
	Name           string       `json:"name"`
	StageName      string       `json:"stage_name"`
	Amount         *json.Number `json:"amount"`
	CloseDate      *string      `json:"close_date"`
	AccountID      *string      `json:"account_id"`
	AccountName    *string      `json:"account_name"`
	SystemModstamp string       `json:"system_modstamp"`
}

// FromOpportunity builds the event for one Salesforce row.
func FromOpportunity(o salesforce.Opportunity, publishedAt time.Time) (OpportunityEvent, error) {
	stamp, err := salesforce.ParseDateTime(o.SystemModstamp)
	if err != nil {
		return OpportunityEvent{}, fmt.Errorf("opportunity %s: %w", o.ID, err)
	}
	var accountName *string
	if o.Account != nil {
		name := o.Account.Name
		accountName = &name
	}
	normalised := stamp.Format(modstampLayout)
	return OpportunityEvent{
		Schema:   Schema,
		EventKey: o.ID + ":" + normalised,
		Version:  stamp.UnixMilli(),
		Opportunity: Opportunity{
			ID:             o.ID,
			Name:           o.Name,
			StageName:      o.StageName,
			Amount:         o.Amount,
			CloseDate:      o.CloseDate,
			AccountID:      o.AccountID,
			AccountName:    accountName,
			SystemModstamp: normalised,
		},
		PublishedAt: publishedAt.UTC().Format(time.RFC3339),
		modstamp:    stamp,
	}, nil
}

// Modstamp is the event's SystemModstamp as a time.
func (e OpportunityEvent) Modstamp() time.Time { return e.modstamp }
