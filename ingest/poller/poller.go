// Package poller finds Closed Won deals that changed since the last run and
// publishes one event per deal version.
//
// The poll window is half-open, [watermark, upper):
//
//   - upper is Salesforce's clock minus Lag. SystemModstamp is stamped when a
//     record is written, not when its transaction commits, so the newest
//     seconds may still be missing rows that are about to appear. Staying Lag
//     behind gives those commits time to land.
//   - >= on the lower bound: a row stamped exactly at the watermark is never
//     skipped, even if it shares its second with rows from the last window.
//   - < on the upper bound plus watermark := upper: consecutive windows tile
//     time exactly, so an idle org publishes nothing.
package poller

import (
	"context"
	"fmt"
	"time"

	"relay/internal/events"
	"relay/internal/salesforce"
)

// Source is the Salesforce side.
type Source interface {
	ServerTime(ctx context.Context) (time.Time, error)
	QueryAll(ctx context.Context, soql string) ([]salesforce.Opportunity, error)
}

// Publisher sends events in order. It returns how many were sent before the
// first failure; if that is less than len(evs) it also returns an error.
type Publisher interface {
	Publish(ctx context.Context, evs []events.OpportunityEvent) (int, error)
}

// Watermark stores the lower bound of the next poll window.
type Watermark interface {
	Get(ctx context.Context) (time.Time, error)
	Set(ctx context.Context, t time.Time) error
}

// Poller runs one poll per Run call.
type Poller struct {
	Source    Source
	Publisher Publisher
	Watermark Watermark
	Lag       time.Duration
	Now       func() time.Time // for published_at; defaults to time.Now
}

// Result summarises one run, for the Lambda response and logs.
type Result struct {
	From      time.Time `json:"from"`
	To        time.Time `json:"to"`
	Found     int       `json:"found"`
	Published int       `json:"published"`
	Events    []string  `json:"-"`
}

// Query builds the SOQL for one window. IsWon is set by Salesforce from the
// stage's type, so it survives an admin renaming the "Closed Won" stage.
func Query(from, upper time.Time) string {
	return "SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate, StageName, SystemModstamp " +
		"FROM Opportunity WHERE IsWon = true" +
		" AND SystemModstamp >= " + salesforce.Literal(from) +
		" AND SystemModstamp < " + salesforce.Literal(upper) +
		" ORDER BY SystemModstamp ASC, Id ASC"
}

// Run polls one window and advances the watermark past what was published.
func (p *Poller) Run(ctx context.Context) (Result, error) {
	from, err := p.Watermark.Get(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("read watermark: %w", err)
	}
	serverNow, err := p.Source.ServerTime(ctx)
	if err != nil {
		return Result{From: from, To: from}, err
	}
	// Salesforce stamps whole seconds, and SOQL literals are whole seconds.
	upper := serverNow.Add(-p.Lag).Truncate(time.Second)
	if !upper.After(from) {
		return Result{From: from, To: from}, nil // the window is empty
	}

	rows, err := p.Source.QueryAll(ctx, Query(from, upper))
	if err != nil {
		return Result{From: from, To: from}, err
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	evs := make([]events.OpportunityEvent, 0, len(rows))
	for _, row := range rows {
		ev, err := events.FromOpportunity(row, now())
		if err != nil {
			return Result{From: from, To: from, Found: len(rows)}, err
		}
		evs = append(evs, ev)
	}

	sent, pubErr := p.Publisher.Publish(ctx, evs)
	res := Result{From: from, To: upper, Found: len(evs), Published: sent}
	for _, ev := range evs[:sent] {
		res.Events = append(res.Events, ev.EventKey)
	}
	if pubErr != nil {
		// Move the watermark only up to the first event that wasn't sent.
		// With >= the next run re-reads from exactly that record (and maybe a
		// few already-sent ones with the same stamp: harmless, the ERP write is
		// idempotent). Nothing unsent is ever skipped.
		res.To = from
		if sent > 0 && sent < len(evs) && evs[sent].Modstamp().After(from) {
			res.To = evs[sent].Modstamp()
			if err := p.Watermark.Set(ctx, res.To); err != nil {
				return res, fmt.Errorf("publish failed (%w) and watermark write failed: %w", pubErr, err)
			}
		}
		return res, fmt.Errorf("published %d of %d events: %w", sent, len(evs), pubErr)
	}
	if err := p.Watermark.Set(ctx, upper); err != nil {
		// Everything was published, so the worst case is a re-publish next time.
		return res, fmt.Errorf("write watermark: %w", err)
	}
	return res, nil
}
