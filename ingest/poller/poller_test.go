package poller

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"

	"relay/internal/events"
	"relay/internal/salesforce"
)

// ---- fakes -----------------------------------------------------------------

// fakeSalesforce holds Closed Won rows and answers the poller's query the way
// Salesforce would: only rows inside the requested window, sorted.
type fakeSalesforce struct {
	now     time.Time
	rows    []salesforce.Opportunity
	queries []string
	fail    error
}

var windowRE = regexp.MustCompile(`SystemModstamp >= (\S+) AND SystemModstamp < (\S+) `)

func (f *fakeSalesforce) ServerTime(context.Context) (time.Time, error) { return f.now, nil }

func (f *fakeSalesforce) QueryAll(_ context.Context, soql string) ([]salesforce.Opportunity, error) {
	f.queries = append(f.queries, soql)
	if f.fail != nil {
		return nil, f.fail
	}
	m := windowRE.FindStringSubmatch(soql)
	if m == nil {
		return nil, fmt.Errorf("query has no window: %s", soql)
	}
	from, _ := salesforce.ParseDateTime(m[1])
	upper, _ := salesforce.ParseDateTime(m[2])
	var out []salesforce.Opportunity
	for _, r := range f.rows {
		s, _ := salesforce.ParseDateTime(r.SystemModstamp)
		if !s.Before(from) && s.Before(upper) {
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SystemModstamp != out[j].SystemModstamp {
			return out[i].SystemModstamp < out[j].SystemModstamp
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (f *fakeSalesforce) add(id string, stamp time.Time) {
	f.rows = append(f.rows, salesforce.Opportunity{
		ID: id, Name: id, StageName: "Closed Won",
		SystemModstamp: stamp.UTC().Format("2006-01-02T15:04:05.000-0700"),
	})
}

// fakeQueue records published event keys and can fail at a given index.
type fakeQueue struct {
	published []string
	failAt    int // -1 = never
}

func (q *fakeQueue) Publish(_ context.Context, evs []events.OpportunityEvent) (int, error) {
	for i, ev := range evs {
		if i == q.failAt {
			return i, errors.New("SQS said no")
		}
		q.published = append(q.published, ev.EventKey)
	}
	return len(evs), nil
}

type memWatermark struct{ t time.Time }

func (m *memWatermark) Get(context.Context) (time.Time, error) { return m.t, nil }
func (m *memWatermark) Set(_ context.Context, t time.Time) error {
	m.t = t
	return nil
}

var t0 = time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)

func setup(watermark time.Time) (*Poller, *fakeSalesforce, *fakeQueue, *memWatermark) {
	sf := &fakeSalesforce{now: t0}
	q := &fakeQueue{failAt: -1}
	wm := &memWatermark{t: watermark}
	return &Poller{Source: sf, Publisher: q, Watermark: wm, Lag: 2 * time.Minute}, sf, q, wm
}

func key(id string, stamp time.Time) string {
	return id + ":" + stamp.UTC().Format("2006-01-02T15:04:05.000Z")
}

// ---- tests -------------------------------------------------------------------

func TestQueryUsesIsWonAndAHalfOpenWindow(t *testing.T) {
	got := Query(time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC), time.Date(2026, 9, 22, 9, 58, 0, 0, time.UTC))
	want := "SELECT Id, Name, AccountId, Account.Name, Amount, CloseDate, StageName, SystemModstamp " +
		"FROM Opportunity WHERE IsWon = true AND SystemModstamp >= 2026-09-15T09:00:00Z " +
		"AND SystemModstamp < 2026-09-22T09:58:00Z ORDER BY SystemModstamp ASC, Id ASC"
	if got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
}

func TestWindowEndsLagBehindSalesforceClock(t *testing.T) {
	p, sf, q, wm := setup(t0.Add(-time.Hour))
	sf.add("006A", t0.Add(-10*time.Minute))
	res, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := t0.Add(-2 * time.Minute); !wm.t.Equal(want) || !res.To.Equal(want) {
		t.Errorf("watermark = %v, want %v (server time minus lag)", wm.t, want)
	}
	if len(q.published) != 1 {
		t.Errorf("published %v", q.published)
	}
}

// The original plan set the watermark to the newest published stamp and used
// >=, so the newest deal was re-sent on every poll. Tiled windows fix that.
func TestIdleOrgPublishesNothingOnTheNextPoll(t *testing.T) {
	p, sf, q, _ := setup(t0.Add(-time.Hour))
	sf.add("006A", t0.Add(-10*time.Minute))
	for i := 0; i < 5; i++ {
		if _, err := p.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		sf.now = sf.now.Add(2 * time.Minute) // the next scheduled run
	}
	if len(q.published) != 1 {
		t.Errorf("5 polls of an idle org published %d events, want 1: %v", len(q.published), q.published)
	}
}

// Two deals share a SystemModstamp. With >= on the lower bound and the
// window closing between runs, neither can be skipped or split.
func TestEqualTimestampsAreBothPublishedExactlyOnce(t *testing.T) {
	p, sf, q, _ := setup(t0.Add(-time.Hour))
	tie := t0.Add(-2 * time.Minute) // exactly at the first run's upper bound
	sf.add("006A", tie.Add(-time.Second))
	sf.add("006C", tie)
	sf.add("006B", tie)

	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{key("006A", tie.Add(-time.Second))}; fmt.Sprint(q.published) != fmt.Sprint(want) {
		t.Fatalf("run 1 published %v, want %v (rows at the upper bound wait for the next window)", q.published, want)
	}
	sf.now = sf.now.Add(2 * time.Minute)
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{key("006A", tie.Add(-time.Second)), key("006B", tie), key("006C", tie)}
	if fmt.Sprint(q.published) != fmt.Sprint(want) {
		t.Errorf("published %v, want %v", q.published, want)
	}
}

// A row stamped inside the lag zone may belong to a transaction that hasn't
// committed yet, so the poller leaves it for a later run.
func TestRowInsideTheLagZoneWaitsForALaterRun(t *testing.T) {
	p, sf, q, _ := setup(t0.Add(-time.Hour))
	sf.add("006A", t0.Add(-30*time.Second))
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(q.published) != 0 {
		t.Fatalf("published too early: %v", q.published)
	}
	sf.now = sf.now.Add(2 * time.Minute)
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(q.published) != 1 {
		t.Errorf("published %v, want the row once it is older than the lag", q.published)
	}
}

// The limit of the design: a transaction slower than the lag commits a row
// stamped behind the watermark. The poller never looks back, so the nightly
// reconciler is what repairs this.
func TestCommitSlowerThanTheLagIsLeftForTheReconciler(t *testing.T) {
	p, sf, q, _ := setup(t0.Add(-time.Hour))
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	sf.add("006LATE", t0.Add(-5*time.Minute)) // behind the new watermark (t0-2m)
	sf.now = sf.now.Add(2 * time.Minute)
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(q.published) != 0 {
		t.Errorf("published %v", q.published)
	}
}

func TestPartialPublishFailureNeverSkipsAnUnsentEvent(t *testing.T) {
	p, sf, q, wm := setup(t0.Add(-time.Hour))
	var stamps []time.Time
	for i := 0; i < 10; i++ {
		s := t0.Add(time.Duration(-50+i) * time.Minute)
		stamps = append(stamps, s)
		sf.add(fmt.Sprintf("006%02d", i), s)
	}
	q.failAt = 3
	_, err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected the run to fail")
	}
	if !wm.t.Equal(stamps[3]) {
		t.Fatalf("watermark = %v, want %v (the first unsent event)", wm.t, stamps[3])
	}

	q.failAt = -1
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(q.published) != 10 {
		t.Errorf("published %d events in total, want all 10 exactly once: %v", len(q.published), q.published)
	}
}

func TestPartialFailureInsideATieRepublishesTheTie(t *testing.T) {
	p, sf, q, wm := setup(t0.Add(-time.Hour))
	tie := t0.Add(-30 * time.Minute)
	sf.add("006A", tie)
	sf.add("006B", tie)
	q.failAt = 1
	if _, err := p.Run(context.Background()); err == nil {
		t.Fatal("expected the run to fail")
	}
	if !wm.t.Equal(tie) {
		t.Fatalf("watermark = %v, want the tie's stamp %v", wm.t, tie)
	}
	q.failAt = -1
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 006A goes out twice. That is the price of never skipping 006B, and the
	// ERP's conditional write makes the second copy a no-op.
	want := []string{key("006A", tie), key("006A", tie), key("006B", tie)}
	if fmt.Sprint(q.published) != fmt.Sprint(want) {
		t.Errorf("published %v, want %v", q.published, want)
	}
}

func TestFailureOnTheFirstEventLeavesTheWatermark(t *testing.T) {
	start := t0.Add(-time.Hour)
	p, sf, q, wm := setup(start)
	sf.add("006A", t0.Add(-30*time.Minute))
	q.failAt = 0
	if _, err := p.Run(context.Background()); err == nil {
		t.Fatal("expected the run to fail")
	}
	if !wm.t.Equal(start) {
		t.Errorf("watermark moved to %v", wm.t)
	}
}

func TestSalesforceErrorLeavesTheWatermark(t *testing.T) {
	start := t0.Add(-time.Hour)
	p, sf, _, wm := setup(start)
	sf.fail = &salesforce.AuthError{Status: 400, Body: "invalid_client"}
	_, err := p.Run(context.Background())
	var authErr *salesforce.AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("want the AuthError to reach the handler, got %v", err)
	}
	if !wm.t.Equal(start) {
		t.Errorf("watermark moved to %v", wm.t)
	}
}

func TestNoQueryWhenTheWindowIsEmpty(t *testing.T) {
	p, sf, _, wm := setup(t0.Add(-time.Minute)) // already ahead of now-lag
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sf.queries) != 0 {
		t.Errorf("queried Salesforce for an empty window: %v", sf.queries)
	}
	if !wm.t.Equal(t0.Add(-time.Minute)) {
		t.Errorf("watermark moved backwards to %v", wm.t)
	}
}

func TestUnparseableRowFailsTheRunWithoutAdvancing(t *testing.T) {
	start := t0.Add(-time.Hour)
	p, sf, q, wm := setup(start)
	p.Source = &badRowSource{fakeSalesforce: sf}
	if _, err := p.Run(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	if !wm.t.Equal(start) || len(q.published) != 0 {
		t.Errorf("watermark %v, published %v", wm.t, q.published)
	}
}

// badRowSource returns a row whose SystemModstamp can't be parsed.
type badRowSource struct{ *fakeSalesforce }

func (b *badRowSource) QueryAll(context.Context, string) ([]salesforce.Opportunity, error) {
	return []salesforce.Opportunity{
		{ID: "006A", SystemModstamp: "2026-09-22T09:30:00.000+0000"},
		{ID: "006B", SystemModstamp: "garbage"},
	}, nil
}
