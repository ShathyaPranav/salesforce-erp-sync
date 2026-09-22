package poller

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"

	"relay/internal/events"
	"relay/internal/salesforce"
)

// fakeSQS accepts batches and reports one global entry index as Failed, the
// way SendMessageBatch does: HTTP 200, err == nil, and a Failed list.
type fakeSQS struct {
	sent      []string
	batches   int
	failEntry int // global index across batches; -1 = none
}

func (f *fakeSQS) SendMessageBatch(_ context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if len(in.Entries) > 10 {
		return nil, fmt.Errorf("TooManyEntriesInBatchRequest: %d", len(in.Entries))
	}
	out := &sqs.SendMessageBatchOutput{}
	base := f.batches * 10
	f.batches++
	for i, e := range in.Entries {
		if base+i == f.failEntry {
			out.Failed = append(out.Failed, sqstypes.BatchResultErrorEntry{
				Id: e.Id, Code: aws.String("InternalError"), Message: aws.String("try again"), SenderFault: false,
			})
			continue
		}
		f.sent = append(f.sent, aws.ToString(e.MessageAttributes["event_key"].StringValue))
		out.Successful = append(out.Successful, sqstypes.SendMessageBatchResultEntry{Id: e.Id})
	}
	return out, nil
}

func makeEvents(t *testing.T, n int) []events.OpportunityEvent {
	t.Helper()
	evs := make([]events.OpportunityEvent, n)
	for i := range evs {
		ev, err := events.FromOpportunity(salesforce.Opportunity{
			ID: "006" + strconv.Itoa(i), SystemModstamp: "2026-09-15T09:00:00.000+0000",
		}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		evs[i] = ev
	}
	return evs
}

func TestPublisherSendsInBatchesOfTen(t *testing.T) {
	q := &fakeSQS{failEntry: -1}
	p := &SQSPublisher{Client: q, QueueURL: "q"}
	n, err := p.Publish(context.Background(), makeEvents(t, 23))
	if err != nil || n != 23 {
		t.Fatalf("Publish = %d, %v", n, err)
	}
	if q.batches != 3 || len(q.sent) != 23 {
		t.Errorf("batches %d, sent %d", q.batches, len(q.sent))
	}
}

func TestPublisherReportsAFailedEntryEvenWhenTheCallSucceeds(t *testing.T) {
	q := &fakeSQS{failEntry: 13} // 4th entry of the second batch
	p := &SQSPublisher{Client: q, QueueURL: "q"}
	n, err := p.Publish(context.Background(), makeEvents(t, 20))
	if err == nil {
		t.Fatal("a Failed entry must be an error even though SendMessageBatch returned nil")
	}
	if n != 13 {
		t.Errorf("sent-before-failure = %d, want 13", n)
	}
	if q.batches != 2 {
		t.Errorf("kept publishing after a failure: %d batches", q.batches)
	}
}

type fakeSSM struct{ params map[string]string }

func (f *fakeSSM) GetParameter(_ context.Context, in *ssm.GetParameterInput, _ ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
	v, ok := f.params[aws.ToString(in.Name)]
	if !ok {
		return nil, &ssmtypes.ParameterNotFound{Message: aws.String("not found")}
	}
	return &ssm.GetParameterOutput{Parameter: &ssmtypes.Parameter{Value: aws.String(v)}}, nil
}

func (f *fakeSSM) PutParameter(_ context.Context, in *ssm.PutParameterInput, _ ...func(*ssm.Options)) (*ssm.PutParameterOutput, error) {
	if in.Type != ssmtypes.ParameterTypeString || !aws.ToBool(in.Overwrite) {
		return nil, fmt.Errorf("unexpected put %+v", in)
	}
	f.params[aws.ToString(in.Name)] = aws.ToString(in.Value)
	return &ssm.PutParameterOutput{}, nil
}

func TestSSMWatermarkStartsFromConfiguredDateAndRoundTrips(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := &fakeSSM{params: map[string]string{}}
	w := &SSMWatermark{Client: store, Name: "/relay/ingest/watermark", Start: start}
	ctx := context.Background()

	got, err := w.Get(ctx)
	if err != nil || !got.Equal(start) {
		t.Fatalf("missing parameter: Get = %v, %v; want %v", got, err, start)
	}
	next := time.Date(2026, 9, 22, 9, 58, 0, 0, time.UTC)
	if err := w.Set(ctx, next); err != nil {
		t.Fatal(err)
	}
	if v := store.params["/relay/ingest/watermark"]; v != "2026-09-22T09:58:00Z" {
		t.Errorf("stored %q", v)
	}
	got, err = w.Get(ctx)
	if err != nil || !got.Equal(next) {
		t.Errorf("Get = %v, %v; want %v", got, err, next)
	}
}

func TestSSMWatermarkReadsTheBootstrapFormat(t *testing.T) {
	store := &fakeSSM{params: map[string]string{"/w": "1970-01-01T00:00:00.000Z"}}
	got, err := (&SSMWatermark{Client: store, Name: "/w"}).Get(context.Background())
	if err != nil || !got.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("Get = %v, %v", got, err)
	}
}
