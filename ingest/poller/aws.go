package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"

	"relay/internal/events"
	"relay/internal/salesforce"
)

// SQSAPI is the part of the SQS client the publisher needs.
type SQSAPI interface {
	SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

// SQSPublisher publishes events in batches of 10 (the SQS maximum), in order.
type SQSPublisher struct {
	Client   SQSAPI
	QueueURL string
}

// Publish implements Publisher. SendMessageBatch returns HTTP 200 even when
// some entries fail, listing them in Failed: code that only checks err loses
// messages silently. So both are checked, and the count stops at the first
// failed entry.
func (p *SQSPublisher) Publish(ctx context.Context, evs []events.OpportunityEvent) (int, error) {
	for start := 0; start < len(evs); start += 10 {
		chunk := evs[start:min(start+10, len(evs))]
		entries := make([]sqstypes.SendMessageBatchRequestEntry, len(chunk))
		for i, ev := range chunk {
			body, err := json.Marshal(ev)
			if err != nil {
				return start, err
			}
			entries[i] = sqstypes.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(i)),
				MessageBody: aws.String(string(body)),
				MessageAttributes: map[string]sqstypes.MessageAttributeValue{
					"event_key": {DataType: aws.String("String"), StringValue: aws.String(ev.EventKey)},
				},
			}
		}
		out, err := p.Client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(p.QueueURL),
			Entries:  entries,
		})
		if err != nil {
			return start, fmt.Errorf("send batch: %w", err)
		}
		if len(out.Failed) > 0 {
			firstIdx, first := len(chunk), out.Failed[0]
			for _, f := range out.Failed {
				idx, convErr := strconv.Atoi(aws.ToString(f.Id))
				if convErr != nil {
					return start, fmt.Errorf("send batch: unexpected entry id %q", aws.ToString(f.Id))
				}
				if idx < firstIdx {
					firstIdx, first = idx, f
				}
			}
			return start + firstIdx, fmt.Errorf("send batch: entry %d failed: %s %s",
				start+firstIdx, aws.ToString(first.Code), aws.ToString(first.Message))
		}
	}
	return len(evs), nil
}

// SSMAPI is the part of the SSM client the watermark store needs.
type SSMAPI interface {
	GetParameter(ctx context.Context, in *ssm.GetParameterInput, opts ...func(*ssm.Options)) (*ssm.GetParameterOutput, error)
	PutParameter(ctx context.Context, in *ssm.PutParameterInput, opts ...func(*ssm.Options)) (*ssm.PutParameterOutput, error)
}

// SSMWatermark keeps the watermark in an SSM String parameter. The parameter
// is created on first write, not by the stack, so a redeploy never resets it.
type SSMWatermark struct {
	Client SSMAPI
	Name   string
	Start  time.Time // used when the parameter doesn't exist yet
}

// Get implements Watermark.
func (w *SSMWatermark) Get(ctx context.Context) (time.Time, error) {
	out, err := w.Client.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(w.Name)})
	var notFound *ssmtypes.ParameterNotFound
	if errors.As(err, &notFound) {
		return w.Start.UTC(), nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return salesforce.ParseDateTime(aws.ToString(out.Parameter.Value))
}

// Set implements Watermark.
func (w *SSMWatermark) Set(ctx context.Context, t time.Time) error {
	_, err := w.Client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String(w.Name),
		Value:     aws.String(salesforce.Literal(t)),
		Type:      ssmtypes.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	return err
}
