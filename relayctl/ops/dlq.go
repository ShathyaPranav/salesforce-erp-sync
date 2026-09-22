// Package ops holds relayctl's operations, written against small interfaces
// so they can be tested without AWS.
package ops

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SQSAPI is the part of the SQS client relayctl uses.
type SQSAPI interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// DeadLetter is one message in the DLQ, as an operator sees it.
type DeadLetter struct {
	MessageID string
	EventKey  string
	// Reason is set when the worker dead-lettered the message itself: a
	// permanent error such as missing_amount. Empty means SQS moved it after
	// maxReceiveCount failed attempts: transient failures that ran out of retries.
	Reason   string
	Detail   string
	Received int
	SentAt   time.Time
	Body     string

	receipt string
}

// Permanent reports whether retrying this message can't help.
func (d DeadLetter) Permanent() bool { return d.Reason != "" }

// peek receives up to limit DLQ messages and holds them invisible for hold, so
// the caller can act on them. Callers release what they don't delete.
func peek(ctx context.Context, api SQSAPI, dlqURL string, limit int, hold int32) ([]DeadLetter, error) {
	var out []DeadLetter
	seen := map[string]bool{}
	for empty := 0; len(out) < limit && empty < 2; {
		resp, err := api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(dlqURL),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             1,
			VisibilityTimeout:           hold,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"All"},
		})
		if err != nil {
			return out, fmt.Errorf("receive from DLQ: %w", err)
		}
		if len(resp.Messages) == 0 {
			empty++
			continue
		}
		for _, m := range resp.Messages {
			id := aws.ToString(m.MessageId)
			if seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, toDeadLetter(m))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SentAt.Before(out[j].SentAt) })
	return out, nil
}

func toDeadLetter(m sqstypes.Message) DeadLetter {
	attr := func(name string) string {
		if v, ok := m.MessageAttributes[name]; ok {
			return aws.ToString(v.StringValue)
		}
		return ""
	}
	received, _ := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
	sentMs, _ := strconv.ParseInt(m.Attributes["SentTimestamp"], 10, 64)
	return DeadLetter{
		MessageID: aws.ToString(m.MessageId),
		EventKey:  attr("event_key"),
		Reason:    attr("reason"),
		Detail:    attr("detail"),
		Received:  received,
		SentAt:    time.UnixMilli(sentMs).UTC(),
		Body:      aws.ToString(m.Body),
		receipt:   aws.ToString(m.ReceiptHandle),
	}
}

func release(ctx context.Context, api SQSAPI, queueURL string, msgs []DeadLetter) {
	for _, m := range msgs {
		_, _ = api.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(queueURL), ReceiptHandle: aws.String(m.receipt), VisibilityTimeout: 0,
		})
	}
}

// ListDLQ returns what's in the DLQ without removing anything.
func ListDLQ(ctx context.Context, api SQSAPI, dlqURL string) ([]DeadLetter, error) {
	msgs, err := peek(ctx, api, dlqURL, 100, 30)
	release(ctx, api, dlqURL, msgs)
	return msgs, err
}

// RedriveResult says what a redrive did.
type RedriveResult struct {
	Moved   []DeadLetter
	Skipped []DeadLetter // permanent errors left in place
}

// Redrive moves dead-lettered messages back to the main queue. By default
// only transient failures move: a permanent error (the worker set a reason)
// would only be dead-lettered again. Fix the deal in Salesforce instead, and
// its new version flows through on its own. Each message is deleted from the
// DLQ only after its copy is safely on the main queue.
func Redrive(ctx context.Context, api SQSAPI, dlqURL, mainURL string, all, dryRun bool) (RedriveResult, error) {
	msgs, err := peek(ctx, api, dlqURL, 1000, 60)
	if err != nil {
		release(ctx, api, dlqURL, msgs)
		return RedriveResult{}, err
	}
	var res RedriveResult
	var keep []DeadLetter
	for i, m := range msgs {
		if m.Permanent() && !all {
			res.Skipped = append(res.Skipped, m)
			keep = append(keep, m)
			continue
		}
		if dryRun {
			res.Moved = append(res.Moved, m)
			keep = append(keep, m)
			continue
		}
		attrs := map[string]sqstypes.MessageAttributeValue{
			"redriven_from_dlq": {DataType: aws.String("String"), StringValue: aws.String(m.MessageID)},
		}
		if m.EventKey != "" {
			attrs["event_key"] = sqstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(m.EventKey)}
		}
		if _, err := api.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl: aws.String(mainURL), MessageBody: aws.String(m.Body), MessageAttributes: attrs,
		}); err != nil {
			release(ctx, api, dlqURL, append(keep, msgs[i:]...))
			return res, fmt.Errorf("send %s back to the main queue: %w", m.MessageID, err)
		}
		if _, err := api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl: aws.String(dlqURL), ReceiptHandle: aws.String(m.receipt),
		}); err != nil {
			// The copy is already on the main queue; a duplicate is harmless there.
			release(ctx, api, dlqURL, append(keep, msgs[i+1:]...))
			return res, fmt.Errorf("delete %s from the DLQ: %w", m.MessageID, err)
		}
		res.Moved = append(res.Moved, m)
	}
	release(ctx, api, dlqURL, keep)
	return res, nil
}

// Ack deletes dead-lettered messages whose event key or message ID matches
// id, once a human has dealt with them (e.g. fixed the deal in Salesforce).
func Ack(ctx context.Context, api SQSAPI, dlqURL, id string) ([]DeadLetter, error) {
	msgs, err := peek(ctx, api, dlqURL, 1000, 30)
	if err != nil {
		release(ctx, api, dlqURL, msgs)
		return nil, err
	}
	var acked, keep []DeadLetter
	for _, m := range msgs {
		if m.MessageID != id && m.EventKey != id {
			keep = append(keep, m)
			continue
		}
		if _, err := api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl: aws.String(dlqURL), ReceiptHandle: aws.String(m.receipt),
		}); err != nil {
			keep = append(keep, m)
			continue
		}
		acked = append(acked, m)
	}
	release(ctx, api, dlqURL, keep)
	return acked, nil
}

// QueueDepth is a queue's approximate message counts.
type QueueDepth struct {
	Visible, InFlight, Delayed int
}

// Depth reads a queue's approximate counts.
func Depth(ctx context.Context, api SQSAPI, queueURL string) (QueueDepth, error) {
	out, err := api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	})
	if err != nil {
		return QueueDepth{}, err
	}
	n := func(k sqstypes.QueueAttributeName) int {
		v, _ := strconv.Atoi(out.Attributes[string(k)])
		return v
	}
	return QueueDepth{
		Visible:  n(sqstypes.QueueAttributeNameApproximateNumberOfMessages),
		InFlight: n(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible),
		Delayed:  n(sqstypes.QueueAttributeNameApproximateNumberOfMessagesDelayed),
	}, nil
}
