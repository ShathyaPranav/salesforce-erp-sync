package ops

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// fakeQueues is a tiny in-memory SQS: two queues, receipts, visibility.
type fakeQueues struct {
	msgs     map[string][]*fakeMsg // queue URL -> messages
	nextID   int
	failSend bool
}

type fakeMsg struct {
	id, body string
	attrs    map[string]sqstypes.MessageAttributeValue
	hidden   bool
	receipt  string
}

func newFake() *fakeQueues { return &fakeQueues{msgs: map[string][]*fakeMsg{}} }

func (f *fakeQueues) put(queue, body string, attrs map[string]string) {
	f.nextID++
	m := &fakeMsg{id: fmt.Sprintf("m%d", f.nextID), body: body, attrs: map[string]sqstypes.MessageAttributeValue{}}
	for k, v := range attrs {
		m.attrs[k] = sqstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	f.msgs[queue] = append(f.msgs[queue], m)
}

func (f *fakeQueues) visible(queue string) int {
	n := 0
	for _, m := range f.msgs[queue] {
		if !m.hidden {
			n++
		}
	}
	return n
}

func (f *fakeQueues) ReceiveMessage(_ context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	out := &sqs.ReceiveMessageOutput{}
	for _, m := range f.msgs[aws.ToString(in.QueueUrl)] {
		if m.hidden || len(out.Messages) >= int(in.MaxNumberOfMessages) {
			continue
		}
		m.hidden = in.VisibilityTimeout > 0
		f.nextID++
		m.receipt = fmt.Sprintf("r%d", f.nextID)
		out.Messages = append(out.Messages, sqstypes.Message{
			MessageId: aws.String(m.id), Body: aws.String(m.body), ReceiptHandle: aws.String(m.receipt),
			MessageAttributes: m.attrs, Attributes: map[string]string{"ApproximateReceiveCount": "5", "SentTimestamp": "0"},
		})
	}
	return out, nil
}

func (f *fakeQueues) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if f.failSend {
		return nil, errors.New("SQS is down")
	}
	attrs := map[string]string{}
	for k, v := range in.MessageAttributes {
		attrs[k] = aws.ToString(v.StringValue)
	}
	f.put(aws.ToString(in.QueueUrl), aws.ToString(in.MessageBody), attrs)
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeQueues) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	q := aws.ToString(in.QueueUrl)
	for i, m := range f.msgs[q] {
		if m.receipt == aws.ToString(in.ReceiptHandle) {
			f.msgs[q] = append(f.msgs[q][:i], f.msgs[q][i+1:]...)
			return &sqs.DeleteMessageOutput{}, nil
		}
	}
	return nil, errors.New("ReceiptHandleIsInvalid")
}

func (f *fakeQueues) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	for _, m := range f.msgs[aws.ToString(in.QueueUrl)] {
		if m.receipt == aws.ToString(in.ReceiptHandle) {
			m.hidden = in.VisibilityTimeout > 0
		}
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeQueues) GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{}, nil
}

const (
	mainQ = "main"
	dlqQ  = "dlq"
)

func seeded() *fakeQueues {
	f := newFake()
	f.put(dlqQ, `{"transient":1}`, map[string]string{"event_key": "006A:t1"})                       // moved by SQS
	f.put(dlqQ, `{"transient":2}`, map[string]string{"event_key": "006B:t1"})                       // moved by SQS
	f.put(dlqQ, `{"bad":1}`, map[string]string{"event_key": "006C:t1", "reason": "missing_amount"}) // parked by the worker
	return f
}

func TestListDoesNotRemoveOrHideAnything(t *testing.T) {
	f := seeded()
	msgs, err := ListDLQ(context.Background(), f, dlqQ)
	if err != nil || len(msgs) != 3 {
		t.Fatalf("list = %d, %v", len(msgs), err)
	}
	if f.visible(dlqQ) != 3 {
		t.Errorf("listing left %d visible, want 3", f.visible(dlqQ))
	}
}

func TestRedriveMovesTransientFailuresAndLeavesPermanentOnes(t *testing.T) {
	f := seeded()
	res, err := Redrive(context.Background(), f, dlqQ, mainQ, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Moved) != 2 || len(res.Skipped) != 1 || res.Skipped[0].Reason != "missing_amount" {
		t.Fatalf("moved %d, skipped %+v", len(res.Moved), res.Skipped)
	}
	if len(f.msgs[mainQ]) != 2 || len(f.msgs[dlqQ]) != 1 || f.visible(dlqQ) != 1 {
		t.Errorf("main %d, dlq %d (visible %d)", len(f.msgs[mainQ]), len(f.msgs[dlqQ]), f.visible(dlqQ))
	}
	if got := aws.ToString(f.msgs[mainQ][0].attrs["event_key"].StringValue); got != "006A:t1" {
		t.Errorf("event_key attribute not carried over: %q", got)
	}
}

func TestRedriveAllAndDryRun(t *testing.T) {
	f := seeded()
	res, err := Redrive(context.Background(), f, dlqQ, mainQ, true, true)
	if err != nil || len(res.Moved) != 3 {
		t.Fatalf("dry run = %+v, %v", res, err)
	}
	if len(f.msgs[mainQ]) != 0 || f.visible(dlqQ) != 3 {
		t.Errorf("a dry run changed the queues")
	}
}

func TestRedriveNeverDeletesAMessageItCouldNotResend(t *testing.T) {
	f := seeded()
	f.failSend = true
	if _, err := Redrive(context.Background(), f, dlqQ, mainQ, false, false); err == nil {
		t.Fatal("expected an error")
	}
	if len(f.msgs[dlqQ]) != 3 || f.visible(dlqQ) != 3 {
		t.Errorf("DLQ lost messages: %d left, %d visible", len(f.msgs[dlqQ]), f.visible(dlqQ))
	}
}

func TestAckDeletesOnlyTheMatchingMessage(t *testing.T) {
	f := seeded()
	acked, err := Ack(context.Background(), f, dlqQ, "006C:t1")
	if err != nil || len(acked) != 1 {
		t.Fatalf("ack = %d, %v", len(acked), err)
	}
	if len(f.msgs[dlqQ]) != 2 || f.visible(dlqQ) != 2 {
		t.Errorf("dlq has %d (%d visible), want 2", len(f.msgs[dlqQ]), f.visible(dlqQ))
	}
}

func TestSetChaosRejectsNonsense(t *testing.T) {
	if err := SetChaos(context.Background(), nil, "/relay", Chaos{ERPFailRate: 1.5}); err == nil {
		t.Error("rate 1.5 accepted")
	}
	if err := SetChaos(context.Background(), nil, "/relay", Chaos{CrashAfter: -2}); err == nil {
		t.Error("crash-after -2 accepted")
	}
}
