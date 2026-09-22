// Command relayctl operates Relay from your laptop: inspect and redrive the
// dead-letter queue, check queue and ERP state, run the reconciler, and flip
// the chaos switches for a live demo.
//
//	relayctl [--local | --profile NAME] [--stack relay-dev] <command>
//
//	stats                           queues, ERP counts, watermark, chaos flags
//	dlq list                        what's dead-lettered, and why
//	dlq redrive [--all] [--dry-run] move transient failures back to the queue
//	dlq ack <event-key|message-id>  delete a message a human has dealt with
//	reconcile [--dry-run]           run the reconciler now and print the drift
//	chaos [--erp-fail-rate F] [--crash-after N] [--off]
//
// --local talks to the docker compose stack; otherwise the real account is
// reached only through an explicit --profile (or CI's ambient role).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"relay/relayctl/ops"
)

const usage = `usage: relayctl [--local | --profile NAME] [--stack NAME] <command>

commands:
  stats                            queues, ERP counts, watermark, chaos flags
  dlq list                         what's dead-lettered, and why
  dlq redrive [--all] [--dry-run]  move transient failures back to the main queue
  dlq ack <event-key|message-id>   delete a message a human has dealt with
  reconcile [--dry-run]            run the reconciler now and print the drift
  chaos [--erp-fail-rate F] [--crash-after N] [--off]
`

type target struct {
	sqs    *sqs.Client
	ssm    *ssm.Client
	ddb    *dynamodb.Client
	lambda *lambda.Client

	local         bool
	prefix        string
	queueURL      string
	dlqURL        string
	tables        [3][2]string // (label, table name)
	reconcilerFn  string
	reconcilerURL string
}

func main() {
	global := flag.NewFlagSet("relayctl", flag.ExitOnError)
	global.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	local := global.Bool("local", false, "use the docker compose stack")
	endpoint := global.String("endpoint", "http://127.0.0.1:4566", "emulator endpoint for --local")
	reconcilerURL := global.String("reconciler-url",
		"http://127.0.0.1:9003/2015-03-31/functions/function/invocations", "reconciler invoke URL for --local")
	profile := global.String("profile", "", "AWS profile (from `aws login --profile NAME`)")
	stack := global.String("stack", "relay-dev", "application stack name")
	region := global.String("region", "us-east-1", "AWS region")
	prefix := global.String("prefix", "/relay", "SSM parameter prefix")
	_ = global.Parse(os.Args[1:])
	args := global.Args()
	if len(args) == 0 {
		global.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t, err := connect(ctx, *local, *endpoint, *reconcilerURL, *profile, *stack, *region, *prefix)
	if err == nil {
		err = run(ctx, t, args, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "relayctl:", err)
		os.Exit(1)
	}
}

func connect(ctx context.Context, local bool, endpoint, reconcilerURL, profile, stack, region, prefix string) (*target, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if local {
		opts = append(opts,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
			config.WithBaseEndpoint(endpoint))
	} else {
		if os.Getenv("AWS_ENDPOINT_URL") != "" {
			return nil, errors.New("AWS_ENDPOINT_URL is set; unset it, or pass --local for the emulator")
		}
		if profile != "" {
			opts = append(opts, config.WithSharedConfigProfile(profile))
		}
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	t := &target{
		sqs: sqs.NewFromConfig(cfg), ssm: ssm.NewFromConfig(cfg),
		ddb: dynamodb.NewFromConfig(cfg), lambda: lambda.NewFromConfig(cfg),
		local: local, prefix: prefix, reconcilerURL: reconcilerURL,
	}
	if local {
		for i, name := range []string{"relay-events", "relay-events-dlq"} {
			out, err := t.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
			if err != nil {
				return nil, fmt.Errorf("is the stack up (docker compose up -d)? %w", err)
			}
			if i == 0 {
				t.queueURL = aws.ToString(out.QueueUrl)
			} else {
				t.dlqURL = aws.ToString(out.QueueUrl)
			}
		}
		t.tables = [3][2]string{{"orders", "orders"}, {"invoices", "invoices"}, {"customers", "customers"}}
		return t, nil
	}
	cfn := cloudformation.NewFromConfig(cfg)
	out, err := cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stack)})
	if err != nil {
		return nil, fmt.Errorf("read stack %s: %w", stack, err)
	}
	outputs := map[string]string{}
	for _, o := range out.Stacks[0].Outputs {
		outputs[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
	}
	t.queueURL, t.dlqURL = outputs["QueueUrl"], outputs["DeadLetterQueueUrl"]
	t.tables = [3][2]string{
		{"orders", outputs["OrdersTableName"]},
		{"invoices", outputs["InvoicesTableName"]},
		{"customers", outputs["CustomersTableName"]},
	}
	t.reconcilerFn = outputs["ReconcilerFunctionName"]
	return t, nil
}

func run(ctx context.Context, t *target, args []string, w io.Writer) error {
	switch args[0] {
	case "stats":
		return stats(ctx, t, w)
	case "dlq":
		if len(args) < 2 {
			return errors.New("dlq needs list, redrive or ack")
		}
		return dlq(ctx, t, args[1], args[2:], w)
	case "reconcile":
		fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
		dryRun := fs.Bool("dry-run", false, "report drift without re-enqueueing")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		return reconcile(ctx, t, *dryRun, w)
	case "chaos":
		return chaos(ctx, t, args[1:], w)
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

func stats(ctx context.Context, t *target, w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, q := range []struct{ label, url string }{{"queue", t.queueURL}, {"dead-letter queue", t.dlqURL}} {
		d, err := ops.Depth(ctx, t.sqs, q.url)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d waiting\t%d in flight or backing off\t%d delayed\n", q.label, d.Visible, d.InFlight, d.Delayed)
	}
	for _, tbl := range t.tables {
		n, err := ops.CountItems(ctx, t.ddb, tbl[1])
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\t\t\n", tbl[0], n)
	}
	wm, err := ops.GetParam(ctx, t.ssm, t.prefix+"/ingest/watermark")
	if err != nil {
		return err
	}
	c, err := ops.GetChaos(ctx, t.ssm, t.prefix)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(tw, "watermark\t%s\t\t\n", orDash(wm))
	_, _ = fmt.Fprintf(tw, "chaos\terp fail rate %.2f\tcrash after %d\t\n", c.ERPFailRate, c.CrashAfter)
	return tw.Flush()
}

func dlq(ctx context.Context, t *target, sub string, args []string, w io.Writer) error {
	switch sub {
	case "list":
		msgs, err := ops.ListDLQ(ctx, t.sqs, t.dlqURL)
		if err != nil {
			return err
		}
		if len(msgs) == 0 {
			_, _ = fmt.Fprintln(w, "the dead-letter queue is empty")
			return nil
		}
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "SENT\tEVENT KEY\tKIND\tREASON\tMESSAGE ID")
		for _, m := range msgs {
			kind, reason := "transient", "retries exhausted"
			if m.Permanent() {
				kind, reason = "permanent", m.Reason
			}
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
				m.SentAt.Format(time.RFC3339), orDash(m.EventKey), kind, reason, m.MessageID)
		}
		return tw.Flush()
	case "redrive":
		fs := flag.NewFlagSet("redrive", flag.ContinueOnError)
		all := fs.Bool("all", false, "also move permanent failures (they will fail again unless the code changed)")
		dryRun := fs.Bool("dry-run", false, "show what would move")
		if err := fs.Parse(args); err != nil {
			return err
		}
		res, err := ops.Redrive(ctx, t.sqs, t.dlqURL, t.queueURL, *all, *dryRun)
		verb := "moved"
		if *dryRun {
			verb = "would move"
		}
		_, _ = fmt.Fprintf(w, "%s %d message(s) back to the queue; left %d permanent failure(s) in the DLQ\n",
			verb, len(res.Moved), len(res.Skipped))
		for _, m := range res.Skipped {
			_, _ = fmt.Fprintf(w, "  left %s (%s): fix the deal in Salesforce, then `relayctl dlq ack %s`\n",
				orDash(m.EventKey), m.Reason, m.MessageID)
		}
		return err
	case "ack":
		if len(args) != 1 {
			return errors.New("dlq ack needs one event key or message ID")
		}
		acked, err := ops.Ack(ctx, t.sqs, t.dlqURL, args[0])
		if err != nil {
			return err
		}
		if len(acked) == 0 {
			return fmt.Errorf("no dead-lettered message matches %q", args[0])
		}
		_, _ = fmt.Fprintf(w, "acknowledged %d message(s)\n", len(acked))
		return nil
	default:
		return fmt.Errorf("unknown dlq command %q", sub)
	}
}

func reconcile(ctx context.Context, t *target, dryRun bool, w io.Writer) error {
	payload, _ := json.Marshal(map[string]bool{"dry_run": dryRun})
	var body []byte
	if t.local {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.reconcilerURL, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("invoke the local reconciler: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if body, err = io.ReadAll(resp.Body); err != nil {
			return err
		}
	} else {
		out, err := t.lambda.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String(t.reconcilerFn), Payload: payload})
		if err != nil {
			return err
		}
		if out.FunctionError != nil {
			return fmt.Errorf("reconciler failed: %s", out.Payload)
		}
		body = out.Payload
	}
	var report struct {
		Drift     map[string]int `json:"drift"`
		Reenqueue int            `json:"reenqueued"`
		Items     []struct {
			Kind   string `json:"kind"`
			ID     string `json:"opportunity_id"`
			Detail string `json:"detail"`
		} `json:"items"`
		Error string `json:"errorMessage"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		return fmt.Errorf("unexpected reconciler response: %s", body)
	}
	if report.Error != "" {
		return fmt.Errorf("reconciler failed: %s", report.Error)
	}
	_, _ = fmt.Fprintf(w, "drift found: %d, re-enqueued: %d\n", len(report.Items), report.Reenqueue)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, it := range report.Items {
		_, _ = fmt.Fprintf(tw, "  %s\t%s\t%s\n", it.Kind, it.ID, it.Detail)
	}
	return tw.Flush()
}

func chaos(ctx context.Context, t *target, args []string, w io.Writer) error {
	fs := flag.NewFlagSet("chaos", flag.ContinueOnError)
	rate := fs.Float64("erp-fail-rate", -1, "share of ERP writes that fail (0-1)")
	crash := fs.Int("crash-after", -1, "crash the worker after N records (0 = off)")
	off := fs.Bool("off", false, "turn all chaos off")
	if err := fs.Parse(args); err != nil {
		return err
	}
	current, err := ops.GetChaos(ctx, t.ssm, t.prefix)
	if err != nil {
		return err
	}
	next := current
	switch {
	case *off:
		next = ops.Chaos{}
	default:
		if *rate >= 0 {
			next.ERPFailRate = *rate
		}
		if *crash >= 0 {
			next.CrashAfter = *crash
		}
	}
	if next != current {
		if err := ops.SetChaos(ctx, t.ssm, t.prefix, next); err != nil {
			return err
		}
	}
	_, _ = fmt.Fprintf(w, "chaos: erp fail rate %.2f, crash after %d record(s)%s\n",
		next.ERPFailRate, next.CrashAfter, map[bool]string{true: " (off)", false: ""}[next == ops.Chaos{}])
	return nil
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}
