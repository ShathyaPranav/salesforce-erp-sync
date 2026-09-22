// Command ingest is the Salesforce poller Lambda. EventBridge Scheduler
// invokes it every 2 minutes; each invocation polls one window and publishes
// one SQS message per changed Closed Won deal.
//
// Configuration (environment):
//
//	RELAY_PARAM_PREFIX  SSM prefix for Salesforce settings and the watermark (default /relay)
//	QUEUE_URL           target queue URL, or QUEUE_NAME to look it up (default relay-events)
//	INGEST_LAG_SECONDS  how far behind Salesforce's clock to stay (default 120)
//	WATERMARK_START     where to start if the watermark parameter doesn't exist (default 1970-01-01T00:00:00Z)
//
// AWS_ENDPOINT_URL (set only in docker compose) points the SDK at the local emulator.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"relay/ingest/poller"
	"relay/internal/salesforce"
)

var (
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "ingest")

	// Built on the first successful invocation and reused while the Lambda is
	// warm, so the Salesforce token is cached across polls.
	mu     sync.Mutex
	cached *poller.Poller
)

type response struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Found     int    `json:"found"`
	Published int    `json:"published"`
}

func handler(ctx context.Context) (response, error) {
	p, err := getPoller(ctx)
	if err != nil {
		logger.Error("setup failed", "error", err.Error())
		return response{}, err
	}
	res, err := p.Run(ctx)
	for _, key := range res.Events {
		logger.Info("published", "event_key", key)
	}
	out := response{
		From: salesforce.Literal(res.From), To: salesforce.Literal(res.To),
		Found: res.Found, Published: res.Published,
	}
	if err != nil {
		var authErr *salesforce.AuthError
		kind := "transient"
		if errors.As(err, &authErr) {
			kind = "auth" // needs a human: fix the External Client App or the secret
		}
		logger.Error("poll failed", "kind", kind, "error", err.Error(),
			"from", out.From, "watermark", out.To, "published", out.Published)
		return out, err
	}
	logger.Info("poll complete", "from", out.From, "to", out.To, "found", out.Found, "published", out.Published)
	return out, nil
}

func getPoller(ctx context.Context) (*poller.Poller, error) {
	mu.Lock()
	defer mu.Unlock()
	if cached != nil {
		return cached, nil
	}
	p, err := newPoller(ctx)
	if err != nil {
		return nil, err // not cached: the next invocation tries again
	}
	cached = p
	return p, nil
}

func newPoller(ctx context.Context) (*poller.Poller, error) {
	prefix := env("RELAY_PARAM_PREFIX", "/relay")
	lagSeconds, err := strconv.Atoi(env("INGEST_LAG_SECONDS", "120"))
	if err != nil || lagSeconds < 0 {
		return nil, fmt.Errorf("INGEST_LAG_SECONDS must be a whole number of seconds")
	}
	start, err := salesforce.ParseDateTime(env("WATERMARK_START", "1970-01-01T00:00:00Z"))
	if err != nil {
		return nil, fmt.Errorf("WATERMARK_START: %w", err)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	ssmClient := ssm.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)

	params, err := getParams(ctx, ssmClient, prefix+"/salesforce/",
		"login_url", "api_version", "client_id", "client_secret")
	if err != nil {
		return nil, err
	}
	queueURL, err := resolveQueueURL(ctx, sqsClient)
	if err != nil {
		return nil, err
	}

	return &poller.Poller{
		Source: &salesforce.Client{
			LoginURL:     params["login_url"],
			APIVersion:   params["api_version"],
			ClientID:     params["client_id"],
			ClientSecret: params["client_secret"],
		},
		Publisher: &poller.SQSPublisher{Client: sqsClient, QueueURL: queueURL},
		Watermark: &poller.SSMWatermark{Client: ssmClient, Name: prefix + "/ingest/watermark", Start: start},
		Lag:       time.Duration(lagSeconds) * time.Second,
	}, nil
}

// getParams reads the Salesforce settings in one call. The client secret is a
// SecureString, hence WithDecryption; its value is never logged.
func getParams(ctx context.Context, client *ssm.Client, prefix string, names ...string) (map[string]string, error) {
	full := make([]string, len(names))
	for i, n := range names {
		full[i] = prefix + n
	}
	out, err := client.GetParameters(ctx, &ssm.GetParametersInput{Names: full, WithDecryption: aws.Bool(true)})
	if err != nil {
		return nil, fmt.Errorf("read SSM parameters: %w", err)
	}
	if len(out.InvalidParameters) > 0 {
		return nil, fmt.Errorf("missing SSM parameters: %v", out.InvalidParameters)
	}
	values := make(map[string]string, len(names))
	for _, p := range out.Parameters {
		values[aws.ToString(p.Name)[len(prefix):]] = aws.ToString(p.Value)
	}
	return values, nil
}

func resolveQueueURL(ctx context.Context, client *sqs.Client) (string, error) {
	if u := os.Getenv("QUEUE_URL"); u != "" {
		return u, nil
	}
	out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(env("QUEUE_NAME", "relay-events"))})
	if err != nil {
		return "", fmt.Errorf("resolve queue URL: %w", err)
	}
	return aws.ToString(out.QueueUrl), nil
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func main() {
	lambda.Start(handler)
}
