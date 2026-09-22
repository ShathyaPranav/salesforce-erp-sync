package ops

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

// SSMAPI is the part of the SSM client relayctl uses.
type SSMAPI interface {
	GetParameter(ctx context.Context, in *ssm.GetParameterInput, opts ...func(*ssm.Options)) (*ssm.GetParameterOutput, error)
	PutParameter(ctx context.Context, in *ssm.PutParameterInput, opts ...func(*ssm.Options)) (*ssm.PutParameterOutput, error)
}

// DynamoAPI is the part of the DynamoDB client relayctl uses.
type DynamoAPI interface {
	Scan(ctx context.Context, in *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// Chaos is the pair of failure switches the worker reads on every invocation.
type Chaos struct {
	ERPFailRate float64 // share of ERP writes that fail with a throttling error
	CrashAfter  int     // crash the worker after N records; 0 = off
}

// GetParam reads a String parameter; missing is "", not an error.
func GetParam(ctx context.Context, api SSMAPI, name string) (string, error) {
	out, err := api.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(name)})
	var notFound *ssmtypes.ParameterNotFound
	if errors.As(err, &notFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return aws.ToString(out.Parameter.Value), nil
}

// GetChaos reads the current chaos settings. Missing parameters mean off.
func GetChaos(ctx context.Context, api SSMAPI, prefix string) (Chaos, error) {
	rate, err := GetParam(ctx, api, prefix+"/chaos/erp_fail_rate")
	if err != nil {
		return Chaos{}, err
	}
	crash, err := GetParam(ctx, api, prefix+"/chaos/worker_crash_after")
	if err != nil {
		return Chaos{}, err
	}
	c := Chaos{}
	c.ERPFailRate, _ = strconv.ParseFloat(rate, 64)
	c.CrashAfter, _ = strconv.Atoi(crash)
	return c, nil
}

// SetChaos writes both switches. The parameters are created on first use, so
// the stack never owns (or resets) them.
func SetChaos(ctx context.Context, api SSMAPI, prefix string, c Chaos) error {
	if c.ERPFailRate < 0 || c.ERPFailRate > 1 {
		return fmt.Errorf("erp fail rate must be between 0 and 1, got %v", c.ERPFailRate)
	}
	if c.CrashAfter < 0 {
		return fmt.Errorf("crash-after must be 0 (off) or more, got %d", c.CrashAfter)
	}
	for name, value := range map[string]string{
		prefix + "/chaos/erp_fail_rate":      strconv.FormatFloat(c.ERPFailRate, 'f', -1, 64),
		prefix + "/chaos/worker_crash_after": strconv.Itoa(c.CrashAfter),
	} {
		if _, err := api.PutParameter(ctx, &ssm.PutParameterInput{
			Name: aws.String(name), Value: aws.String(value),
			Type: ssmtypes.ParameterTypeString, Overwrite: aws.Bool(true),
		}); err != nil {
			return fmt.Errorf("set %s: %w", name, err)
		}
	}
	return nil
}

// CountItems counts a table's items with a paginated COUNT scan. Fine for
// Relay's small tables; DescribeTable's ItemCount is only refreshed about
// every six hours.
func CountItems(ctx context.Context, api DynamoAPI, table string) (int, error) {
	total := 0
	var start map[string]ddbtypes.AttributeValue
	for {
		out, err := api.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(table), Select: ddbtypes.SelectCount, ExclusiveStartKey: start,
		})
		if err != nil {
			return 0, err
		}
		total += int(out.Count)
		if len(out.LastEvaluatedKey) == 0 {
			return total, nil
		}
		start = out.LastEvaluatedKey
	}
}
