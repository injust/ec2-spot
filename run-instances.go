package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"

	flag "github.com/spf13/pflag"
)

var (
	count          = flag.Int32P("count", "n", 1, "Number of instances to launch simultaneously")
	interval       = flag.Duration("interval", 1*time.Second, "Time between instance launch attempts")
	launchTemplate = flag.String("launch-template", "", "Launch template name")
)

func InfiniteRetryer() aws.Retryer {
	backoff := retry.BackoffDelayerFunc(func(attempt int, err error) (time.Duration, error) {
		return *interval, nil
	})
	retryables := retry.IsErrorRetryableFunc(func(err error) aws.Ternary {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "MaxSpotInstanceCountExceeded", "SpotMaxPriceTooLow":
				return aws.TrueTernary
			}
		}
		return aws.UnknownTernary
	})

	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.Backoff = backoff
		o.RateLimiter = ratelimit.None
		o.Retryables = append(o.Retryables, retryables)
	})
	// NOTE(https://github.com/aws/aws-sdk-go-v2/issues/3193): Using functional option (`o.MaxAttempts = 0`) sets MaxAttempts to the default
	return retry.AddWithMaxAttempts(retryer, 0)
}

func RunInstances(ctx context.Context, client *ec2.Client, launchTemplate string) (*ec2.RunInstancesOutput, error) {
	input := &ec2.RunInstancesInput{
		LaunchTemplate: &types.LaunchTemplateSpecification{
			LaunchTemplateName: aws.String(launchTemplate),
		},
		MinCount: aws.Int32(1),
		MaxCount: count,
	}
	return client.RunInstances(ctx, input)
}

func main() {
	flag.Parse()
	if *launchTemplate == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRetryer(InfiniteRetryer))
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	client := ec2.NewFromConfig(cfg)

	for {
		resp, err := RunInstances(ctx, client, *launchTemplate)
		if err != nil {
			log.Printf("Launch failed: %v", err)
		} else {
			for _, instance := range resp.Instances {
				log.Printf("Launched %s instance in %s: %s", instance.InstanceType, *instance.Placement.AvailabilityZone, *instance.InstanceId)
			}
		}

		time.Sleep(*interval)
	}
}
