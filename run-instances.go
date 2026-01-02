package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/alexflint/go-arg"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

var args struct {
	LaunchTemplate string        `arg:"positional,required" help:"Launch template name" placeholder:"LAUNCH_TEMPLATE"`
	Count          int32         `arg:"-n,--" default:"1" help:"number of instances to launch simultaneously"`
	Interval       time.Duration `arg:"-t,--" default:"1s" help:"time between instance launch attempts"`
	Region         string        `arg:"--,env:AWS_REGION,required" help:"AWS region"`
}

func Retryer() aws.Retryer {
	backoff := retry.BackoffDelayerFunc(func(attempt int, err error) (time.Duration, error) {
		return args.Interval, nil
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

	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.Backoff = backoff
		o.MaxAttempts = 128 // Retry attempts leak memory (https://github.com/aws/aws-sdk-go-v2/issues/3241)
		o.RateLimiter = ratelimit.None
		o.Retryables = append(o.Retryables, retryables)
	})
}

func RunInstances(ctx context.Context, client *ec2.Client) (*ec2.RunInstancesOutput, error) {
	input := &ec2.RunInstancesInput{
		LaunchTemplate: &types.LaunchTemplateSpecification{
			LaunchTemplateName: aws.String(args.LaunchTemplate),
		},
		MinCount: aws.Int32(1),
		MaxCount: aws.Int32(args.Count),
	}
	return client.RunInstances(ctx, input)
}

func main() {
	arg.MustParse(&args)

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRetryer(Retryer))
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	client := ec2.NewFromConfig(cfg)

	for {
		resp, err := RunInstances(ctx, client)
		if err != nil {
			var maxAttemptsErr *retry.MaxAttemptsError
			if !errors.As(err, &maxAttemptsErr) {
				log.Printf("Launch failed: %v", err)
			}
		} else {
			for _, instance := range resp.Instances {
				log.Printf("Launched %s instance in %s: %s", instance.InstanceType, *instance.Placement.AvailabilityZone, *instance.InstanceId)
			}
		}

		time.Sleep(args.Interval)
	}
}
