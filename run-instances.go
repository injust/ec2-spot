package main

import (
	"context"
	"log"
	"os"
	"time"

	flag "github.com/spf13/pflag"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

var (
	count          = flag.Int32P("count", "n", 1, "Number of instances to launch simultaneously")
	interval       = flag.Duration("interval", 500*time.Millisecond, "Interval between instance launch attempts")
	launchTemplate = flag.String("launch-template", "", "EC2 launch template name")
	region         = flag.String("region", os.Getenv("AWS_REGION"), "AWS region (defaults to $AWS_REGION)")
)

func runInstances(ctx context.Context, client *ec2.Client, launchTemplate string) (*ec2.RunInstancesOutput, error) {
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
	if *region == "" || *launchTemplate == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*region),
		config.WithRetryer(func() aws.Retryer {
			// NOTE(https://github.com/aws/aws-sdk-go-v2/issues/3193): `o.MaxAttempts = 0` does not work
			return retry.AddWithMaxAttempts(retry.NewStandard(func(o *retry.StandardOptions) {
				o.Backoff = retry.BackoffDelayerFunc(func(attempt int, err error) (time.Duration, error) {
					return *interval, nil
				})
				o.RateLimiter = ratelimit.None
			}), 0)
		}),
	)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	client := ec2.NewFromConfig(cfg)

	for {
		if resp, err := runInstances(ctx, client, *launchTemplate); err != nil {
			log.Printf("Launch failed: %v", err)
		} else {
			for _, instance := range resp.Instances {
				log.Printf("Launched %s instance in %s: %s", instance.InstanceType, *instance.Placement.AvailabilityZone, *instance.InstanceId)
			}
		}

		time.Sleep(*interval)
	}
}
