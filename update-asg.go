package main

import (
	"context"
	"log"
	"time"

	"github.com/alexflint/go-arg"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
)

var args struct {
	AutoScalingGroup string        `arg:"positional,required" help:"Auto Scaling group name" placeholder:"ASG"`
	Interval         time.Duration `arg:"-t,--" default:"0.5s" help:"time between instance launch attempts"`
	Region           string        `arg:"--,env:AWS_REGION,required" help:"AWS region"`
}

func UpdateAsg(ctx context.Context, client *autoscaling.Client) (*autoscaling.UpdateAutoScalingGroupOutput, error) {
	input := &autoscaling.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String(args.AutoScalingGroup),
	}
	return client.UpdateAutoScalingGroup(ctx, input)
}

func main() {
	arg.MustParse(&args)

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(args.Region))
	if err != nil {
		log.Fatalf("Failed to load config: %+v", err)
	}
	client := autoscaling.NewFromConfig(cfg)

	for {
		_, err := UpdateAsg(ctx, client)
		if err != nil {
			log.Printf("ASG update failed: %+v", err)
		}

		time.Sleep(args.Interval)
	}
}
