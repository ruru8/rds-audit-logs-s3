package main

import (
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	"rdsauditlogss3/internal/database"
	"rdsauditlogss3/internal/logcollector"
	"rdsauditlogss3/internal/parser"
	"rdsauditlogss3/internal/processor"
	"rdsauditlogss3/internal/s3writer"
)

// HandlerConfig holds the configuration for the lambda function
type HandlerConfig struct {
	RdsClusterIdentifier string `envconfig:"RDS_CLUSTER_IDENTIFIER" required:"true" desc:"Identifier of the RDS cluster"`
	S3BucketName         string `envconfig:"S3_BUCKET_NAME" required:"true" desc:"Name of the bucket to write logs to"`
	DynamoDbTableName    string `envconfig:"DYNAMODB_TABLE_NAME" required:"true" desc:"DynamoDb table name"`
	AwsRegion            string `envconfig:"AWS_REGION" required:"true" desc:"AWS region"`
	Debug                bool   `envconfig:"DEBUG" required:"true" desc:"Enable debug mode."`
}

type lambdaHandler struct {
	processor *processor.Processor
}

// Handler is the handler registered as the lambda function handler
func (lh *lambdaHandler) Handler() error {
	err := lh.processor.Process()
	if err != nil {
		log.WithError(err).Errorf("Error in Lambda function")
		return fmt.Errorf("error in Lambda function")
	}
	return nil
}

// 取得したInstanceごとにCreate
func HandleRequest() {
	var c HandlerConfig
	err := envconfig.Process("", &c)
	if err != nil {
		log.WithError(err).Fatal("Error parsing configuration")
	}

	if c.Debug {
		log.SetLevel(log.DebugLevel)
	}

	// Initialize AWS session
	sessionConfig := &aws.Config{
		Region: aws.String(c.AwsRegion),
	}
	sess := session.New(sessionConfig)

	// Cluster名からInstance名を取得
	svc := rds.New(sess)
	clusterName := c.RdsClusterIdentifier
	input := &rds.DescribeDBInstancesInput{
		Filters: []*rds.Filter{
			{
				Name:   aws.String("db-cluster-id"),
				Values: []*string{aws.String(clusterName)},
			},
		},
	}

	result, err := svc.DescribeDBInstances(input)
	if err != nil {
		log.WithError(err).Fatal("Error get rds Instances")
	}

	for _, instance := range result.DBInstances {
		rdsInstanceIdentifier := aws.StringValue(instance.DBInstanceIdentifier)
		fmt.Println("Instance Name:", rdsInstanceIdentifier)

		lh := &lambdaHandler{
			processor: processor.NewProcessor(
				database.NewDynamoDb(
					dynamodb.New(sess),
					c.DynamoDbTableName,
				),
				logcollector.NewRdsLogCollector(
					rds.New(sess),
					logcollector.NewAWSHttpClient(sess),
					c.AwsRegion,
					rdsInstanceIdentifier,
					"mysql",
				),
				s3writer.NewS3Writer(
					s3manager.NewUploader(sess),
					c.S3BucketName,
					fmt.Sprintf("%s/%s", rdsInstanceIdentifier, "audit-logs"),
				),
				parser.NewAuditLogParser(),
				rdsInstanceIdentifier,
			),
		}
		lh.Handler()
	}
}

// start HandleRequest
func main() {
	lambda.Start(HandleRequest)
}
