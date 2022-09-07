package gateway

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"time"
)

var (
	athenaClient *athena.Client
	output       string
)

func init() {
	// todo lambda env 설정
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile("zigzag-main"))
	if err != nil {
		fmt.Println(err)
	}
	athenaClient = athena.NewFromConfig(cfg)
	output = "s3://log-center-athena-query-result"
}

func StartQuery(query string) (*string, error) {
	res, err := athenaClient.StartQueryExecution(context.TODO(), &athena.StartQueryExecutionInput{
		QueryString: &query,
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: &output,
		},
	})
	if err != nil {
		return nil, err
	}
	return res.QueryExecutionId, nil
}

func WaitForFinish(executionId *string) error {
	isRunning := true
	for isRunning {
		res, err := athenaClient.GetQueryExecution(context.TODO(), &athena.GetQueryExecutionInput{
			QueryExecutionId: executionId,
		})
		if err != nil {
			return err
		}
		if res.QueryExecution.Status.State == types.QueryExecutionStateFailed {
			return errors.New(*res.QueryExecution.Status.AthenaError.ErrorMessage)
		} else if res.QueryExecution.Status.State == types.QueryExecutionStateCancelled {
			return errors.New("athena cancelled")
		} else if res.QueryExecution.Status.State == types.QueryExecutionStateSucceeded {
			isRunning = false
		} else {
			<-time.After(time.Second * 5)
		}
	}
	return nil
}

func ResultProcessing(executionId *string) error {
	res, err := athenaClient.GetQueryResults(context.TODO(), &athena.GetQueryResultsInput{
		QueryExecutionId: executionId,
	})
	if err != nil {
		return err
	}
	for _, row := range res.ResultSet.Rows {
		for _, datum := range row.Data {
			fmt.Println(*datum.VarCharValue)
		}
	}
	return nil
}
