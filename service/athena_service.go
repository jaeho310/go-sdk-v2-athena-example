package service

import (
	"athena-example/gateway"
	"fmt"
)

func ExecuteQuery(query string) {
	executionId, err := gateway.StartQuery(query)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = gateway.WaitForFinish(executionId)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = gateway.ResultProcessing(executionId)
	if err != nil {
		fmt.Println(err)
		return
	}
}
