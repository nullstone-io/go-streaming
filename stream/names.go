package stream

import (
	"fmt"
	"github.com/google/uuid"
)

func RunLogsStreamName(orgName string, stackId int64, runUid uuid.UUID) string {
	return fmt.Sprintf("runLogsStream:%s:%d:%s", orgName, stackId, runUid)
}

func DeployLogsStreamName(orgName string, stackId int64, deployId int64) string {
	return fmt.Sprintf("deployLogsStream:%s:%d:%d", orgName, stackId, deployId)
}

func DeploysStreamName(orgName string, stackId int64) string {
	return fmt.Sprintf("deploysStream:%s:%d", orgName, stackId)
}

func RunsStreamName(orgName string, stackId, blockId, envId int64) string {
	return fmt.Sprintf("runsStream:%s:%d:%d:%d", orgName, stackId, blockId, envId)
}

func WorkspacesStreamName(orgName string, stackId, blockId, envId int64) string {
	return fmt.Sprintf("workspacesStream:%s:%d:%d:%d", orgName, stackId, blockId, envId)
}
