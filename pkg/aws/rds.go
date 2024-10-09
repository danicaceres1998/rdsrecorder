package aws

import (
	"context"
	"fmt"
	"time"

	"rdsrecorder/pkg/logger"
	phelper "rdsrecorder/pkg/processhelper"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

func CreateDBSnapshot(client RDSClient, dbName string, startAt time.Time) error {
	time.Sleep(time.Until(startAt)) // Wait until the start time

	if dbClusterIdentifier, ok := belongsToACluster(client, dbName); ok {
		r, err := client.CreateDBClusterSnapshot(&rds.CreateDBClusterSnapshotInput{
			DBClusterIdentifier:         &dbClusterIdentifier,
			DBClusterSnapshotIdentifier: awsSDK.String(buildSnapshotIdentifier(client.GetContext())),
			Tags:                        buildTagsSnapshot(),
		})
		if err != nil {
			return err
		}

		logger.Log(logger.Info, fmt.Sprintf("the snapshot is created, arn: %s", *r.DBClusterSnapshot.DBClusterSnapshotArn))
	} else {
		r, err := client.CreateDBSnapshot(&rds.CreateDBSnapshotInput{
			DBInstanceIdentifier: &dbName,
			DBSnapshotIdentifier: awsSDK.String(buildSnapshotIdentifier(client.GetContext())),
			Tags:                 buildTagsSnapshot(),
		})
		if err != nil {
			return err
		}

		logger.Log(logger.Info, fmt.Sprintf("the snapshot is created, arn: %s", *r.DBSnapshot.DBSnapshotArn))
	}

	return nil
}

// Private Functions //

func belongsToACluster(client RDSClient, dbIdentifier string) (string, bool) {
	dbInstances, err := client.DescribeDBInstances(
		&rds.DescribeDBInstancesInput{DBInstanceIdentifier: &dbIdentifier},
	)
	if err != nil {
		logger.Log(logger.Error, "unable to describe the DB", "error", err.Error())
		return "", false
	}
	if len(dbInstances.DBInstances) == 0 {
		logger.Log(logger.Info, "database not found", "dbIdentifier", dbIdentifier)
		return "", false
	}

	if dbCLI := dbInstances.DBInstances[0].DBClusterIdentifier; dbCLI != nil {
		return *dbCLI, true
	}

	return "", false
}

func buildSnapshotIdentifier(ctx context.Context) string {
	return fmt.Sprintf("pgreplay-%s", ctx.Value(phelper.ContextKeyPid))
}

func buildTagsSnapshot() []types.Tag {
	return []types.Tag{
		{Key: awsSDK.String("app"), Value: awsSDK.String("rdsrecorder")},
	}
}
