package main

import (
	"context"
	"os"
	"time"

	"rdsrecorder/pkg/aws"
	"rdsrecorder/pkg/logger"
	"rdsrecorder/pkg/metrics"
	"rdsrecorder/pkg/process"
	pHelper "rdsrecorder/pkg/processhelper"

	kingpin "github.com/alecthomas/kingpin/v2"
)

var (
	app = kingpin.New("rdsrecorder", "Save Postgres logs/snapshots from AWS")

	// Global Flags applied to every command
	debug            = app.Flag("debug", "Enable debug logging").Default("false").Bool()
	startFlag        = app.Flag("start", "Actions from this time onward. Format("+pHelper.TimeStampFormat+")").String()
	finishFlag       = app.Flag("finish", "Stop actions at this time. Format("+pHelper.TimeStampFormat+")").String()
	bucketFlag       = app.Flag("bucket", "Bucket identifier name. Default value is obtained from AWS_S3_BUCKET_NAME env var").String()
	dbIdentifierFlag = app.Flag("db-identifier", "Database identifier name").String()
	metricsAddress   = app.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("0.0.0.0").String()
	metricsPort      = app.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9445").Uint16()

	// Commands
	sync     = app.Command("sync", "Save Logs for the period of time provided & take a snapshot at the start time")
	snapshot = app.Command("snapshot", "Take a Snapshot at the current time")
	pID      = app.Command("pid", "Create an PID for rdsrecorder")
)

func main() {
	setTimezone() // Always call this function first
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	if *debug {
		logger.EnableDebug()
	}

	// Process Config
	ctx, err := process.CreateContextWithPid(context.Background())
	if err != nil {
		logger.Log(logger.Fatal, "unable to create the context with the PID", "error", err.Error())
		return
	}
	logger.Log(logger.Info, "starting process", "pid", pHelper.GetProcessID(ctx))
	if pID.FullCommand() == command {
		return
	}

	// AWS credentials
	cfg, err := aws.VerifyAWSConfig(ctx)
	if err != nil {
		logger.Log(logger.Fatal, "the aws credentials are not valid", "error", err.Error())
		return
	}

	// Prometheus Server
	server := metrics.StartPrometheusServer(*metricsAddress, *metricsPort)

	switch command {
	case sync.FullCommand():
		err = process.StartSyncProcess(
			ctx, cfg,
			*dbIdentifierFlag,
			*startFlag, *finishFlag,
			*bucketFlag,
		)
	case snapshot.FullCommand():
		err = process.StartSnapshotProcess(ctx, cfg, *dbIdentifierFlag, *startFlag)
	default:
		logger.Log(logger.Fatal, "no command was provided")
	}

	if err := metrics.ShutdownServer(ctx, server); err != nil {
		logger.Log(logger.Error, "unable to shutdown the prometheus server", "err", err.Error())
	}
	if err != nil {
		logger.Log(logger.Fatal, "the process finished with an error", "error", err.Error())
	}
}

func setTimezone() {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		logger.Log(logger.Fatal, "unable to set UTC timezone globally")
	}

	time.Local = location
}
