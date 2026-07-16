package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/qw33ha/simpleService/handler"
	trpc "trpc.group/trpc-go/trpc-go"
	trpclog "trpc.group/trpc-go/trpc-go/log"
	thttp "trpc.group/trpc-go/trpc-go/http"
	trpckafka "trpc.group/trpc-go/trpc-database/kafka"
)

const serviceName = "trpc.qw33ha.simpleService.http"

func main() {
	// Register Kafka configuration from environment variables
	if err := handler.RegisterKafkaConfigFromEnv(); err != nil {
		trpclog.Fatalf("configure Kafka: %v", err)
	}

	// Initialize MySQL handler (client)
	mysqlHandler := handler.NewMySQLHandler()

	// Create tRPC server
	s := trpc.NewServer()

	// Register Kafka consumer service
	trpckafka.RegisterKafkaConsumerService(s, handler.NewKafkaConsumer())

	// Create HTTP handler with dependencies
	httpHandler := handler.NewHTTPHandlerWithDeps(handler.NewKafkaProducer(), mysqlHandler)
	httpHandler.Register()

	// Register HTTP service
	thttp.RegisterNoProtocolService(s.Service(serviceName))

	// Start server
	go func() {
		trpclog.Infof("starting %s trpc runtime", serviceName)
		if err := s.Serve(); err != nil {
			trpclog.Error(err)
		}
	}()

	// Wait for shutdown signal
	waitForShutdown()
	trpclog.Info("shutting down server")
}

func waitForShutdown() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}
