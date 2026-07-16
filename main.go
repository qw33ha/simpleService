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
	if err := handler.RegisterKafkaConfigFromEnv(); err != nil {
		trpclog.Fatalf("configure Kafka: %v", err)
	}

	// Initialize MySQL handler
	mysqlHandler := handler.NewMySQLHandler()

	// Initialize Kafka producer
	kafkaProducer := handler.NewKafkaProducer()

	// Create HTTP handler with dependencies
	httpHandler := handler.NewHTTPHandler(kafkaProducer, mysqlHandler)
	httpHandler.Register()

	// Create tRPC server
	s := trpc.NewServer()

	// Register Kafka consumer
	trpckafka.RegisterKafkaConsumerService(s, handler.NewKafkaConsumer())

	// Register HTTP service
	thttp.RegisterNoProtocolService(s.Service(serviceName))

	// Start server
	trpclog.Infof("starting %s trpc runtime", serviceName)
	go func() {
		if err := s.Serve(); err != nil {
			trpclog.Error(err)
		}
	}()

	waitForShutdown()
	trpclog.Info("shutting down server")
}

func waitForShutdown() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
