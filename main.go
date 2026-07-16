package main

import (
	"os"
	"os/signal"
	"syscall"

	trpc "trpc.group/trpc-go/trpc-go"
	trpclog "trpc.group/trpc-go/trpc-go/log"
	thttp "trpc.group/trpc-go/trpc-go/http"
	trpckafka "trpc.group/trpc-go/trpc-database/kafka"
	"github.com/qw33ha/simpleService/handler"
)

const serviceName = "trpc.qw33ha.simpleService.http"

func main() {
	if err := handler.RegisterKafkaConfigFromEnv(); err != nil {
		trpclog.Fatalf("configure Kafka: %v", err)
	}

	mysqlHandler := handler.NewMySQLHandler()
	kafkaProducer := handler.NewKafkaProducer()

	s := trpc.NewServer()
	httpHandler := handler.NewHTTPHandler(mysqlHandler, kafkaProducer)
	httpHandler.Register()

	thttp.RegisterNoProtocolService(s.Service(serviceName))

	trpckafka.RegisterKafkaConsumerService(s, handler.NewKafkaConsumer())

	trpclog.Infof("starting %s trpc runtime", serviceName)
	if err := s.Serve(); err != nil {
		trpclog.Error(err)
	}
}

// waitForShutdown is unused but can be used for graceful shutdown if needed
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
