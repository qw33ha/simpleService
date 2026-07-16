package main

import (
	"os"
	"os/signal"
	"syscall"

	trpc "trpc.group/trpc-go/trpc-go"
	trpclog "trpc.group/trpc-go/trpc-go/log"
	trpcserver "trpc.group/trpc-go/trpc-go/server"
	thttp "trpc.group/trpc-go/trpc-go/http"
	trpckafka "trpc.group/trpc-go/trpc-database/kafka"
	"github.com/qw33ha/simpleService/handler"
)

const serviceName = "trpc.qw33ha.simpleService.http"

func main() {
	if err := handler.RegisterKafkaConfigFromEnv(); err != nil {
		trpclog.Fatalf("configure Kafka: %v", err)
	}
	initDatabaseClients()
	s := trpc.NewServer()
	registerKafkaConsumers(s)
	httpHandler := handler.NewHTTPHandler()
	httpHandler.Register()
	thttp.RegisterNoProtocolService(s.Service("trpc.qw33ha.simpleService.http"))
	serveTRPC(s)
}

func initDatabaseClients() {
	_ = handler.NewMySQLHandler()
	// [LLM: inject the MySQL handler into the transport or business handlers that need persistence.]
}

func registerKafkaConsumers(s *trpcserver.Server) {
	trpckafka.RegisterKafkaConsumerService(s, handler.NewKafkaConsumer())
}

func serveTRPC(s *trpcserver.Server) {
	trpclog.Infof("starting %s trpc runtime", serviceName)
	if err := s.Serve(); err != nil {
		trpclog.Error(err)
	}
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