package main

import (
	"log"

	"github.com/qw33ha/simpleService/handler"
	trpc "trpc.group/trpc-go/trpc-go"
	thttp "trpc.group/trpc-go/trpc-go/http"
	trpckafka "trpc.group/trpc-go/trpc-database/kafka"
)

const serviceName = "trpc.qw33ha.simpleService.http"

func main() {
	if err := handler.RegisterKafkaConfigFromEnv(); err != nil {
		log.Fatalf("configure Kafka: %v", err)
	}

	mysqlHandler := handler.NewMySQLHandler()
	kafkaProducer := handler.NewKafkaProducer()

	server := trpc.NewServer()
	serverHandler := handler.NewHTTPHandler(kafkaProducer, mysqlHandler)
	serverHandler.Register()

	thttp.RegisterNoProtocolService(server.Service(serviceName))
	trpckafka.RegisterKafkaConsumerService(server, handler.NewKafkaConsumer())

	log.Printf("starting %s HTTP server", serviceName)
	if err := server.Serve(); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
