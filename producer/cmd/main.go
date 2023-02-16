package main

import (
	"log"
	"producer/broker"
)

func main() {
	kafkaHost := "kafka:9092"
	kafkaTopic := "test-topic"

	err := broker.CreateTopic(kafkaHost, kafkaTopic)
	if err != nil {
		log.Fatal(err)
	}

	producer := broker.NewProducer(kafkaHost, kafkaTopic)
	defer producer.Close()

	broker.Run(producer)
}
