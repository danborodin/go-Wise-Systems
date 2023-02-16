package main

import "consumer/broker"

func main() {
	kafkaHost := "kafka:9092"
	kafkaTopic := "test-topic"

	consumer := broker.NewConsumer(kafkaHost, kafkaTopic)
	defer consumer.Close()

	broker.Run(consumer)
}
