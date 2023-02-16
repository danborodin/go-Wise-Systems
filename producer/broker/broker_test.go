package broker

import (
	"testing"
)

func TestCreateTopic(t *testing.T) {
	var want error = nil

	kafkaHost := "localhost:9092"
	kafkaTopic := "test-topic"

	var got = CreateTopic(kafkaHost, kafkaTopic)
	if want != got {
		t.Fatal("failed to create topic")
	}
}
