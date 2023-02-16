package broker

import (
	"context"
	"crypto/rand"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"log"
	"math/big"
	"strconv"
	"time"
)

type Event struct {
	Id   uuid.UUID
	Time uint64
}

func NewProducer(host, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(host),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	}
}

func Run(w *kafka.Writer) {
	ticker := time.NewTicker(time.Second)

	getMsg := messages()

	for {
		msg := getMsg()

		select {
		case <-ticker.C:
			err := w.WriteMessages(context.Background(), msg)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("wrote msg with key: ", string(msg.Key))
			// can be added a stop signal in the select
		}
	}
}

func CreateTopic(host, topic string) error {
	_, err := kafka.DialLeader(context.Background(), "tcp", host, topic, 0)
	return err
}

func newEvent() *Event {
	id := uuid.New()
	max := new(big.Int)
	max.SetInt64(9999999)
	time, err := rand.Int(rand.Reader, max)
	if err != nil {
		log.Fatal(err)
	}

	e := Event{
		Id:   id,
		Time: time.Uint64(),
	}

	return &e
}

func messages() func() kafka.Message {
	var key uint64 = 1

	return func() kafka.Message {
		key++
		event := newEvent()
		body := struct {
			Id   string
			Time string
		}{
			Id:   event.Id.String(),
			Time: strconv.Itoa(int(event.Time)),
		}

		encodedBody, err := kafka.Marshal(body)
		if err != nil {
			log.Fatal(err)
		}
		msg := kafka.Message{
			Key:   []byte(strconv.Itoa(int(key))),
			Value: encodedBody,
		}

		return msg
	}
}
