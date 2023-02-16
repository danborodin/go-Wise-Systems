package broker

import (
	"consumer/window"
	"context"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"log"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	Id   uuid.UUID
	Time uint64
}

func NewConsumer(host, topic string) *kafka.Reader {
	brokers := strings.Split(host, ",")
	config := kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
	}
	return kafka.NewReader(config)
}

func Run(r *kafka.Reader) {

	tw := window.NewTumblingWindow(time.Second * 5)
	defer tw.Stop()
	in := tw.In()
	out := tw.Out()

	go func() {
		for m := range out {
			msg, ok := m.([]interface{})
			if ok {
				for _, v := range msg {
					e, ok := v.(uint64)
					if ok {
						log.Println("time:", e)
					}
				}
			}
		}
	}()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		e := newEventFromMsg(m)

		in <- e.Time
	}

}

func newEventFromMsg(m kafka.Message) Event {
	var e Event

	body := struct {
		Id   string
		Time string
	}{}

	err := kafka.Unmarshal(m.Value, &body)
	if err != nil {
		log.Fatal(err)
	}
	err = e.Id.UnmarshalText([]byte(body.Id))
	if err != nil {
		log.Fatal(err)
	}
	t, err := strconv.Atoi(body.Time)
	if err != nil {
		log.Fatal(err)
	}
	e.Time = uint64(t)

	return e
}
