package main

import (
	"log/slog"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dubyte/watermill-franz/pkg/franz"
)

func main() {
	c := franz.PublisherConfig{Brokers: []string{"localhost:9092"}, Marshaler: franz.DefaultMarshaler{}}
	pub, err := franz.NewPublisher(c, watermill.NewSlogLogger(slog.Default()))
	if err != nil {
		panic(err)
	}
	defer pub.Close()

	err = pub.Publish("test", message.NewMessage(watermill.NewUUID(), message.Payload([]byte("Hello world!"))))
	if err != nil {
		panic(err)
	}
}
