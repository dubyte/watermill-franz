package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/dubyte/watermill-franz/pkg/franz"
)

func main() {
	c := franz.SubscriberConfig{Brokers: []string{"localhost:9092"}, Unmarshaler: franz.DefaultMarshaler{}}
	sub, err := franz.NewSubscriber(c, watermill.NewSlogLogger(slog.Default()))
	if err != nil {
		panic(err)
	}
	defer sub.Close()

	messages, err := sub.Subscribe(context.Background(), "test")
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		fmt.Printf("%s\n", msg.Payload)
	}
}
