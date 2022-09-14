package main

import (
	"fmt"
	"os"

	"kafka-example/env"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := env.Topic

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     env.KafkaHost,
		"acks":                                  -1,   // (-1 == all)
		"retries":                               3,    // (default: 2147483647)
		"retry.backoff.ms":                      1000, // (deafult: 100)
		"max.in.flight.requests.per.connection": 5,    //(default: 1000000)
		"enable.idempotence":                    true,
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	userID := 0
	for i := 1; i <= env.ProduceCount; i++ {
		if i > userID+5 {
			userID += 5
		}

		msgVal := []byte(fmt.Sprintf(`{"user_id":%d, "trx_id":%d}`, userID, i))

		deliveryChan := make(chan kafka.Event)
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          msgVal,
			Key:            []byte(fmt.Sprint(userID)),
		},
			deliveryChan,
		)

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered msg %s\n", msgVal)
		}
		close(deliveryChan)
	}
}
