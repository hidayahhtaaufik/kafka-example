package main

import (
	"fmt"
	"kafka-example/env"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var wg sync.WaitGroup

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             env.KafkaHost,
		"group.id":                      env.Group,
		"enable.auto.commit":            false,
		"partition.assignment.strategy": "cooperative-sticky", // c.IncrementalAssign() must be implemented
		//"go.application.rebalance.enable": true, // c.Assign() c.Unassign() must be implemented
		"auto.offset.reset": "latest",
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	serviceCh := make(chan struct{})

	err = consumer.SubscribeTopics([]string{env.Topic}, rebalanceCallback)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	wg.Add(1)

	go func() {
	loopConsume:
		for {
			select {
			case <-serviceCh:
				break loopConsume
			default:
				ev := consumer.Poll(0)
				switch e := ev.(type) {
				case *kafka.Message:
					wg.Add(1)

					_, err := consumer.CommitMessage(e)
					if err == nil {
						processMsg(e.Value)
					}
					wg.Done()

				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
					break loopConsume
				}
			}
		}

		err = consumer.Close()
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println("Consumer Closed!")
		wg.Done()

		return
	}()

	<-stopSignal
	close(serviceCh)
	wg.Wait()

	fmt.Println("Shutdown gracefully!")

}

func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {

	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		if len(ev.Partitions) > 0 {
			// err := c.Assign(ev.Partitions)
			err := c.IncrementalAssign(ev.Partitions)
			if err != nil {
				fmt.Println(err)
			}

			fmt.Println("--ASSIGNED---")
			for _, v := range ev.Partitions {
				fmt.Printf("Partition %v \n", v.Partition)
			}
			fmt.Println("-------------")
		}
	case kafka.RevokedPartitions:
		// err := c.Unassign()
		err := c.IncrementalUnassign(ev.Partitions)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println("\n---REVOKED---")
		for _, v := range ev.Partitions {
			fmt.Printf("Partition %v \n", v.Partition)
		}
		fmt.Println("-------------")
	}

	return nil
}

func processMsg(msg []byte) error {

	fmt.Print(string(msg))
	// simulate some process
	time.Sleep(2 * time.Second)
	//
	fmt.Println("-> Processed!")

	return nil
}
