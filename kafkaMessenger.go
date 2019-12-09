package main

import (
	"log"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Client is a middleman between the websocket connection and the hub.
type KafkaClient struct {
	KafkaConsumer *kafka.Consumer

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client

	// Inbound messages from the clients.
	messages chan []byte

	// Registered clients.
	clients map[*Client]bool
}

func CreateConsumer(broker string, topic string) *kafka.Consumer {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "vernemq-group",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)
	log.Println("Consumer created")
	return c
}

func NewKafka(broker string, topic string) *KafkaClient {

	return &KafkaClient{
		KafkaConsumer: CreateConsumer(broker, topic),
		messages:      make(chan []byte),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		clients:       make(map[*Client]bool),
	}

}

func (h *KafkaClient) KafkaMessageRead() {
	log.Println("Running broker..")

	for {
		msg, err := h.KafkaConsumer.ReadMessage(-1)
		if err == nil {

			for client := range h.clients {
				select {
				case client.send <- msg.Value:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}

		} else {
			// The client will automatically try to recover from all errors.
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	h.KafkaConsumer.Close()
}

func (h *KafkaClient) Run() {
	log.Println("Running client listener..")

	for {
		select {
		case client := <-h.register:
			log.Println("New client registered..")
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
		}
	}
}
