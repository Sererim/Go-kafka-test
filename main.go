package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type Config struct {
	BootStrap   string
	Topic       string
	Mode        string
	MessageFile string
}

type Message struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	cfg := parse_flags()
	validate_configs(cfg)

	switch cfg.Mode {
	case "producer":
		producer_messages(cfg)
	case "consumer":
		consumer_messages(cfg)
	default:
		log.Fatalf("Invalid mode: %s. Use 'producer' or 'consumer'", cfg.Mode)
	}
}

func parse_flags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.BootStrap, "bootstrap-server", "localhos:9092",
		"Kafka bootstrap server address")

	flag.StringVar(&cfg.Topic, "topic", "dead_letter_topic",
		"Kafka topic")

	flag.StringVar(&cfg.Mode, "mode", "consumer",
		"Client mode: `producer` or `consumer`")

	flag.StringVar(&cfg.MessageFile, "message-file", "message.json",
		"Path to JSON file with a message.")

	flag.Parse()
	return cfg
}

func validate_configs(cfg *Config) {
	if cfg.Mode == "" {
		log.Fatalf("Mode flag is required. Use `producer` or `consumer`")
	}
	if _, err := os.Stat(cfg.MessageFile); os.IsNotExist(err) && cfg.Mode == "producer" {
		log.Fatalf("Message file not found: %s", cfg.MessageFile)
	}
}

func producer_messages(cfg *Config) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{cfg.BootStrap}, config)
	if err != nil {
		log.Fatalf("Failted to create producer: %v", err)
	}
	defer producer.Close()

	messages := load_messages(cfg.MessageFile)
	for _, msg := range messages {
		kafkaMsg := &sarama.ProducerMessage{
			Topic: cfg.Topic,
			Key:   sarama.StringEncoder(msg.Key),
			Value: sarama.StringEncoder(msg.Value),
		}

		partition, offset, err := producer.SendMessage(kafkaMsg)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		log.Printf("Successfully sent message to partition %d at offset %d", partition, offset)
	}
}

func consumer_messages(cfg *Config) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{cfg.BootStrap}, config)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %v", err)
	}
	defer consumer.Close()

	part_consumer, err := consumer.ConsumePartition(cfg.Topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Fialed to start partition consumer: %v", err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Started consuming messages from topic %s", cfg.Topic)
	for {
		select {
		case msg := <-part_consumer.Messages():
			print_messages(msg)
		case err := <-part_consumer.Errors():
			log.Printf("Consumer error: %v", err)
		case <-sigchan:
			log.Printf("Shutting down consumer")
			return
		}
	}
}

func load_messages(filePath string) []Message {
	file, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading messages file: %v", err)
	}

	var messages []Message
	if err := json.Unmarshal(file, &messages); err != nil {
		log.Fatalf("Error on parsing JSON: %v", err)
	}

	return messages
}

func print_messages(msg *sarama.ConsumerMessage) {
	fmt.Printf("\nReceived message:\n")
	fmt.Printf("Topic:     %s\n", msg.Topic)
	fmt.Printf("Partition: %d\n", msg.Partition)
	fmt.Printf("Offset:    %d\n", msg.Offset)
	fmt.Printf("Key:       %s\n", string(msg.Key))
	fmt.Printf("Value:     %s\n", string(msg.Value))
	fmt.Println("-------------------------")
}
