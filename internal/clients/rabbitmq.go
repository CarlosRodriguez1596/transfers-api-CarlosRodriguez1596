package clients

import (
	"fmt"
	"strings"
	"transfers-api/internal/config"
	"transfers-api/internal/logging"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	conn  *amqp.Connection
	queue string
}

type RabbitMQConsumer struct {
	conn  *amqp.Connection
	queue string
}

func NewRabbitMQClient(cfg config.RabbitMQ) *RabbitMQClient {
	conn, err := amqp.Dial(
		fmt.Sprintf(
			"amqp://%s:%s@%s:%d/",
			cfg.Username,
			cfg.Password,
			cfg.Hostname,
			cfg.Port,
		),
	)
	if err != nil {
		logging.Logger.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	return &RabbitMQClient{
		conn:  conn,
		queue: cfg.QueueName,
	}
}

func NewRabbitMQConsumer(cfg config.RabbitMQ) *RabbitMQConsumer {
	conn, err := amqp.Dial(
		fmt.Sprintf(
			"amqp://%s:%s@%s:%d/",
			cfg.Username,
			cfg.Password,
			cfg.Hostname,
			cfg.Port,
		),
	)
	if err != nil {
		logging.Logger.Fatalf("failed to connect to RabbitMQ: %v", err)
	}

	return &RabbitMQConsumer{
		conn:  conn,
		queue: cfg.QueueName,
	}
}

func (c *RabbitMQClient) Publish(operation string, transferID string) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		c.queue,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	body := fmt.Sprintf("%s:%s", operation, transferID)
	err = ch.Publish(
		"",      // exchange
		c.queue, // routing key (queue name)
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

func (c *RabbitMQConsumer) Consume() error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		c.queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("failed to set qos: %w", err)
	}

	msgs, err := ch.Consume(
		c.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	logging.Logger.Infof("waiting for messages from queue %s", c.queue)

	for msg := range msgs {
		body := string(msg.Body)
		logging.Logger.Infof("message received: %s", body)

		parts := strings.SplitN(body, ":", 2)
		if len(parts) != 2 {
			logging.Logger.Errorf("invalid message format: %s", body)
			_ = msg.Nack(false, false)
			continue
		}

		operation := parts[0]
		transferID := parts[1]

		switch operation {
		case "CREATE", "create":
			logging.Logger.Infof("processing CREATE for transfer %s", transferID)
		case "UPDATE", "update":
			logging.Logger.Infof("processing UPDATE for transfer %s", transferID)
		case "DELETE", "delete":
			logging.Logger.Infof("processing DELETE for transfer %s", transferID)
		default:
			logging.Logger.Errorf("unknown operation: %s", operation)
			_ = msg.Nack(false, false)
			continue
		}

		if err := msg.Ack(false); err != nil {
			return fmt.Errorf("failed to ack message: %w", err)
		}
	}

	return nil
}

func (c *RabbitMQConsumer) Close() error {
	return c.conn.Close()
}
