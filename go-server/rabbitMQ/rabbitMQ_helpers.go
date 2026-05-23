package rabbitMQ

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	rabbitMQHost = mustGetEnv("RABBITMQ_HOST")
	rabbitMQPort = mustGetEnvInt("RABBITMQ_PORT")
	rabbitMQUser = mustGetEnv("RABBITMQ_USER")
	rabbitMQPass = mustGetEnv("RABBITMQ_PASS")
	exchangeName = mustGetEnv("RABBITMQ_EXCHANGE_NAME")
)

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	return value
}

func mustGetEnvInt(key string) int {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	v, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("Environment variable %s must be an integer, got: %s", key, value)
	}
	return v
}

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	ctx     context.Context
	cancel  context.CancelFunc
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func ProducerConnexionInit() *Producer {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", rabbitMQUser, rabbitMQPass, rabbitMQHost, rabbitMQPort))
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return &Producer{conn, ch, ctx, cancel}
}

func PublishMessage(p *Producer, body, topic string) {
	err := p.channel.PublishWithContext(p.ctx,
		exchangeName, // exchange
		topic,        // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	log.Printf(" [%s] Sent %s", topic, body)
}

func (p *Producer) ConnexionEnd() {
	if err := p.conn.Close(); err != nil {
		log.Printf("Failed to close connection: %s", err)
	}
	if err := p.channel.Close(); err != nil {
		log.Printf("Failed to close channel: %s", err)
	}
	p.cancel()
}

func Listen(topic string, consumeAction func(amqp.Delivery)) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", rabbitMQUser, rabbitMQPass, rabbitMQHost, rabbitMQPort))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	log.Printf("Binding queue %s to exchange %s with routing key %s",
		q.Name, exchangeName, topic)
	err = ch.QueueBind(
		q.Name,       // queue name
		topic,        // routing key
		exchangeName, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			consumeAction(d)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
