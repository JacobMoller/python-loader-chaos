package rabbitMQ

// Helpers for the "standalone" plugin architecture.
//
// Every standalone plugin owns one durable, named queue (see QueueName) that it
// declares itself and binds to the shared topic exchange. Because each plugin
// has its own queue, every message published on the exchange is fanned out to
// all plugins whose bindings match, while replicas of the same plugin compete
// for the messages of that one queue.
//
// Producers are unchanged: they keep publishing to the exchange with
// PublishMessage and never need to know which queues exist.

import (
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	queuePrefix    = "standalone."
	reconnectDelay = 5 * time.Second
)

// QueueName returns the name of the queue owned by the given plugin.
func QueueName(plugin string) string {
	return queuePrefix + plugin
}

// DeclarePluginQueue declares the exchange, the plugin's durable queue and its
// bindings. Declarations are idempotent, so this is safe to run on every start.
func DeclarePluginQueue(ch *amqp.Channel, queueName string, bindingKeys []string) error {
	err := ch.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("declare exchange %s: %w", exchangeName, err)
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable: survives plugin restarts, messages wait for the plugin
		false,     // delete when unused
		false,     // exclusive: replicas of the plugin share the queue
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("declare queue %s: %w", queueName, err)
	}

	for _, key := range bindingKeys {
		log.Printf("Binding queue %s to exchange %s with routing key %s", queueName, exchangeName, key)
		if err := ch.QueueBind(queueName, key, exchangeName, false, nil); err != nil {
			return fmt.Errorf("bind queue %s to %s: %w", queueName, key, err)
		}
	}
	return nil
}

// Consume declares the plugin's queue and calls handler for each message, one
// at a time. A message is acknowledged once handler returns, so messages that
// were not fully handled (crash, lost connection) are redelivered.
// Reconnects forever and never returns.
func Consume(plugin string, bindingKeys []string, handler func(amqp.Delivery)) {
	queueName := QueueName(plugin)
	for {
		err := consumeOnce(queueName, bindingKeys, handler)
		log.Printf("Consumer on %s stopped: %v - reconnecting in %s", queueName, err, reconnectDelay)
		time.Sleep(reconnectDelay)
	}
}

func consumeOnce(queueName string, bindingKeys []string, handler func(amqp.Delivery)) error {
	// Named connection, shown in the management UI instead of only the client's ip:port
	props := amqp.NewConnectionProperties()
	props.SetClientConnectionName(queueName + " consumer")
	conn, err := amqp.DialConfig(
		fmt.Sprintf("amqp://%s:%s@%s:%d/", rabbitMQUser, rabbitMQPass, rabbitMQHost, rabbitMQPort),
		amqp.Config{Properties: props, Heartbeat: 10 * time.Second, Locale: "en_US"},
	)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if err := DeclarePluginQueue(ch, queueName, bindingKeys); err != nil {
		return err
	}

	// Only hand one unacknowledged message at a time to this consumer, the
	// rest stay in the queue (and go to other replicas, if any)
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set QoS: %w", err)
	}

	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto ack
		false,     // exclusive
		false,     // no local
		false,     // no wait
		nil,       // args
	)
	if err != nil {
		return fmt.Errorf("register consumer: %w", err)
	}

	log.Printf(" [*] Consuming from %s", queueName)
	for d := range msgs {
		handler(d)
		if err := d.Ack(false); err != nil {
			log.Printf("Failed to ack message from %s: %v", d.RoutingKey, err)
		}
	}
	return errors.New("delivery channel closed")
}
