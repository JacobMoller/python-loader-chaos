package main

import (
	"encoding/json"
	"log"
	"math"

	amqp "github.com/rabbitmq/amqp091-go"
	rmq "m3.dataloader/rabbitMQ"
)

// Speed thresholds in km/h.
// Each entry is {upper bound (inclusive), label}.
// The last entry uses math.MaxFloat64 as a catch-all.
var activityThresholds = []struct {
	upperBound float64
	label      string
}{
	{7, "walking"},
	{30, "running"},
	{150, "car"},
	{400, "train"},
	{math.MaxFloat64, "plane"},
}

var prod *rmq.Producer

func main() {
	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()
	log.Println("Activity classifier started")

	// Listen to all media.* messages
	rmq.Listen("tagging.*.1.Speed", processMessage)
}

func processMessage(d amqp.Delivery) {
	var message map[string]interface{}
	if err := json.Unmarshal(d.Body, &message); err != nil {
		log.Printf("Failed to parse message body: %v", err)
		return
	}

	mediaID, ok := message["ID"].(string)
	if !ok || mediaID == "" {
		log.Printf("Missing or invalid ID in message")
		return
	}

	// Speed field absent or not a number – skip silently
	rawSpeed, exists := message["speed"]
	if !exists || rawSpeed == nil {
		log.Printf("No speed field in message for ID=%s, skipping", mediaID)
		return
	}

	speed, ok := rawSpeed.(float64)
	if !ok || speed <= 0 {
		log.Printf("Invalid or zero speed for ID=%s, skipping", mediaID)
		return
	}

	activity := classifyActivity(speed)
	log.Printf("ID=%s speed=%.2f km/h -> %s", mediaID, speed, activity)

	payload, err := json.Marshal(map[string]string{
		"taggingValue": activity,
		"mediaID":      mediaID,
	})
	if err != nil {
		log.Printf("Failed to marshal payload for ID=%s: %v", mediaID, err)
		return
	}

	rmq.PublishMessage(prod, string(payload), "tagging.not_added.1.Activity Classification")
}

// classifyActivity returns the activity label matching the given speed (km/h).
func classifyActivity(speedKmh float64) string {
	for _, t := range activityThresholds {
		if speedKmh <= t.upperBound {
			return t.label
		}
	}
	return "unknown"
}
