package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	rmq "m3.dataloader/rabbitMQ"
)

var (
	prod *rmq.Producer
)

func main() {
	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()
	log.Println("producer created")

	// Listen to taggings of the type 2 (timestamp) and also to media.* messages
	rmq.Listen("tagging.*.2.*", processMessage)
	// rmq.Listen("media.*", processMessage)
}

// extractTimestamp tries to get a valid time.Time from the message.
// It first checks for a "taggingValue" field (format "2006-01-02 15:04:05"),
// then falls back to "local_time" (format "2006-01-02_15:04").
func extractTimestamp(message map[string]string) (time.Time, error) {
	log.Printf("Extracting timestamp from message: %v", message)
	// Try taggingValue first (standard tagging.*.2.* format)
	if tag, ok := message["taggingValue"]; ok {
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02 15:04",
			"UTC_2006-01-02_15:04:05",
			"UTC_2006-01-02_15:04",
		}
		for _, layout := range formats {
			t, err := time.Parse(layout, tag)
			if err == nil {
				return t, nil
			}
		}
		log.Printf("taggingValue present but failed to parse with any known format: %s", tag)
	}

	// Fallback: try local_time field (media.* format, e.g. "2015-02-23_00:00")
	if localTime, ok := message["local_time"]; ok {
		// Replace underscore separator with a space for standard parsing
		normalized := strings.Replace(localTime, "_", " ", 1)
		t, err := time.Parse("2006-01-02 15:04", normalized)
		if err == nil {
			return t, nil
		}
		log.Printf("local_time present but failed to parse: %v", err)
	}

	return time.Time{}, fmt.Errorf("no valid timestamp found in message (neither taggingValue nor local_time)")
}

func processMessage(d amqp.Delivery) {
	log.Println("new message")
	var message map[string]string
	err := json.Unmarshal(d.Body, &message)
	if err != nil {
		log.Printf("Failed to parse message body: %v", err)
		return
	}

	timestamp, err := extractTimestamp(message)
	if err != nil {
		log.Printf("Could not extract timestamp: %v", err)
		return
	}

	year := timestamp.Year()
	month := timestamp.Month()
	yearMonth := fmt.Sprintf("%d-%d", year, month)
	dateString := timestamp.Format("2006-01-02")

	hierarchy := fmt.Sprintf(`{
	"hierarchy": "Dates",
	"tagset": "Dates",
	"tagTypeId": 1,
	"tag": "Dates",
	"child": {
		"tagTypeId": %d,
		"tag": "%d",
		"tagset": "Year",
		"child": {
			"tag": "%s",
			"tagTypeId": 1,
			"tagset": "Year Month",
			"child": {
				"tag": "%s",
				"tagTypeId": 4,
				"tagset": "Date",
				"child": {}
				}
			}
		}
	}`, 5, year, yearMonth, dateString)

	// Publish the result
	rmq.PublishMessage(prod, hierarchy, "hierarchy")
}
