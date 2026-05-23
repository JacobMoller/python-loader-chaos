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
	prod                  *rmq.Producer
	timeOfDayTagset       string = "Time of day (string)"
	dayOfWeekStringTagset string = "Day of week (string)"
	dayOfWeekNumberTagset string = "Day of week (number)"
	dayWithinMonthTagset  string = "Day within month"
	dayWithinYearTagset   string = "Day within year"
	monthNumberTagset     string = "Month (number)"
	monthStringTagset     string = "Month (string)"
	yearTagset            string = "Year (number)"
	yearMonthTagset       string = "Year Month"
	timeTagset            string = "Time"
	dateTagset            string = "Date"
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
		// Try multiple known formats
		formats := []string{
			"2006-01-02 15:04:05",     // standard timestamp
			"2006-01-02 15:04",        // without seconds
			"UTC_2006-01-02_15:04:05", // UTC-prefixed with seconds
			"UTC_2006-01-02_15:04",    // UTC-prefixed without seconds
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

	mediaId, ok := message["mediaID"]
	if !ok {
		log.Printf("Missing mediaID in message body")
		return
	}

	timestamp, err := extractTimestamp(message)
	if err != nil {
		log.Printf("Could not extract timestamp: %v", err)
		return
	}

	log.Printf("Message received : mediaID: %s, timestamp: %s", mediaId, timestamp)

	// Determine the time of day
	hour := timestamp.Hour()
	var timeOfDay string
	switch {
	case hour >= 5 && hour < 12:
		timeOfDay = "Morning"
	case hour >= 12 && hour < 17:
		timeOfDay = "Afternoon"
	case hour >= 17 && hour < 21:
		timeOfDay = "Evening"
	default:
		timeOfDay = "Night"
	}

	// Publish the result
	body := fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, timeOfDay, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", timeOfDayTagset))

	dayOfWeek := timestamp.Weekday().String()

	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, dayOfWeek, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", dayOfWeekStringTagset))

	dayOfWeekNumber := timestamp.Weekday()
	body = fmt.Sprintf(`{"taggingValue": "%d", "mediaID": "%s"}`, int(dayOfWeekNumber), mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", dayOfWeekNumberTagset))

	dayWithinMonth := timestamp.Day()
	body = fmt.Sprintf(`{"tagset": "%s", "taggingValue": "%d", "mediaID": "%s"}`, dayWithinMonthTagset, dayWithinMonth, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", dayWithinMonthTagset))

	dayWithinYear := timestamp.YearDay()
	body = fmt.Sprintf(`{"taggingValue": "%d", "mediaID": "%s"}`, dayWithinYear, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", dayWithinYearTagset))

	month := timestamp.Month()
	body = fmt.Sprintf(`{"taggingValue": "%d", "mediaID": "%s"}`, month, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", monthNumberTagset))

	monthString := month.String()
	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, monthString, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", monthStringTagset))

	year := timestamp.Year()
	body = fmt.Sprintf(`{"taggingValue": "%d", "mediaID": "%s"}`, year, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", yearTagset))

	yearMonth := fmt.Sprintf("%d-%d", year, month)
	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, yearMonth, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", yearMonthTagset))

	timeString := timestamp.Format("15:04:05")
	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, timeString, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.3.%s", timeTagset))

	dateString := timestamp.Format("2006-01-02")
	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, dateString, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.4.%s", dateTagset))

}
