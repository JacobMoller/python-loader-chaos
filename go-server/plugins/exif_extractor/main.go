package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/dsoprea/go-exif/v3"
	exifcommon "github.com/dsoprea/go-exif/v3/common"
	amqp "github.com/rabbitmq/amqp091-go"
	rmq "m3.dataloader/rabbitMQ"
)

var (
	prod            *rmq.Producer
	timestampTagset string = "Timestamp UTC"
	locationTagset  string = "Location"
)

func main() {
	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()
	log.Println("producer created")

	// Listen only to taggings of the type 2 (timestamp) (ignoring if they are already added to the database or not)
	rmq.Listen("media.*", processMessage)
}

func processMessage(d amqp.Delivery) {
	log.Println("new message")
	var message map[string]string
	err := json.Unmarshal(d.Body, &message)
	if err != nil {
		log.Printf("Failed to parse message body: %v", err)
		return
	}

	mediaId, ok := message["ID"]
	if !ok {
		log.Printf("Missing mediaID in message body")
		return
	}
	mediaURI, ok := message["MediaURI"]
	if !ok {
		log.Printf("Missing mediaURI in message body")
		return
	}
	log.Printf("mediaURI: %s", mediaURI)

	rawExif, err := exif.SearchFileAndExtractExif(mediaURI)
	if err != nil {
		log.Printf("failed to extract EXIF data: %s", err)
	}

	im, err := exifcommon.NewIfdMappingWithStandard()
	if err != nil {
		log.Printf("failed to create IFD mapping: %s", err)
		return
	}

	ti := exif.NewTagIndex()

	_, index, err := exif.Collect(im, ti, rawExif)
	if err != nil {
		log.Printf("failed to collect EXIF data: %s", err)
		return
	}

	rootIfd := index.RootIfd

	tagEntries := rootIfd.DumpTags()
	fmt.Println("EXIF field names in", mediaURI)
	for _, entry := range tagEntries {
		fmt.Println(entry.TagName())
	}

	// We know the tag we want is on IFD0 (the first/root IFD).
	timestamp, err := getTagFromName(rootIfd, "DateTime")
	if err == nil {
		parsedTime, err := time.Parse("2006:01:02 15:04:05", timestamp)
		if err != nil {
			log.Printf("failed to parse timestamp: %s", err)
			return
		}
		formattedTimestamp := parsedTime.Format("2006-01-02 15:04:05")
		body := fmt.Sprintf(`{"tagset": "%s", "taggingValue": "%s", "mediaID": "%s"}`, timestampTagset, formattedTimestamp, mediaId)
		rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.2.%s", timestampTagset))
	}

	latitude, err := getTagFromName(rootIfd, "GPSLatitude")
	if err == nil {
		longitude, err := getTagFromName(rootIfd, "GPSLongitude")
		if err == nil {
			body := fmt.Sprintf(`{"tagset": "%s", "taggingValue": "%s %s", "mediaID": "%s"}`, locationTagset, latitude, longitude, mediaId)
			rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", locationTagset))

		}
	}
}

func getTagFromName(rootIfd *exif.Ifd, tagName string) (string, error) {
	results, err := rootIfd.FindTagWithName(tagName)
	if err != nil {
		log.Printf("failed to find tag with name %s: %s", tagName, err)
		return "", err
	}

	// This should never happen.
	if len(results) != 1 {
		log.Printf("there wasn't exactly one result")
		return "", fmt.Errorf("there wasn't exactly one result")
	}

	ite := results[0]

	valueRaw, err := ite.Value()
	if err != nil {
		log.Printf("failed to get value of tag: %s", err)
		return "", err
	}

	value := valueRaw.(string)
	return value, nil
}
