package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	rmq "m3.dataloader/rabbitMQ"
)

var (
	prod          *rmq.Producer
	cityTagset    string = "City (string)"
	countryTagset string = "Country (string)"
	poiTagset     string = "Point of interest (string)"
	username      string = "gwendalt"
)

func main() {
	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()
	log.Println("producer created")

	// Listen only to taggings of the type 2 (timestamp) (ignoring if they are already added to the database or not)
	rmq.Listen("tagging.*.1.Location", processMessage)
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

	tag, ok := message["taggingValue"]
	if !ok {
		log.Printf("Missing taggingValue in message body")
		return
	}

	// Extract latitude and longitude from the tag
	var lat, long string
	coords := strings.Split(tag, " ")
	if len(coords) == 2 {
		lat = coords[0]
		long = coords[1]
	} else {
		log.Printf("Invalid taggingValue format: %s", tag)
		return
	}

	cityResponse, err := http.Get(fmt.Sprintf("http://api.geonames.org/findNearbyPostalCodesJSON?lat=%s&lng=%s&username=%s", lat, long, username))
	if err != nil {
		log.Printf("Failed to get city information: %v", err)
		return
	}
	defer cityResponse.Body.Close()

	var geoNames struct {
		PostalCodes []struct {
			PlaceName string `json:"placeName"`
		} `json:"postalCodes"`
	}
	body, err := io.ReadAll(cityResponse.Body)
	if err != nil {
		log.Printf("Failed to read city response body: %v", err)
		return
	}

	err = json.Unmarshal(body, &geoNames)
	if err != nil {
		log.Printf("Failed to parse city response JSON: %v", err)
		return
	}

	// Extract the placeName from the first postal code entry
	if len(geoNames.PostalCodes) > 0 {
		cityName := geoNames.PostalCodes[0].PlaceName
		log.Printf("Extracted city name: %s", cityName)

		// Publish the result
		body := fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, cityName, mediaId)
		rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", cityTagset))
	} else {
		log.Printf("No city information found in the response")
	}

	countryResponse, err := http.Get(fmt.Sprintf("http://api.geonames.org/countryCodeJSON?lat=%s&lng=%s&username=%s", lat, long, username))
	if err != nil {
		log.Printf("Failed to get country information: %v", err)
		return
	}
	defer countryResponse.Body.Close()
	var countryInfo struct {
		CountryCode string `json:"countryCode"`
		CountryName string `json:"countryName"`
	}
	body, err = io.ReadAll(countryResponse.Body)
	if err != nil {
		log.Printf("Failed to read country response body: %v", err)
		return
	}
	log.Printf("Country response body: %s", body)

	err = json.Unmarshal(body, &countryInfo)
	if err != nil {
		log.Printf("Failed to parse country response JSON: %v", err)
		return
	}

	// Check if the country name is empty
	if countryInfo.CountryName == "" {
		log.Printf("No country information found in the response")
		return
	}

	// Publish the result
	responseBody := fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, countryInfo.CountryName, mediaId)
	rmq.PublishMessage(prod, responseBody, fmt.Sprintf("tagging.not_added.1.%s", countryTagset))
	log.Printf("Extracted country name: %s", countryInfo.CountryName)

	poiResponse, err := http.Get(fmt.Sprintf("http://api.geonames.org/findNearbyJSON?lat=%s&lng=%s&username=%s", lat, long, username))
	if err != nil {
		log.Printf("Failed to get point of interest information: %v", err)
		return
	}
	defer poiResponse.Body.Close()
	var poiData struct {
		Geonames []struct {
			Name string `json:"name"`
		} `json:"geonames"`
	}
	poiBody, err := io.ReadAll(poiResponse.Body)
	if err != nil {
		log.Printf("Failed to read point of interest response body: %v", err)
		return
	}

	err = json.Unmarshal(poiBody, &poiData)
	if err != nil {
		log.Printf("Failed to parse point of interest response JSON: %v", err)
		return
	}

	if len(poiData.Geonames) > 0 {
		poiName := poiData.Geonames[0].Name
		log.Printf("Extracted point of interest name: %s", poiName)

		// Publish the result
		poiMsg := fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, poiName, mediaId)
		rmq.PublishMessage(prod, poiMsg, fmt.Sprintf("tagging.not_added.1.%s", poiTagset))
	} else {
		log.Printf("No point of interest information found in the response")
	}
}
