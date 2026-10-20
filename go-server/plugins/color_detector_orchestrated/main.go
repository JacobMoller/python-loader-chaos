package main

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"log"
	"math"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	pb "m3.dataloader/dataloader"
	rmq "m3.dataloader/rabbitMQ"
)

// Name sent to the media-orchestrator in the ACK, used in its logs
const pluginName = "color"

var prod *rmq.Producer

var tagSet string = "Colors (orchestrated)"

var defaultTagTypeId int64 = 1

// Tag type of tagSet... looked up once at startup
var tagTypeId int64

func main() {
	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()
	log.Println("producer created")

	// setup gRPC client to the dataloader (go-server) to look up the tag type of our tagset
	grpcHost := os.Getenv("GRPC_HOST")
	if grpcHost == "" {
		grpcHost = "go-server"
	}
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}
	dataloaderTarget := fmt.Sprintf("%s:%s", grpcHost, grpcPort)
	dataloaderConn, err := grpc.NewClient(dataloaderTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to create dataloader client for %s: %v", dataloaderTarget, err)
	}
	tagTypeId, err = getTagTypeId(pb.NewDataLoaderClient(dataloaderConn), tagSet)

	// The dataloader is only needed for this lookup
	dataloaderConn.Close()
	if err != nil {
		log.Fatalf("Failed to get tag type: %v", err)
	}

	// The media-orchestrator downloads the media and dispatches it on process.color
	rmq.Listen("process.color", processMessage)
}

func processMessage(d amqp.Delivery) {
	log.Println("new message")
	var message map[string]string
	err := json.Unmarshal(d.Body, &message)
	if err != nil {
		log.Printf("Failed to parse message body: %v", err)
		return
	}

	// process.* messages are published by the media-orchestrator as {"ID", "MediaURI", "MediaPath"}
	mediaId, ok := message["ID"]
	if !ok {
		log.Printf("Missing ID in message body")
		return
	}

	// Always ACK once we know the media, even on failure, otherwise the
	// orchestrator waits for its timeout before moving on to the next media
	defer sendAck(mediaId)

	mediaPath, ok := message["MediaPath"]
	if !ok {
		log.Printf("Missing MediaPath in message body")
		return
	}

	color, err := extractColor(mediaPath)
	if err != nil {
		log.Printf("Failed to extract color for media %s: %v", mediaId, err)
		return
	}

	body := fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, color, mediaId)

	// Publish the result
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.%d.%s", tagTypeId, tagSet))
}

// sendAck tells the media-orchestrator that this plugin is done with the media
func sendAck(mediaId string) {
	body := fmt.Sprintf(`{"mediaID": "%s", "plugin": "%s"}`, mediaId, pluginName)
	rmq.PublishMessage(prod, body, fmt.Sprintf("process.done.%s", mediaId))
}

func getTagTypeId(dataloader pb.DataLoaderClient, tagsetName string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tagset, err := dataloader.GetTagSetByName(ctx, &pb.GetTagSetRequestByName{Name: tagsetName})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			log.Printf("Tagset %q not found, using default tag type %d", tagsetName, defaultTagTypeId)
			return defaultTagTypeId, nil
		}
		return 0, fmt.Errorf("could not look up tagset %q: %w", tagsetName, err)
	}

	log.Printf("Tagset %q has tag type %d", tagsetName, tagset.GetTagTypeId())
	return tagset.GetTagTypeId(), nil
}

// Domain specific code
func extractColor(mediaPath string) (string, error) {
	file, err := os.Open(mediaPath)
	if err != nil {
		return "", fmt.Errorf("could not open image %q: %w", mediaPath, err)
	}
	defer file.Close()

	img, _, err := image.Decode(file)
	if err != nil {
		return "", fmt.Errorf("could not decode image %q: %w", mediaPath, err)
	}

	var totalSaturation float64
	var pixelCount int

	bounds := img.Bounds()

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			ir, ig, ib, _ := img.At(x, y).RGBA()

			r := float64(ir) / 65535
			g := float64(ig) / 65535
			b := float64(ib) / 65535

			max := math.Max(r, math.Max(g, b))
			min := math.Min(r, math.Min(g, b))

			if max > 0 {
				// Add normalized saturation to totalSaturation
				totalSaturation += (max - min) / max
			}
			pixelCount++

		}
	}

	if pixelCount == 0 {
		return "", fmt.Errorf("image has no pixels")
	}

	averageSaturation := totalSaturation / float64(pixelCount)

	color := "Color"

	if averageSaturation < 0.05 {
		color = "Black&White"
	}

	return color, nil
}
