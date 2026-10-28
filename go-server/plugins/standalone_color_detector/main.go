package main

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	_ "image/jpeg" // registers the JPEG decoder for image.Decode
	_ "image/png"  // registers the PNG decoder for image.Decode
	"log"
	"math"
	"os"
	"time"

	md "media_downloader/gen/go"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	pb "m3.dataloader/dataloader"
	rmq "m3.dataloader/rabbitMQ"
)

const (
	maxGrpcRetries  = 3
	grpcRetryDelay  = 2 * time.Second
	downloadTimeout = 30 * time.Second
)

var prod *rmq.Producer
var downloader md.MediaDownloaderClient
var tagSet string = "Colors"

var defaultTagTypeId int64 = 1

// Tag type of tagSet... looked up once at startup
var tagTypeId int64

func main() {
	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()
	log.Println("producer created")

	// setup gRPC client to media-downloader as we need to analyze the image
	target := os.Getenv("MEDIA_DOWNLOADER_TARGET")
	if target == "" {
		target = "media-downloader:50052"
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to create media-downloader client for %s: %v", target, err)
	}
	defer conn.Close()

	downloader = md.NewMediaDownloaderClient(conn)
	log.Printf("media-downloader client created -> %s", target)

	// setup gRPC client to the dataloader (go-server) to look up the tag type of our tagset
	// (this was added to mimic the behavior of the exif_extractor plugin, which also needs to know the tag type of its tagset)
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

	rmq.Consume("color_detector", []string{"media.*"}, processMessage)
}

func processMessage(d amqp.Delivery) {
	log.Println("new message")
	var message map[string]string
	err := json.Unmarshal(d.Body, &message)
	if err != nil {
		log.Printf("Failed to parse message body: %v", err)
		return
	}

	// media.* messages are published by the server as {"ID", "MediaURI", "ThumbnailURI"}
	mediaId, ok := message["ID"]
	if !ok {
		log.Printf("Missing ID in message body")
		return
	}

	mediaURI, ok := message["MediaURI"]
	if !ok {
		log.Printf("Missing MediaURI in message body")
		return
	}

	mediaPath, err := requestMedia(mediaURI)
	if err != nil {
		log.Printf("Skipping media %s: %v", mediaId, err)
		return
	}
	defer releaseMedia(mediaURI)

	color, err := extractColor(mediaPath)
	if err != nil {
		log.Printf("Failed to extract color for media %s: %v", mediaId, err)
		return
	}

	body := fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, color, mediaId)

	// Publish the result
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.%d.%s", tagTypeId, tagSet))
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

func requestMedia(mediaURI string) (string, error) {
	var lastErr error
	for attempt := 1; attempt <= maxGrpcRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
		resp, err := downloader.RequestMedia(ctx, &md.RequestMediaRequest{MediaUri: mediaURI})
		cancel()
		if err == nil {
			return resp.GetMediaPath(), nil
		}
		if status.Code(err) == codes.NotFound {
			return "", fmt.Errorf("media not found: %s", mediaURI)
		}
		lastErr = err
		log.Printf("RequestMedia failed (attempt %d/%d) for %s: %v", attempt, maxGrpcRetries, mediaURI, err)
		if attempt < maxGrpcRetries {
			time.Sleep(grpcRetryDelay * time.Duration(attempt))
		}
	}
	return "", fmt.Errorf("could not download %s: %w", mediaURI, lastErr)
}

func releaseMedia(mediaURI string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := downloader.ReleaseMedia(ctx, &md.ReleaseMediaRequest{MediaUri: mediaURI}); err != nil {
		log.Printf("ReleaseMedia failed for %s: %v", mediaURI, err)
	}
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
