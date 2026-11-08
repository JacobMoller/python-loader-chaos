module color_detector

go 1.24.2

require (
	github.com/rabbitmq/amqp091-go v1.10.0
	google.golang.org/grpc v1.73.0
	m3.dataloader v0.0.0-00010101000000-000000000000
	media_downloader v0.0.0-00010101000000-000000000000
)

require (
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250324211829-b45e905df463 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace m3.dataloader => ../..

replace media_downloader => ../../media_downloader
