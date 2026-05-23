import json
import os

import grpc
import rabbitMQ_helpers
import dataloader_pb2
import dataloader_pb2_grpc

# Fields that should NOT be published individually (handled separately as location)
LOCATION_FIELDS = {"latitude", "longitude", "lat", "lon"}

GRPC_HOST = os.getenv("GRPC_HOST", "go-server")
GRPC_PORT = os.getenv("GRPC_PORT", "50051")


def get_grpc_stub():
    """Create and return a gRPC stub for the DataLoader service."""
    channel = grpc.insecure_channel(f"{GRPC_HOST}:{GRPC_PORT}")
    stub = dataloader_pb2_grpc.DataLoaderStub(channel)
    return stub, channel


def get_tag_type_id(stub, tagset_name):
    """
    Query the gRPC server to get the tagTypeId for a given tagset name.
    Returns the tagTypeId if found, None otherwise.
    """
    try:
        response = stub.getTagSetByName(
            dataloader_pb2.GetTagSetRequestByName(name=tagset_name)
        )
        return response.tagTypeId
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            print(f"Tagset '{tagset_name}' not found in database, skipping.")
        else:
            print(f"gRPC error while fetching tagset '{tagset_name}': {e}")
        return None


def extract_location_keys(data):
    """
    Return the original keys for latitude and longitude, handling case-insensitive
    variations of: lat/lon or latitude/longitude.
    """
    data_lower = {key.lower(): key for key in data.keys()}

    lat_key = data_lower.get("lat") or data_lower.get("latitude")
    lon_key = data_lower.get("lon") or data_lower.get("longitude")

    return lat_key, lon_key


def main():
    print("Starting Metadata Extractor Plugin...", flush=True)
    print("Connecting to RabbitMQ...", flush=True)

    # Connect to RabbitMQ
    channel, connection = rabbitMQ_helpers.producer_connection_init()

    # Connect to gRPC server
    grpc_stub, grpc_channel = get_grpc_stub()
    print(f"Connected to gRPC server at {GRPC_HOST}:{GRPC_PORT}", flush=True)

    rabbitMQ_helpers.listen(
        "media.*",
        lambda ch, method, properties, body: callback(
            ch, method, properties, body, grpc_stub
        ),
    )

    grpc_channel.close()
    rabbitMQ_helpers.connection_end(connection, channel)


def callback(ch, method, properties, body, grpc_stub):
    """Handle incoming messages from RabbitMQ."""
    content = json.loads(body)
    media_id = content["ID"]
    data = content.get("Tags", {})

    print(f"Received message: {media_id}")

    if not data:
        print("No tags found in message.")
        print("Metadata processed...")
        print("Finished processing the message")
        return

    # Handle location separately: single publish for lat+lon / latitude+longitude,
    # case-insensitive while preserving original keys.
    lat_key, lon_key = extract_location_keys(data)
    if lat_key and lon_key:
        gps_latitude = data[lat_key]
        gps_longitude = data[lon_key]
        response = {
            "taggingValue": f"{gps_latitude} {gps_longitude}",
            "mediaID": media_id,
        }
        rabbitMQ_helpers.publish_message(
            ch,
            "tagging.not_added.1.Location",
            json.dumps(response),
        )
        print(
            f"Published GPS coordinates: {gps_latitude}, {gps_longitude} "
            f"for media ID: {media_id}"
        )

    # Publish all other tag fields individually
    for field_name, value in data.items():
        if field_name.lower() in LOCATION_FIELDS:
            continue

        tag_type_id = get_tag_type_id(grpc_stub, field_name)
        
        if tag_type_id is None:
            print(f"Skipping field '{field_name}': tagset not found")
            continue

        if tag_type_id==5:
            value=(int(value) if value.isdigit() else value)

        response = {
            "taggingValue": value,
            "mediaID": media_id,
        }
        routing_key = f"tagging.not_added.{tag_type_id}.{field_name}"
        rabbitMQ_helpers.publish_message(ch, routing_key, json.dumps(response))
        # print(
        #     f"Published tag '{field_name}' (type {tag_type_id}): {value} "
        #     f"for media ID: {media_id}"
        # )

    print("Metadata processed...")
    print("Finished processing the message")


if __name__ == "__main__":
    main()
