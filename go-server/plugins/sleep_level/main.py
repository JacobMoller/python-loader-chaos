import json
import os

import grpc
import rabbitMQ_helpers
import dataloader_pb2
import dataloader_pb2_grpc

GRPC_HOST = os.getenv("GRPC_HOST", "go-server")
GRPC_PORT = os.getenv("GRPC_PORT", "50051")

TAGSET_NAME = "Sleep level"


# ———————————————————————————————————————————
# Hierarchy builder
# ———————————————————————————————————————————

def build_hierarchy(raw_text):
    """Parse sleep level lines and return the hierarchy dict."""
    hier = {
        "tagset": TAGSET_NAME,
        "name": TAGSET_NAME,
    }

    root = {
        "tag": "Sleep Level",
        "children": [],
    }

    parent = ""
    node = {}

    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split("/")
        current_parent = parts[0].strip()

        if current_parent != parent:
            if parent != "":
                root["children"].append(node)
            parent = current_parent
            node = {"tag": parent, "children": []}

        if len(parts) > 1:
            leaf = {"tag": parts[1].strip(), "children": []}
            node["children"].append(leaf)

    if node:
        root["children"].append(node)

    hier["rootnode"] = root
    return hier


# ———————————————————————————————————————————
# gRPC
# ———————————————————————————————————————————

def get_grpc_stub():
    """Create and return a gRPC stub for the DataLoader service."""
    channel = grpc.insecure_channel(f"{GRPC_HOST}:{GRPC_PORT}")
    stub = dataloader_pb2_grpc.DataLoaderStub(channel)
    return stub, channel


_tag_type_id_cache = {}


def get_tag_type_id(stub, tagset_name):
    """
    Query the gRPC server to get the tagTypeId for a given tagset name.
    Results are cached to avoid redundant gRPC calls.
    Returns the tagTypeId if found, None otherwise.
    """
    if tagset_name in _tag_type_id_cache:
        return _tag_type_id_cache[tagset_name]

    try:
        response = stub.getTagSetByName(
            dataloader_pb2.GetTagSetRequestByName(name=tagset_name)
        )
        _tag_type_id_cache[tagset_name] = response.tagTypeId
        return response.tagTypeId
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            print(f"Tagset '{tagset_name}' not found in database, skipping.")
        else:
            print(f"gRPC error while fetching tagset '{tagset_name}': {e}")
        _tag_type_id_cache[tagset_name] = None
        return None


# ———————————————————————————————————————————
# Callback
# ———————————————————————————————————————————

def callback(ch, method, properties, body, grpc_stub):
    """Handle incoming raw sleep level data from RabbitMQ."""
    raw_text = body.decode("utf-8")
    print(f"Received sleep level data ({len(raw_text)} bytes)")

    # Build hierarchy from received data
    hierarchy = build_hierarchy(raw_text)

    # Resolve tagTypeId for the tagset
    tag_type_id = get_tag_type_id(grpc_stub, TAGSET_NAME)
    if tag_type_id is None:
        print(f"Cannot resolve tagTypeId for '{TAGSET_NAME}', skipping.")
        return

    # Publish hierarchy result
    routing_key = f"hierarchy.output.{tag_type_id}.sleep_level"
    rabbitMQ_helpers.publish_message(ch, routing_key, json.dumps(hierarchy))
    print(f"Published hierarchy to '{routing_key}':")
    print(json.dumps(hierarchy, indent=4))
    print("Finished processing the message")


# ———————————————————————————————————————————
# Main
# ———————————————————————————————————————————

def main():
    print("Starting Sleep Level Hierarchy Plugin...", flush=True)
    print("Connecting to RabbitMQ...", flush=True)

    # Connect to RabbitMQ
    channel, connection = rabbitMQ_helpers.producer_connection_init()

    # Connect to gRPC server
    grpc_stub, grpc_channel = get_grpc_stub()
    print(f"Connected to gRPC server at {GRPC_HOST}:{GRPC_PORT}", flush=True)

    # Listen for raw sleep level data
    rabbitMQ_helpers.listen(
        "tagging.*.*.Sleep Level",
        lambda ch, method, properties, body: callback(
            ch, method, properties, body, grpc_stub
        ),
    )

    grpc_channel.close()
    rabbitMQ_helpers.connection_end(connection, channel)


if __name__ == "__main__":
    main()
