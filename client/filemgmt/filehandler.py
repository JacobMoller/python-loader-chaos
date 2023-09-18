import grpc_client

class FileHandler:
# Abstract class for file handlers (CSV and JSON)
    def __init__(self) -> None:
        self.client  = grpc_client.LoaderClient()