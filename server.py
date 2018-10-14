import sample_pb2
import sample_pb2_grpc
import grpc
from concurrent import futures
import time

ids = []
pair = {}
pairs = {}


class DataTransfer(sample_pb2_grpc.DataTransferServicer):
    def __init__(self):
        pass

    def sendMessage(self, request, context):
        return sample_pb2.Empty()

    def recvMessage(self, request, context):
        pass


def serve():
    chat = DataTransfer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    sample_pb2_grpc.add_DataTransferServicer_to_server(chat, server)
    server.add_insecure_port("localhost:3001")
    server.start()
    try:
        time.sleep(86400)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    serve()
