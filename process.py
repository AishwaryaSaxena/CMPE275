import sample_pb2
import sample_pb2_grpc
import grpc
from concurrent import futures
import time
import threading

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


def run():
    stub = sample_pb2_grpc.DataTransferStub(grpc.insecure_channel(
        "localhost:3002"))  # server on another client
    prev_mess = stub.recvMessage(sample_pb2.Empty()).mess
    try:
        while True:
            mess = stub.recvMessage(sample_pb2.Empty()).mess
            if mess != prev_mess:
                print(mess)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    t1 = threading.Thread(target=serve)
    t2 = threading.Thread(target=run)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
