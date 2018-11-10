import file_transfer_pb2
import file_transfer_pb2_grpc
import grpc
from concurrent import futures
import time
from collections import deque
from threading import Thread

mq = deque([])

class DataTransferServiceProxy(file_transfer_pb2_grpc.DataTransferServiceServicer):
    def __init__(self):
        pass

    def DownloadFile(self, fileInfo, context):
        fileName = fileInfo.fileName
        stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("localhost:4000"))
        resps = stub.DownloadFile(file_transfer_pb2.FileInfo(fileName=fileName))
        for resp in resps:
            yield file_transfer_pb2.FileMetadata(fileName=fileName, data=resp.data)

    def UploadFile(self, fileMetadata_iterator, context):
        global mq
        # stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("localhost:4000"))
        # fileInfo = stub.UploadFile(fileMetadata_iterator)
        # list_chunks = gen_stream(fileMetadata_iterator)
        # mq.append(list_chunks)
        for chunk in fileMetadata_iterator:
            mq.append(chunk)
        return file_transfer_pb2.Empty()
    
    def UploadFile1(self, request, context):
        pass

# def gen_stream(list_of_chunks):
#     for chunk in list_of_chunks:
#         yield chunk

def serve():
    file_transfer = DataTransferServiceProxy()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    file_transfer_pb2_grpc.add_DataTransferServiceServicer_to_server(file_transfer, server)
    server.add_insecure_port("localhost:3000")
    server.start()
    while True:
        time.sleep(86400)


def client():
    global mq
    stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("localhost:4000"))
    
    while True:
        if len(mq) == 0:
            continue
        else:
            stub.UploadFile1(mq.popleft())

if __name__ == "__main__":
    t1 = Thread(target=serve)
    t2 = Thread(target=client)

    t1.start()
    t2.start()