import file_transfer_pb2
import file_transfer_pb2_grpc
import grpc
from concurrent import futures
import time

class DataTransferService(file_transfer_pb2_grpc.DataTransferServiceServicer):
    def __init__(self):
        pass

    def DownloadFile(self, fileInfo, context):
        fileName = fileInfo.fileName
        with open(fileName, "rb") as f:
            for chunk in iter(lambda: f.read(1024*1024), b""):
                yield file_transfer_pb2.FileMetadata(fileName=fileName, data=chunk)
    

    def UploadFile(self, fileMetadata_iterator, context):
        # fileName = fileMetadata_iterator.next().fileName
        # print(fileName)
        with open("fileName.iso", "wb") as f:
            for resp in fileMetadata_iterator:
                f.write(resp.data)
        return file_transfer_pb2.Empty()

    def UploadFile1(self, fileMetadata, context):
        with open("fileName.iso", "wb") as f:
            f.write(fileMetadata.data)
        return file_transfer_pb2.Empty()



def serve():
    file_transfer = DataTransferService()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    file_transfer_pb2_grpc.add_DataTransferServiceServicer_to_server(file_transfer, server)
    server.add_insecure_port("localhost:4000")
    server.start()
    while True:
        time.sleep(86400)
    

serve()
