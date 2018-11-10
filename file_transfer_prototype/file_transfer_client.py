import file_transfer_pb2
import file_transfer_pb2_grpc
import grpc
from concurrent import futures
import sys
# server_ip = "localhost:3000"



def client():
    stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("localhost:3000"))

    # with open("antergos_server_to_client.iso", "wb") as f:
    #     resps = stub.DownloadFile(file_transfer_pb2.FileInfo(fileName="antergos.iso"))
    #     for resp in resps:
    #         f.write(resp.data)

    chunks = []
    with open("antergos.iso", "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            chunks.append(file_transfer_pb2.FileMetadata(fileName="antergos_client_to_server.iso", data=chunk))

    chunk_iter = gen_stream(chunks)
    # print(chunk_iter)
    # print(sys.getsizeof(chunk_iter))
    stub.UploadFile(chunk_iter)

def gen_stream(list_of_chunks):
    for chunk in list_of_chunks:
        yield chunk

client()


