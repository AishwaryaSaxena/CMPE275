import grpc, file_transfer_pb2, file_transfer_pb2_grpc, raft_pb2, raft_pb2_grpc, sys, os, threading, datetime, multiprocessing
from concurrent import futures
from time import sleep

def run():
    stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("localhost:4000"))
    seq_list = []
    # with open("files/vbox_client.tar.xz", "rb") as f:
    #     for seq in iter(lambda: f.read(1024*10), b""):
    #         seq_list.append(file_transfer_pb2.FileUploadData(fileName="vbox.tar.xz", data=seq))

    # list_1, list_2, list_3 = seq_list[:len(seq_list)//3], seq_list[len(seq_list)//3:(len(seq_list)//3)*2], seq_list[(len(seq_list)//3)*2:]
    # list_1 = [file_transfer_pb2.FileUploadData(fileName="vbox.tar.xz", data=fud.data, chunkId = 1, maxChunks = 3) for fud in list_1]
    # list_2 = [file_transfer_pb2.FileUploadData(fileName="vbox.tar.xz", data=fud.data, chunkId = 2, maxChunks = 3) for fud in list_2]
    # list_3 = [file_transfer_pb2.FileUploadData(fileName="vbox.tar.xz", data=fud.data, chunkId = 3, maxChunks = 3) for fud in list_3]
    # iter_list = [gen_stream(list_1), gen_stream(list_2), gen_stream(list_3)]
    # # print(datetime.datetime.now())
    # callUpload(stub, iter_list[0])
    # callUpload(stub, iter_list[1])
    # callUpload(stub, iter_list[2])

    ##Request File Info
    sleep(1)
    file_loc_info = stub.RequestFileInfo(file_transfer_pb2.FileInfo(fileName = "vbox.tar.xz"))
    proxies = []
    for p in file_loc_info.lstProxy:
        proxies.append(p.ip + ":" + p.port)
    print(file_loc_info.fileName, file_loc_info.maxChunks, proxies, file_loc_info.isFileFound)

    ###Get File List
    # file_list = stub.ListFiles(file_transfer_pb2.RequestFileList(isClient = True))
    # print(file_list.lstFileNames)
    # d = {}
    if file_loc_info.isFileFound:
        with open("downloads/"+file_loc_info.fileName, "wb") as f:
            for i in range(1, file_loc_info.maxChunks+1):
                resps = stub.DownloadChunk(file_transfer_pb2.ChunkInfo(fileName=file_loc_info.fileName, chunkId=i))
                for resp in resps:
                    f.write(resp.data)
            # d[i] = resps

        
        # for key in sorted(d.keys()):



    # try:
    #     resps = stub.DownloadChunk(file_transfer_pb2.ChunkInfo(fileName="vbox.tar.xz", chunkId=1))
    #     with open("downloads/vbox_client.tar.xz", "wb") as f:
    #         for resp in resps:
    #             f.write(resp.data)
    # except Exception as e:
    #     print(e)




def func1(iterator):
    stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("localhost:4000"))
    stub.UploadFile(iterator)
    print(datetime.datetime.now())

def callUpload(stub, iter):
    stub.UploadFile(iter)
    print(datetime.datetime.now())

def callUpload2(stub, iter):
    stub.UploadFile(iter)
    print(datetime.datetime.now())

def callUpload3(stub, iter):
    stub.UploadFile(iter)
    print(datetime.datetime.now())

def gen_stream(list_of_chunks):
    for chunk in list_of_chunks:
        yield chunk

run()