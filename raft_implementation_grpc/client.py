import grpc, file_transfer_pb2, file_transfer_pb2_grpc, raft_pb2, raft_pb2_grpc, sys, os, threading, datetime, multiprocessing
from concurrent import futures
from time import sleep

def run():
    stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("localhost:4000"))
    seq_list = []
    with open("files/sample.txt", "rb") as f:
        for seq in iter(lambda: f.read(1024*10), b""):
            seq_list.append(file_transfer_pb2.FileUploadData(fileName="sample.txt", data=seq))

    list_1, list_2, list_3 = seq_list[:len(seq_list)//3], seq_list[len(seq_list)//3:(len(seq_list)//3)*2], seq_list[(len(seq_list)//3)*2:]
    list_1 = [file_transfer_pb2.FileUploadData(fileName="sample.txt", data=fud.data, chunkId = 1, maxChunks = 3) for fud in list_1]
    list_2 = [file_transfer_pb2.FileUploadData(fileName="sample.txt", data=fud.data, chunkId = 2, maxChunks = 3) for fud in list_2]
    list_3 = [file_transfer_pb2.FileUploadData(fileName="sample.txt", data=fud.data, chunkId = 3, maxChunks = 3) for fud in list_3]
    iter_list = [gen_stream(list_1), gen_stream(list_2), gen_stream(list_3)]
    print(datetime.datetime.now())
    # callUpload(stub, iter_list[0])
    # callUpload(stub, iter_list[1])
    # callUpload(stub, iter_list[2])
    # jobs = []
    # for iterator in iter_list:
    #     jobs.append(threading.Thread(target=callUpload, kwargs={'stub':stub, 'iter':iterator}))
    # for iterator in iter_list:
    #     multiprocessing.Process(target=callUpload, args = (stub, iterator)).start()
    # processes =[]
    # for i in range(3):
    #     t = multiprocessing.Process(target=func1, args = (iter_list[i],))
    #     processes.append(t)
    #     # t.daemon = True
    #     t.start()
    # for process in processes:
    #     process.join()

    #multiprocessing.Process(target=callUpload2, args = (stub, iter_list[1])).start()
    #multiprocessing.Process(target=callUpload3, args = (stub, iter_list[2])).start()
    # for job in jobs:
    #     job.start()

    # for job in jobs:
    #     job.join() 
    sleep(1)
    file_loc_info = stub.RequestFileInfo(file_transfer_pb2.FileInfo(fileName = "sample.txt"))
    proxies = []
    for p in file_loc_info.lstProxy:
        proxies.append(p.ip + ":" + p.port)
    print(file_loc_info.fileName, file_loc_info.maxChunks, proxies, file_loc_info.isFileFound)



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