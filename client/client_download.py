import sys
import os
from pathlib import Path
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent)+'/proto')
import grpc
import file_transfer_pb2
import file_transfer_pb2_grpc
from os.path import isfile, join
from time import sleep, time
import shutil
from random import choice
from threadpool.threadpool import ThreadPool

pool = ThreadPool(10)
# raft_nodes = ["10.0.40.2:10001", "10.0.40.2:10000", "10.0.40.3:10000", "10.0.40.4:10000","10.0.40.1:10000"]
raft_nodes = ["10.0.40.3:10000", "10.0.40.4:10000", "10.0.40.1:10000"]
cache_dir = "/home/tejak/Desktop/CMPE275/client/downloads/.cache/"

def download():
    global pool
    ch = input("Do you want to download something?(y/n) ")
    if ch == 'y':
        file_name = input("enter file name to download: ")
        download_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(choice(raft_nodes)))

        # get the file location
        try:
            file_loc_info = download_stub.RequestFileInfo(file_transfer_pb2.FileInfo(fileName = file_name))
        except:
            for ip in raft_nodes:
                try:
                    download_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(ip))
                    file_loc_info = download_stub.RequestFileInfo(file_transfer_pb2.FileInfo(fileName = file_name))
                    break
                except:
                    pass
        proxies = []
        for p in file_loc_info.lstProxy:
            proxies.append(p.ip + ":" + p.port)
        print(file_loc_info.fileName, file_loc_info.maxChunks, proxies, file_loc_info.isFileFound)

        # Download the file
        start = time()
        if file_loc_info.isFileFound:
            print("Starting Download")
            for chunk_id in range(file_loc_info.maxChunks):
                pool.add_task(downloader, file_name, chunk_id, proxies)
            pool.wait_completion()

            print("Stitching the chunks together")
            list_of_chunks = [f for f in os.listdir(cache_dir + file_name + '/') if isfile(join(cache_dir + file_name + '/',f))]
            # list_of_chunks.sort()
            for i in range(len(list_of_chunks)):
                file_chunk = list_of_chunks[i].rsplit('_',1)
                list_of_chunks[i] = (file_chunk[0], int(file_chunk[1]))
            list_of_chunks = sorted(list_of_chunks, key=lambda x: x[1])
            # print(list_of_chunks)
            list_of_chunks = [s[0]+'_'+str(s[1]) for s in list_of_chunks]
            with open("downloads/" + file_name, "wb") as f:
                for chunk in list_of_chunks:
                    with open(cache_dir + file_name + '/' + chunk, "rb") as ch:
                        f.write(ch.read())
            print("Done...")
            print(time()-start)
            print("Cleaning up...")
            shutil.rmtree(cache_dir + file_name + '/')


def downloader(file_name, chunk_id, proxies):
    download_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(choice(proxies))) # replace this ip addr with choice(ips)
    file_cache_dir = cache_dir + file_name + '/'
    try:
        resps = download_stub.DownloadChunk(file_transfer_pb2.ChunkInfo(fileName=file_name, chunkId=chunk_id))
    except:
        for ip in proxies:
            try:
                download_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(ip))
                resps = download_stub.DownloadChunk(file_transfer_pb2.ChunkInfo(fileName=file_name, chunkId=chunk_id))
                break
            except:
                pass
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)
    if not os.path.exists(file_cache_dir):
        os.makedirs(file_cache_dir, exist_ok=True)
    with open(file_cache_dir + file_name + '_' + str(chunk_id), "wb") as f:
        for resp in resps:
            f.write(resp.data)


def get_file_list():
    print("Getting the list of files...")
    start = time()
    get_file_list_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(choice(raft_nodes)))
    try:
        file_list = get_file_list_stub.ListFiles(file_transfer_pb2.RequestFileList(isClient = True))
    except:
        for ip in raft_nodes:
            try:
                get_file_list_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(ip))
                file_list = get_file_list_stub.ListFiles(file_transfer_pb2.RequestFileList(isClient = True))
                break
            except:
                pass
    print(time()-start)
    print(file_list.lstFileNames)



if __name__ == "__main__":
    get_file_list()
    download()