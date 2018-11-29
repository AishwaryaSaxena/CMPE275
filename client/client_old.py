import grpc, file_transfer_pb2, file_transfer_pb2_grpc, raft_pb2, raft_pb2_grpc, sys, os, threading, datetime, multiprocessing
from concurrent import futures
# from time import sleep
from os.path import isfile, join
import ntpath
import time
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandlers
import pyinotify
from random import choice

stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("10.0.40.1:10000"))

def download():
    start = time.time()
    global stub    
    ##Request File Info
    # sleep(1)
    file_loc_info = stub.RequestFileInfo(file_transfer_pb2.FileInfo(fileName = "BlackPanther.mp4"))
    proxies = []
    for p in file_loc_info.lstProxy:
        proxies.append(p.ip + ":" + p.port)
    print(file_loc_info.fileName, file_loc_info.maxChunks, proxies, file_loc_info.isFileFound)

    ###Get File List
    #file_list = stub.ListFiles(file_transfer_pb2.RequestFileList(isClient = True))
    #print(file_list.lstFileNames)
    stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("10.0.30.3:10000"))
    d = {}
    if file_loc_info.isFileFound:
        with open("downloads/"+file_loc_info.fileName, "wb") as f:
            for i in range(file_loc_info.maxChunks):
                resps = stub.DownloadChunk(file_transfer_pb2.ChunkInfo(fileName=file_loc_info.fileName, chunkId=i))
                for resp in resps:
                    f.write(resp.data)
            d[i] = resps
    print (time.time()-start)

        
        # for key in sorted(d.keys()):

    # try:
    #     resps = stub.DownloadChunk(file_transfer_pb2.ChunkInfo(fileName="vbox.tar.xz", chunkId=1))
    #     with open("downloads/vbox_client.tar.xz", "wb") as f:
    #         for resp in resps:
    #             f.write(resp.data)
    # except Exception as e:
    #     print(e)
            
class MyEventHandler(pyinotify.ProcessEvent):
    def process_IN_CLOSE_WRITE(self, event):
        print ("CLOSE_WRITE event:", event.pathname)
        with open(event.pathname, "rb") as f:
            seq_list = []
            fn = path_leaf(event.pathname)
            for seq in iter(lambda: f.read(1024*1024), b""):
                seq_list.append(file_transfer_pb2.FileUploadData(fileName=fn, data=seq))
                    
            list_1, list_2, list_3 = seq_list[:len(seq_list)//3], seq_list[len(seq_list)//3:(len(seq_list)//3)*2], seq_list[(len(seq_list)//3)*2:]
            list_1 = [file_transfer_pb2.FileUploadData(fileName=fn, data=fud.data, chunkId = 0, maxChunks = 3) for fud in list_1]
            list_2 = [file_transfer_pb2.FileUploadData(fileName=fn, data=fud.data, chunkId = 1, maxChunks = 3) for fud in list_2]
            list_3 = [file_transfer_pb2.FileUploadData(fileName=fn, data=fud.data, chunkId = 2, maxChunks = 3) for fud in list_3]
            iter_list = [gen_stream(list_1), gen_stream(list_2), gen_stream(list_3)]

            threading.Thread(target=callupload1, args=(iter_list[0],)).start()
            threading.Thread(target=callupload2, args=(iter_list[1],)).start()
            threading.Thread(target=callupload3, args=(iter_list[2],)).start()


def getFileList():
    ###Get File List
    file_list = stub.ListFiles(file_transfer_pb2.RequestFileList(isClient = True))
    print(file_list.lstFileNames)

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def gen_stream(list_of_chunks):
    for chunk in list_of_chunks:
        yield chunk

def run():
    wm = pyinotify.WatchManager()
    wm.add_watch('/home/tejak/Desktop/CMPE275/raft_implementation_grpc/uploads', pyinotify.ALL_EVENTS, rec=True)

    # event handler
    eh = MyEventHandler()

    # notifier
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()

def callupload1(it):
    stub.UploadFile(it)
    print("sending chunk0 complete")
    
def callupload2(it):
    stub.UploadFile(it)
    print("sending chunk1 complete")
    
def callupload3(it):
    stub.UploadFile(it)
    print("sending chunk2 complete")

if __name__ == '__main__':
    # run()    # use this for linux based platforms, uncomment pyinotify at the top
    # run1()    # use this for cross platform
    # getFileList()
    download() # get the list of file, and download each one if necessary
