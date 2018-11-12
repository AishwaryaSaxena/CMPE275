import sys, os
from os.path import isfile, join
sys.path.append("/home/sai/Downloads/CMPE275/raft_implementation_grpc")
import file_transfer_pb2
import file_transfer_pb2_grpc
import raft_pb2_grpc
from raft_pb2 import Heartbeat, AckHB, Empty
from time import sleep
import grpc
from concurrent import futures
from threading import Thread, Event

my_id = "localhost:5000"
dc_resp = []
file_max_chunks = {}

class DataCenter(file_transfer_pb2_grpc.DataTransferServiceServicer, raft_pb2_grpc.raftImplemetationServicer):
    def RequestFileInfo(self, FileInfo, context):
        pass
    
    def GetFileLocation(self, Fileinfo, context):
        pass
    
    def DownloadChunk(self, ChunkInfo, context):
        pass

    def UploadFile(self, FileUploadData_stream, context):
        global file_max_chunks
        fud = FileUploadData_stream.next()
        fileName = fud.fileName
        chunkId = str(fud.chunkId)
        maxChunks = fud.maxChunks
        if fileName not in file_max_chunks.keys():
            file_max_chunks[fileName] = maxChunks
        with open(fileName + "_" + chunkId, 'wb') as f:
            f.write(fud.data)
            for seq in FileUploadData_stream:
                f.write(seq.data)
        return file_transfer_pb2.FileInfo(fileName=fileName)


    def ListFiles(self, RequestFileList, context):
        pass

    def RequestVote(self, voteReq, context):
        pass
    
    def AppendEntries(self):
        pass

    def SendHeartBeat(self, hearBeat, context):
        return AckHB(dcAck = dc_resp, maxChunks = file_max_chunks)

    ###TODO replication

def checkFiles():
    global dc_resp
    while True:
        dc_resp = [f for f in os.listdir() if isfile(join(".",f)) and f != "dc.py"]
        sleep(5)

def serve():
    dc = DataCenter()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    file_transfer_pb2_grpc.add_DataTransferServiceServicer_to_server(dc, server)
    raft_pb2_grpc.add_raftImplemetationServicer_to_server(dc, server)
    server.add_insecure_port(my_id)
    server.start()
    # print("server started")
    try: 
        while True:
            sleep(86400)
    except KeyboardInterrupt:
        sys.exit(1)

def client():
    print("dc working")

if __name__ == '__main__':
    t1 = Thread(target=serve)
    t2 = Thread(target=client)
    t3 = Thread(target=checkFiles)
    
    t1.start()
    t2.start()
    t3.start()