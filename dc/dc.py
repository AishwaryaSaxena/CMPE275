import sys
import os
from pathlib import Path
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent)+'/proto')
from os.path import isfile, join
import file_transfer_pb2
import file_transfer_pb2_grpc
import raft_pb2_grpc
from raft_pb2 import Heartbeat, AckHB, Empty
from time import sleep
import grpc
from concurrent import futures
from threading import Thread, Event
import json 

with open('../conf/config.json', 'r') as conf:
    config = json.load(conf)

my_id = config['dc_my_id']
dc_resp = []
file_max_chunks = {}
size_avail = 0
dc_path = '/'

# my_id = "localhost:5000"

class DataCenter(file_transfer_pb2_grpc.DataTransferServiceServicer, raft_pb2_grpc.raftImplemetationServicer):
    def RequestFileInfo(self, fileInfo, context):
        pass
    
    def GetFileLocation(self, fileinfo, context):
        pass
    
    def DownloadChunk(self, chunkInfo, context):
        seq_list = []
        file_name = chunkInfo.fileName
        chunk_id = chunkInfo.chunkId
        start_seq_num = chunkInfo.startSeqNum
        with open(file_name+'_'+str(chunk_id), 'rb') as f:
            seq_num = 0
            for chunk in iter(lambda: f.read(1024*1024), b""):
                seq_list.append(chunk)
                seq_num += 1
        for i in range(start_seq_num, len(seq_list)):
            print("Sending " + file_name + ", ChunkId: " + chunk_id + ", SequenceNumber: " + i)
            yield file_transfer_pb2.FileMetaData(fileName=file_name, chunkId=chunk_id, data=seq_list[i], seqNum=i, seqMax=seq_num)

    def UploadFile(self, FileUploadData_stream, context):
        global file_max_chunks
        event.clear()
        fud = FileUploadData_stream.next()
        fileName = fud.fileName
        chunkId = str(fud.chunkId)
        maxChunks = fud.maxChunks
        print("receiving file "+ fileName + " chunk-" + chunkId)
        if fileName not in file_max_chunks.keys():
            file_max_chunks[fileName] = maxChunks
        with open(fileName + "_" + chunkId, 'wb') as f:
            f.write(fud.data)
            for seq in FileUploadData_stream:
                f.write(seq.data)
        event.set()
        return file_transfer_pb2.FileInfo(fileName=fileName)


    def ListFiles(self, RequestFileList, context):
        pass

    def RequestVote(self, voteReq, context):
        pass
    
    def AppendEntries(self):
        pass

    def SendHeartBeat(self, hearBeat, context):
        global size_avail
        print("Receiving heartbeats: ")
        return AckHB(dcAck = dc_resp, maxChunks = file_max_chunks, sizeAvail = size_avail)

    def ReplicateFile(self, replicateFileInfo, context):
        file_name, chunk_id = replicateFileInfo.fileChunk.rsplit("_", 1)
        try:
            replication_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(replicateFileInfo.dcAddr))
            resps = replication_stub.DownloadChunk(file_transfer_pb2.ChunkInfo(fileName=file_name, chunkId=int(chunk_id)))
            with open(replicateFileInfo.fileChunk, "wb") as f:
                for resp in resps:
                    f.write(resp.data)
        except:
            pass
        return Empty()
    
    def DeleteFile(self, replicateFileInfo, context):
        try:
            os.remove(replicateFileInfo.fileChunk)
        except:
            pass
        return Empty()

def checkFiles():
    global dc_resp, size_avail
    while True:
        event.wait()
        dc_resp = [f for f in os.listdir(".") if isfile(join(".",f)) and f != "dc.py"]
        size_avail = os.statvfs(dc_path).f_frsize * os.statvfs(dc_path).f_bavail
        print(dc_resp)
        sleep(3)

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


if __name__ == '__main__':
    event = Event()
    event.set()
    t1 = Thread(target=serve)
    t3 = Thread(target=checkFiles)
    
    t1.start()
    t3.start()