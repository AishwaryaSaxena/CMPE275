import sys, os
from os.path import isfile, join
sys.path.append("/home/tejak/Documents/275/CMPE275/raft_implementation_grpc")
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
size_avail = 0

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
            yield file_transfer_pb2.FileMetaData(fileName=file_name, chunkId=chunk_id, data=seq_list[i], seqNum=i, seqMax=seq_num)

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
        global size_avail
        return AckHB(dcAck = dc_resp, maxChunks = file_max_chunks, sizeAvail = size_avail)

    ###TODO replication

def checkFiles():
    global dc_resp, size_avail
    while True:
        dc_resp = [f for f in os.listdir() if isfile(join(".",f)) and f != "dc.py"]
        size_avail = os.statvfs('/home/tejak/Documents').f_frsize * os.statvfs('/home/tejak/Documents').f_bavail
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