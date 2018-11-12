import raft_pb2_grpc
from raft_pb2 import Heartbeat, VoteReq, Vote, AckHB, Empty, Log, DcList
from random import uniform 
from time import sleep
import grpc
from concurrent import futures
from threading import Thread, Event
import sys
from enum import Enum
import file_transfer_pb2
import file_transfer_pb2_grpc
from google.protobuf.json_format import MessageToDict

class States(Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

my_id = "localhost:4000"
my_vote = False7
my_term = 1
my_state = States.Follower
delay = uniform(1.0, 1.5)
stubs = []
friends = ["localhost:4001", "localhost:4002"]
hb_recv = False
vr_recv = False
# friends = ["localhost:4001", "localhost:4002", "localhost:4003", "localhost:4004"]
dcs = ["localhost:5000", "localhost:5001"]
dc_files = {}
file_max_chunks = {}
file_log = {}

class RaftImpl(raft_pb2_grpc.raftImplemetationServicer, file_transfer_pb2_grpc.DataTransferServiceServicer):
    def RequestVote(self, voteReq, context):
        global vr_recv, my_state, my_term, hb_recv
        # vr_recv = True
        # if my_term > voteReq.currentTerm or my_state == States.Leader:
        #     return Vote(voted=False, currentTerm=my_term)
        # else:
        #     if my_vote and my_term == voteReq.currentTerm:
        #         return Vote(voted=False, currentTerm=my_term)
        #     else:
        #         my_term = voteReq.currentTerm
        #         return Vote(voted=True, currentTerm=my_term)
        if hb_recv == True:
            return Vote(voted=False, currentTerm=my_term)
        else:
            if my_term >= voteReq.currentTerm or my_state == States.Leader:
                return Vote(voted=False, currentTerm=my_term)
            else:
                vr_recv = True
                my_term = voteReq.currentTerm
                return Vote(voted=True, currentTerm=my_term)

    def AppendEntries(self):
        pass
    
    def SendHeartBeat(self, hearBeat, context):
        global hb_recv, my_state, my_term, vr_recv, file_log, file_max_chunks
        hb_recv = True
        vr_recv = False
        my_state = States.Follower
        my_term = hearBeat.currentTerm
        file_log = dict(hearBeat.log.fileLog)
        for file_key in file_log.keys(): 
            file_log[file_key] = list(file_log[file_key].dcs)
        file_max_chunks = dict(hearBeat.log.maxChunks)
        return AckHB(ack="StillAlive")
    
    def RequestFileInfo(self, FileInfo, context):
        pass
    
    def GetFileLocation(self, Fileinfo, context):
        pass
    
    def DownloadChunk(self, ChunkInfo, context):
        pass

    def UploadFile(self, FileUploadData_stream, context):
        dc = findDataCenter()
        dc_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(dc))
        return dc_stub.UploadFile(FileUploadData_stream)

    def ListFiles(self, RequestFileList, context):
        pass

def findDataCenter():
    return "localhost:5000"
    #######TODO######## find datacenter by available space and upload to multiple data centers

def serve():
    raft = RaftImpl()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_raftImplemetationServicer_to_server(raft, server)
    file_transfer_pb2_grpc.add_DataTransferServiceServicer_to_server(raft, server)
    server.add_insecure_port(my_id)
    server.start()
    try: 
        while True:
            sleep(86400)
    except KeyboardInterrupt:
        sys.exit(1)

def client():
    global my_id, my_vote, my_term, my_state, stubs, friends, delay, hb_recv, vr_recv
    while True:
        print(my_state.name, my_term)
        if my_state == States.Leader:
            leaderActions()
        else:
            timer()
            if not hb_recv and not vr_recv:
                if my_state == States.Follower:
                    my_state = States.Candidate
                    my_term += 1
                    my_vote = True
                    startElection()
                else:
                    my_term += 1
                    my_vote = True
                    startElection()


def timer():
    global delay, hb_recv, vr_recv, my_term, my_state
    if my_term == 1:
        sleep(uniform(2.0, 3.0))
    else:
        while True:
            print("Waiting for heartbeat", my_state.name, my_term)
            print(file_log, file_max_chunks)
            if hb_recv:
                hb_recv = False
                sleep(uniform(2.0, 3.0))
            elif vr_recv:
                vr_recv = False
                sleep(uniform(2.0, 3.0))
            else:
                sleep(uniform(2.0, 3.0))
                break

def startElection():
    global my_id, my_term, my_state, stubs
    vote_count = 1
    vote = ""
    stubs = []
    for friend in friends:
        stubs.append(raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(friend)))
    for stub in stubs:
        try:
            vote = stub.RequestVote(VoteReq(id=my_id, currentTerm=my_term), timeout=0.1)
            if vote.voted:
                vote_count += 1
            else:
                my_term = vote.currentTerm
        except:
            print("node dead")
    print("votes received: ", vote_count)
    if vote_count > (len(stubs)+1)//2:
        my_state = States.Leader
        return
    else:
        return
    

def leaderActions():
    global my_state, stubs, my_id, my_term, file_log, file_max_chunks
    hb_ack = ""
    while my_state == States.Leader:
        print("Sending Heartbeats", my_state.name, my_term)
        print(file_log, file_max_chunks)
        stub = ""
        for friend in friends:
            stub = raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(friend))
            try:
                for file_key in file_log.keys(): 
                    file_log[file_key] = DcList(dcs=file_log[file_key])

                hb_ack = stub.SendHeartBeat(Heartbeat(id=my_id, currentTerm=my_term, log = Log(fileLog = file_log, maxChunks = file_max_chunks)), timeout=0.1)
            except:
                #print("node dead")
                pass
        sleep(0.5)

def checkDcHealth():
    global my_state, dcs, my_id, dc_files, file_max_chunks, file_log
    while True:
        if my_state == States.Leader:
            # stub = ""
            for dc in dcs:
                stub = raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(dc))
                try:
                    hb_ack = stub.SendHeartBeat(Heartbeat(id=my_id))
                    dc_ack = hb_ack.dcAck
                    max_chunks = hb_ack.maxChunks
                    for f in max_chunks.keys():
                        if f not in file_max_chunks.keys():
                            file_max_chunks[f] = max_chunks[f]
                    dc_files[dc] = list(set([f.rsplit('_', 1)[0] for f in dc_ack]))
                    for f_c in list(set(list(dc_ack) + list(file_log.keys()))):
                        if f_c not in file_log.keys():
                            file_log[f_c] = [dc]
                        else:
                            if f_c in dc_ack:
                                if dc not in file_log[f_c]:
                                    file_log[f_c].append(dc)
                            else:
                                if dc in file_log[f_c]:
                                    file_log[f_c].remove(dc)

                    print(dc_files, "\n", file_max_chunks, "\n", file_log)
                except:
                    print("node dead")
        sleep(5)

if __name__ == '__main__':
    t1 = Thread(target=serve)
    t2 = Thread(target=client)
    t3 = Thread(target=checkDcHealth)
    
    t1.start()
    t2.start()
    t3.start()
