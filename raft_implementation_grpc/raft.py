import raft_pb2_grpc
from raft_pb2 import Heartbeat, VoteReq, Vote, AckHB, Empty
from random import uniform 
from time import sleep
import grpc
from concurrent import futures
from threading import Thread, Event
import sys
from enum import Enum
import file_transfer_pb2
import file_transfer_pb2_grpc

class States(Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

my_id = "localhost:4000"
my_vote = False
my_term = 1
my_state = States.Follower
delay = uniform(1.0, 1.5)
stubs = []
# friends = ["localhost:4001", "localhost:4002"]
hb_recv = False
vr_recv = False
friends = ["localhost:4001", "localhost:4002", "localhost:4003", "localhost:4004"]

class RaftImpl(raft_pb2_grpc.raftImplemetationServicer, file_transfer_pb2_grpc.DataTransferServiceServicer):
    def RequestVote(self, voteReq, context):
        global vr_recv, my_state, my_term
        vr_recv = True
        if my_term > voteReq.currentTerm or my_state == States.Leader:
            return Vote(voted=False, currentTerm=my_term)
        else:
            if my_vote and my_term == voteReq.currentTerm:
                return Vote(voted=False, currentTerm=my_term)
            else:
                my_term = voteReq.currentTerm
                return Vote(voted=True, currentTerm=my_term)

    def AppendEntries(self):
        pass
    
    def SendHeartBeat(self, hearBeat, context):
        global hb_recv, my_state, my_term, vr_recv
        hb_recv = True
        vr_recv = False
        my_state = States.Follower
        my_term = hearBeat.currentTerm
        return AckHB(ack="StillAlive")
    
    def RequestFileInfo(self, FileInfo, context):
        pass
    
    def GetFileLocation(self, Fileinfo, context):
        pass
    
    def DownloadChunk(self, ChunkInfo, context):
        pass

    def UploadFile(self, FileUploadData_stream, context):
        pass

    def ListFiles(self, RequestFileList, context):
        pass
    
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
        sleep(delay)
    else:
        while True:
            print("Waiting for heartbeat", my_state.name, my_term)
            if hb_recv:
                hb_recv = False
                sleep(delay)
            elif vr_recv:
                vr_recv = False
                sleep(delay)
            else:
                sleep(delay)
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
    global my_state, stubs, my_id, my_term, stubs
    hb_ack = ""
    while my_state == States.Leader:
        print("Sending Heartbeats", my_state.name, my_term)
        stubs = []
        for friend in friends:
            stubs.append(raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(friend)))
        for stub in stubs:
            try:
                hb_ack = stub.SendHeartBeat(Heartbeat(id=my_id, currentTerm=my_term), timeout=0.1)
            except:
                print("node dead")
        sleep(0.5)

if __name__ == '__main__':
    t1 = Thread(target=serve)
    t2 = Thread(target=client)
    
    t1.start()
    t2.start()
