import sys
import os
from pathlib import Path
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent)+'/proto')
import raft_pb2_grpc
from raft_pb2 import Heartbeat, VoteReq, Vote, AckHB, Empty, Log, DcList, ReplicateFileInfo
from random import uniform, choice 
from time import sleep
import grpc
from concurrent import futures
from threading import Thread, Event
from enum import Enum
from file_transfer_pb2 import FileLocationInfo, ProxyInfo, FileList, RequestFileList, FileInfo
import file_transfer_pb2_grpc
import json

with open('../conf/config.json', 'r') as conf:
    config = json.load(conf)

class States(Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

replication_factor = config['replication_factor']
leader_id = ""
my_vote = False
my_term = 1
my_state = States.Follower
delay = uniform(1.0, 1.5)
stubs = []

my_id = config['raft_my_id']
friends = config['friends']
dcs = config['dcs']

external_nodes = config['external_nodes']
live_external_nodes = config['live_external_nodes']

hb_recv = False
dc_files = {}
file_max_chunks = {}
file_log = {}
dc_sizes = {}
live_nodes = []

cached_list_files = []
list_files_timer = 10
cached_file_info = {}
file_info_timer = {}

class RaftImpl(raft_pb2_grpc.raftImplemetationServicer, file_transfer_pb2_grpc.DataTransferServiceServicer):
    def RequestVote(self, voteReq, context):
        global my_state, my_term, hb_recv, my_vote
        if hb_recv == True or my_state == States.Leader:
            return Vote(voted=False, currentTerm=my_term)
        elif my_state == States.Follower:
            if my_vote:
                if my_term > voteReq.currentTerm:
                    return Vote(voted=False, currentTerm=my_term)
                my_term = voteReq.currentTerm
                return Vote(voted=True, currentTerm=my_term)
            my_term = voteReq.currentTerm
            return Vote(voted=True, currentTerm=my_term)
        else:
            if my_vote:
                if my_term < voteReq.currentTerm:
                    if my_state == States.Follower:
                        my_term = voteReq.currentTerm
                    my_state = States.Follower
                    return Vote(voted=True, currentTerm=my_term)
                return Vote(voted=False, currentTerm=my_term)
            else:
                if my_term > voteReq.currentTerm:
                    return Vote(voted=False, currentTerm=my_term)
                else:
                    my_term = voteReq.currentTerm
                    return Vote(voted=False, currentTerm=my_term)

    def AppendEntries(self):
        pass
    
    def SendHeartBeat(self, hearBeat, context):
        global hb_recv, my_state, my_term, file_log, file_max_chunks, leader_id, my_vote, dc_sizes
        if my_term <= hearBeat.currentTerm:
            hb_recv = True
            # vr_recv = False
            my_vote = True
            my_state = States.Follower
            my_term = hearBeat.currentTerm
            leader_id = hearBeat.id
            file_log = dict(hearBeat.log.fileLog)
            for file_key in file_log.keys(): 
                file_log[file_key] = list(file_log[file_key].dcs)
            file_max_chunks = dict(hearBeat.log.maxChunks)
            # print("HeartBeat received")
            dc_sizes = dict(hearBeat.log.dcSizes)
        return AckHB(ack="StillAlive")
    
    def RequestFileInfo(self, fileInfo, context):
        global file_info_timer, cached_file_info
        fileName = fileInfo.fileName
        if my_state != States.Leader:
            try:
                leader_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(leader_id))
                return leader_stub.RequestFileInfo(fileInfo, timeout=1)
            except:
                return FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
        else:
            fileFound = False
            for f_c in file_log.keys():
                if fileName in f_c and len(file_log[f_c]) != 0:
                    fileFound = True
                    break
            if fileFound:
                proxies = findProxies()
                proxies_ser = []
                for p in proxies:
                    ip_port = p.split(':')
                    proxies_ser.append(ProxyInfo(ip = ip_port[0], port = ip_port[1]))
                try:
                    return FileLocationInfo(fileName = fileName, maxChunks = file_max_chunks[fileName], lstProxy = proxies_ser, isFileFound = True)
                except:
                    return FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
            else:
                if not file_info_cache_event.isSet():
                    external_stub = ""
                    for n in live_external_nodes:
                        try:
                            external_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(n))
                            file_loc_info = external_stub.GetFileLocation(fileInfo, timeout=0.1)
                            if file_loc_info.isFileFound:
                                cached_file_info[fileName] = file_loc_info
                                file_info_timer[fileName] = 10
                                file_info_cache_event.set()
                                return file_loc_info
                        except:
                            pass
                    cached_file_info[fileName] = FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
                    file_info_timer[fileName] = 10
                    file_info_cache_event.set()
                    return FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
                else:
                    if fileName not in cached_file_info.keys():
                        external_stub = ""
                        for n in live_external_nodes:
                            try:
                                external_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(n))
                                file_loc_info = external_stub.GetFileLocation(fileInfo, timeout=0.1)
                                if file_loc_info.isFileFound:
                                    cached_file_info[fileName] = file_loc_info
                                    file_info_timer[fileName] = 10
                                    file_info_cache_event.set()
                                    return file_loc_info
                            except:
                                pass
                        cached_file_info[fileName] = FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
                        file_info_timer[fileName] = 10
                        file_info_cache_event.set()
                        return FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
                    else:
                        file_info_timer[fileName] = 10
                        return cached_file_info[fileName]
    
    def GetFileLocation(self, FileInfo, context):
        fileName = FileInfo.fileName
        if my_state != States.Leader:
            return FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
        else:
            fileFound = False
            for f_c in file_log.keys():
                if fileName in f_c and len(file_log[f_c]) != 0:
                    fileFound = True
                    break
            if fileFound:
                proxies = findProxies()
                proxies_ser = []
                for p in proxies:
                    ip_port = p.split(':')
                    proxies_ser.append(ProxyInfo(ip = ip_port[0], port = ip_port[1]))
                return FileLocationInfo(fileName = fileName, maxChunks = file_max_chunks[fileName], lstProxy = proxies_ser, isFileFound = True)
            else:
                return FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
    
    def DownloadChunk(self, chunkInfo, context):
        dc_list = file_log[chunkInfo.fileName + "_" + str(chunkInfo.chunkId)]
        dc_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(dc_list[0]))
        return dc_stub.DownloadChunk(chunkInfo)

    def UploadFile(self, FileUploadData_stream, context):
        dc = findDataCenter()
        dc_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(dc))
        return dc_stub.UploadFile(FileUploadData_stream)

    def ListFiles(self, requestFileList, context):
        global cached_list_files, list_files_timer
        isClient = requestFileList.isClient
        if my_state != States.Leader:
            if isClient:
                try:
                    leader_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(leader_id))
                    return leader_stub.ListFiles(requestFileList)
                except:
                    return FileList(lstFileNames = [])
            else:
                return FileList(lstFileNames = [])
        else:
            file_list = set()
            for f_c in file_log.keys():
                if len(file_log[f_c]) != 0:
                    file_list.add(f_c.rsplit('_', 1)[0])
            if isClient:
                if not file_list_cache_event.isSet():
                    external_stub = ""
                    for n in live_external_nodes:
                        try:
                            external_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(n))
                            ext_file_list = external_stub.ListFiles(RequestFileList(isClient = False), timeout=0.1)
                            print(ext_file_list.lstFileNames)
                            file_list.update(ext_file_list.lstFileNames)
                        except:
                            pass
                    cached_list_files = list(file_list)
                    list_files_timer = 10
                    file_list_cache_event.set()
                    return FileList(lstFileNames = list(file_list))
                else:
                    list_files_timer = 10
                    return FileList(lstFileNames = cached_list_files)
            else:
                return FileList(lstFileNames = list(file_list))

    def ReplicateFile(self, replicateFileInfo, context):
        pass
    
    def DeleteFile(self, replicateFileInfo, context):
        pass

def fileListCacheHandler():
    global list_files_timer, cached_list_files
    while True:
        file_list_cache_event.wait()
        while(list_files_timer > 0):
            file_list = set()
            for f_c in file_log.keys():
                if len(file_log[f_c]) != 0:
                    file_list.add(f_c.rsplit('_', 1)[0])
            for n in live_external_nodes:
                try:
                    external_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(n))
                    ext_file_list = external_stub.ListFiles(RequestFileList(isClient = False), timeout=0.1)
                    file_list.update(ext_file_list.lstFileNames)
                except:
                    pass
            cached_list_files = list(file_list)
            list_files_timer -= 1
            sleep(1)
        file_list_cache_event.clear()

def fileInfoCacheHandler():
    global file_info_timer, cached_file_info
    while True:
        file_info_cache_event.wait()
        while(len(file_info_timer.keys()) > 0):
            for fileName in list(file_info_timer.keys()):
                if file_info_timer[fileName] == 0:
                    del file_info_timer[fileName]
                    del cached_file_info[fileName]
                    continue
                file_loc_info = FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
                for n in live_external_nodes:
                    try:
                        external_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(n))
                        file_loc_info = external_stub.GetFileLocation(FileInfo(fileName=fileName), timeout=0.1)
                        if file_loc_info.isFileFound:
                            break
                    except:
                        pass
                if file_loc_info.isFileFound:
                    cached_file_info[fileName] = file_loc_info
                    file_info_timer[fileName] -= 1
                else:
                    cached_file_info[fileName] = FileLocationInfo(fileName = fileName, maxChunks = 0, lstProxy = [], isFileFound = False)
                    file_info_timer[fileName] -= 1
            sleep(1)
        file_info_cache_event.clear()
                

def findDataCenter():
    global dc_sizes
    print(dc_sizes)
    return choice(list(dc_sizes.keys()))

def findProxies():
    return live_nodes

def serve():
    global live_external_nodes
    raft = RaftImpl()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_raftImplemetationServicer_to_server(raft, server)
    file_transfer_pb2_grpc.add_DataTransferServiceServicer_to_server(raft, server)
    server.add_insecure_port(my_id)
    server.start()
    try: 
        # Utilizing thread to check live external nodes
        while True:
            if my_state == States.Leader:
                for node in external_nodes:
                    try:
                        external_stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(node))
                        ext_file_list = external_stub.ListFiles(RequestFileList(isClient = False), timeout=0.1)
                        if node not in live_external_nodes:
                            live_external_nodes.append(node)
                    except:
                        if node in live_external_nodes:
                            live_external_nodes.remove(node)
            sleep(3)
    except KeyboardInterrupt:
        sys.exit(1)

def client():
    global my_id, my_vote, my_term, my_state, stubs, friends, delay, hb_recv
    while True:
        print(my_state.name, my_term)
        if my_state == States.Leader:
            leaderActions()
        else:
            timer()
            if not hb_recv:
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
    global delay, hb_recv, my_term, my_state, my_vote
    if my_term == 1:
        #print("in timer -> if")
        sleep(uniform(2.0, 2.5))
    else:
        while True:
            print("Waiting for heartbeat", my_state.name, my_term)
            print(file_log, file_max_chunks)
            if hb_recv:
                hb_recv = False
                my_vote = False
                sleep(uniform(1.0, 1.5))
            else:
                sleep(uniform(1.0, 1.5))
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
            pass
    if vote_count > (len(stubs)+1)//2:
        my_state = States.Leader
        return
    else:
        return
    
def leaderActions():
    global my_state, stubs, my_id, my_term, file_log, file_max_chunks, dc_sizes, live_nodes
    hb_ack = ""
    while my_state == States.Leader:
        print("Sending Heartbeats", my_state.name, my_term)
        print("live_nodes: ", live_nodes)
        stubs = []
        for friend in friends:
            stubs.append(raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(friend)))
        for i in range(len(stubs)):
            try:
                d = {}
                for file_key in file_log.keys(): 
                    d[file_key] = DcList(dcs=file_log[file_key])
                hb_ack = stubs[i].SendHeartBeat(Heartbeat(id=my_id, currentTerm=my_term, log = Log(fileLog = d, maxChunks = file_max_chunks, dcSizes = dc_sizes)), timeout=0.1)
                if friends[i] not in live_nodes:
                    live_nodes.append(friends[i])
            except:
                if friends[i] in live_nodes:
                    live_nodes.remove(friends[i])
        sleep(1)

def checkDcHealth():
    global my_state, dcs, my_id, dc_files, file_max_chunks, file_log, dc_sizes
    while True:
        if my_state == States.Leader:
            for dc in dcs:
                stub = raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(dc))
                try:
                    hb_ack = stub.SendHeartBeat(Heartbeat(id=my_id), timeout=0.2)
                    dc_sizes[dc] = hb_ack.sizeAvail
                    dc_ack = list(hb_ack.dcAck)
                    max_chunks = dict(hb_ack.maxChunks)
                    for f in max_chunks.keys():
                        if f not in file_max_chunks.keys():
                            file_max_chunks[f] = max_chunks[f]
                    dc_files[dc] = list(set([f.rsplit('_', 1)[0] for f in dc_ack]))
                    for f_c in list(set(dc_ack + list(file_log.keys()))):
                        if f_c not in file_log.keys():
                            file_log[f_c] = [dc]
                        else:
                            if f_c in dc_ack:
                                if dc not in file_log[f_c]:
                                    file_log[f_c].append(dc)
                            else:
                                if dc in file_log[f_c]:
                                    file_log[f_c].remove(dc)
                    #print(dc_files, "\n", file_max_chunks, "\n", file_log)
                except:
                    for f_c in list(file_log.keys()):
                        if dc in file_log[f_c]:
                            file_log[f_c].remove(dc)
                    if dc in dc_sizes.keys():
                        del dc_sizes[dc]
                    # print("dc dead")
        sleep(3)


def replicationHandler():
    while True:
        if my_state == States.Leader:
            for f_c in list(file_log.keys()):
                if len(file_log[f_c]) < replication_factor and len(file_log[f_c]) != 0:
                    dest_dcs = list( set(dc_sizes.keys()) - set(file_log[f_c]) )
                    if len(dest_dcs) <= (replication_factor - len(file_log[f_c])):
                        for dest_dc in dest_dcs:
                            try:
                                replication_stub = raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(dest_dc))
                                replication_stub.ReplicateFile(ReplicateFileInfo(fileChunk = f_c, dcAddr = choice(file_log[f_c])), timeout=0.2)
                            except:
                                pass
                    else:
                        for i in range(replication_factor - len(file_log[f_c])):
                            try:
                                replication_stub = raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(dest_dcs[i]))
                                replication_stub.ReplicateFile(ReplicateFileInfo(fileChunk = f_c, dcAddr = choice(file_log[f_c])), timeout=0.2)
                            except:
                                pass
                elif len(file_log[f_c]) == replication_factor:
                    pass
                else:
                    try:
                        delete_stub =  raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel(choice(file_log[f_c])))
                        delete_stub.DeleteFile(ReplicateFileInfo(fileChunk = f_c))
                    except:
                        pass
        sleep(3)


if __name__ == '__main__':
    file_list_cache_event = Event()
    file_info_cache_event = Event()
    t1 = Thread(target=serve)
    t2 = Thread(target=client)
    t3 = Thread(target=checkDcHealth)
    t4 = Thread(target=replicationHandler)
    t5 = Thread(target=fileListCacheHandler)
    t6 = Thread(target=fileInfoCacheHandler)
    
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    