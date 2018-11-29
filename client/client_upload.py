import sys
import os
from pathlib import Path
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent)+'/proto')
import grpc
import file_transfer_pb2
import file_transfer_pb2_grpc
import raft_pb2
import raft_pb2_grpc
import ntpath
from time import sleep, time
import pyinotify
from threading import Thread
from threadpool.threadpool import ThreadPool
import shutil
from math import ceil
from random import choice

pool = ThreadPool(10)
tmp_dir = "/home/tejak/Desktop/CMPE275/client/.cache/"
WATCH_DIR = '/home/tejak/Desktop/CMPE275/client/uploads'
max_chunks = {}
chunk_size = 1024*1024*200
# raft_nodes = ["10.0.40.2:10001", "10.0.40.2:10000", "10.0.40.3:10000", "10.0.40.4:10000","10.0.40.1:10000"]
raft_nodes = ["10.0.40.3:10000", "10.0.40.4:10000", "10.0.40.1:10000"]

class MyEventHandler(pyinotify.ProcessEvent):
    def process_IN_CLOSE_WRITE(self, event):
        global max_chunks, tmp_dir, chunk_size 
        print(event.pathname)
        max_chunks[path_leaf(event.pathname)] = ceil(os.path.getsize(event.pathname) / chunk_size)
        # print(max_chunks)
        part = split(event.pathname, tmp_dir, chunk_size)
            
class MyNewEventHandler(pyinotify.ProcessEvent):
    def process_IN_CLOSE_WRITE(self, event):
        global pool, max_chunks
        start = time()
        print(event.pathname)
        fn, chunk_id = path_leaf(event.pathname).rsplit("_", 1)
        with open(event.pathname, "rb") as f:
            seq_list = []
            for seq in iter(lambda: f.read(1024*1024), b""):
                seq_list.append(file_transfer_pb2.FileUploadData(fileName = "team4-"+fn, data = seq, chunkId = int(chunk_id), maxChunks = max_chunks[fn]))
            pool.add_task(callUpload, gen_stream(seq_list))
        pool.wait_completion()
        print(time()-start)

def split(fromfile, todir, ch_size): 
    if not os.path.exists(todir):                  
        os.mkdir(todir)                 
    fn = path_leaf(fromfile)
    partnum = -1
    input = open(fromfile, 'rb')                   
    while True:                                    
        chunk = input.read(ch_size)   
        if not chunk: 
            break
        partnum  = partnum+1
        filename = os.path.join(todir, (fn+'_'+'%d' % (partnum)))
        fileobj  = open(filename, 'wb')
        fileobj.write(chunk)
        fileobj.close()                            
    input.close(  )
    assert partnum <= 9999        
    return partnum


def gen_stream(list_of_chunks):
    for chunk in list_of_chunks:
        yield chunk

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def callUpload(it):
    stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("10.0.40.1:10000"))
    stub.UploadFile(it)

def run():
    wm = pyinotify.WatchManager()
    wm.add_watch(WATCH_DIR, pyinotify.IN_CLOSE_WRITE, rec=True)

    # event handler
    eh = MyEventHandler()

    # notifier
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()

def run1():
    wm = pyinotify.WatchManager()
    wm.add_watch(tmp_dir, pyinotify.IN_CLOSE_WRITE, rec=True)

    # event handler
    eh = MyNewEventHandler()

    # notifier
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()



if __name__ == '__main__':
    Thread(target=run).start()
    Thread(target=run1).start()





