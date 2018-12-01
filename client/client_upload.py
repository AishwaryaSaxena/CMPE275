import sys
import os
from pathlib import Path
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent)+'/proto')
import grpc
import file_transfer_pb2
import file_transfer_pb2_grpc
import ntpath
from time import sleep, time
import pyinotify
from threading import Thread
from threadpool import ThreadPool
import shutil
from math import ceil
from random import choice
import json

pool = ThreadPool(10)
TMP_DIR = str(os.path.dirname(os.path.abspath(__file__))) + '/.cache/'
WATCH_DIR = str(os.path.dirname(os.path.abspath(__file__))) + '/uploads'

if not os.path.exists(TMP_DIR):
    os.makedirs(TMP_DIR, exist_ok=True)
if not os.path.exists(WATCH_DIR):
    os.makedirs(WATCH_DIR, exist_ok=True)

with open('../conf/config.json', 'r') as conf:
    config = json.load(conf)

max_chunks = {}
chunk_size = 1024*1024*200
raft_nodes = config['raft_nodes']

class MyEventHandler(pyinotify.ProcessEvent):
    def process_IN_CLOSE_WRITE(self, event):
        global max_chunks, TMP_DIR, chunk_size 
        max_chunks[path_leaf(event.pathname)] = ceil(os.path.getsize(event.pathname) / chunk_size)
        print(max_chunks)
        part = split(event.pathname, TMP_DIR, chunk_size)
            
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
    stub = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel(choice(raft_nodes)))
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
    wm.add_watch(TMP_DIR, pyinotify.IN_CLOSE_WRITE, rec=True)

    # event handler
    eh = MyNewEventHandler()

    # notifier
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()



if __name__ == '__main__':
    Thread(target=run).start()
    Thread(target=run1).start()