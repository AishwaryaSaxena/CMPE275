### How to run

Setup a python3 virtual environment:

```sh
python3 -m virtualenv env
source env/bin/activate
```

install requirements

```sh
pip3 install -r requirements.txt
```
### Configuration
Edit the config.json file to match your cluster's IP addresses 

### Generate python files
Run both the proto files to generate python files for serializing messages and grpc communication

__raft.proto__

```sh
python3 -m grpc.tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
```

__file_transfer.proto__

```sh
python3 -m grpc.tools.protoc -I. --python_out=. --grpc_python_out=. file_transfer.proto
```

__raft.py__

```sh
cd raft
python3 raft.py
```

__dc.py__

```sh
cd dc
python3 dc.py
```

### To upload a file to data center

```sh
cd client
python3 client_upload.py
```

After running the above, a python daemon watches a directory called "uploads" in the client folder. Place the file that needs to be uploaded in that uploads directory.

### To get the list of files and download a file

```sh
cd client
python3 client_download.py
```

After running the above file, it will display a list of files available for download and gives the user an option, if he/she wants to download a file 


