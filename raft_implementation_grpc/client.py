import grpc, file_transfer_pb2, file_transfer_pb2_grpc, raft_pb2, raft_pb2_grpc

from concurrent import futures

def run():
    # stub_raft = raft_pb2_grpc.raftImplemetationStub(grpc.insecure_channel("localhost:4000"))

    stub_file_transfer = file_transfer_pb2_grpc.DataTransferServiceStub(grpc.insecure_channel("localhost:4000"))
    recv = stub_file_transfer.ListFiles(file_transfer_pb2.RequestFileList(isClient=True))
    print(recv.lstFileNames)


run()