from __future__ import print_function

import logging
import random

import grpc
import hybrid_dfs_pb2
import hybrid_dfs_pb2_grpc


def read_file(stub: hybrid_dfs_pb2_grpc.ChunkToClientStub):
    request = "ab.txt:0:10"
    ret_status = stub.read_file(hybrid_dfs_pb2.String(str=request))
    print(ret_status.message)


def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel('localhost:50052') as channel:
        stub = hybrid_dfs_pb2_grpc.ChunkToClientStub(channel)
        read_file(stub)


if __name__ == '__main__':
    logging.basicConfig()
    run()
