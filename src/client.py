from __future__ import print_function

import ast
import logging
import json
import os.path

import jsonpickle

import grpc
import hybrid_dfs_pb2
import hybrid_dfs_pb2_grpc
from utils import Chunk
import config as cfg


class Client:
    def __init__(self):
        self.master_channel = grpc.insecure_channel(cfg.MASTER_IP + ":" + cfg.MASTER_SERVER)
        self.master_stub = hybrid_dfs_pb2_grpc.MasterToClientStub(self.master_channel)
        self.chunk_channels = [grpc.insecure_channel(cfg.CHUNK_IPS[i] + ":" + cfg.CHUNK_SERVERS[i]) for i in range(cfg.NUM_CHUNK_SERVERS)]
        self.chunk_stubs = [hybrid_dfs_pb2_grpc.ChunkToClientStub(channel) for channel in self.chunk_channels]

    def close(self):
        self.master_channel.close()
        for channel in self.chunk_channels:
            channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def read_file(self, file_path: str, offset: int, num_bytes: int):
        request = file_path + ':' + str(offset) + ":" + str(num_bytes)
        ret_status = self.chunk_stubs

    def create_file(self, local_file_path: str, dfs_file_path: str):
        try:
            num_bytes = os.path.getsize(local_file_path)
        except OSError as e:
            print(e)
            return
        num_chunks = num_bytes // cfg.CHUNK_SIZE + int(num_bytes % cfg.CHUNK_SIZE != 0)
        request = dfs_file_path + ":" + str(num_chunks)
        ret = self.master_stub.create_file(hybrid_dfs_pb2.String(str=request))
        if ret.code != 0:
            print(ret.message)
            return
        chunk_details = jsonpickle.decode(ret.message)
        print(chunk_details)


def read_file(client: Client):
    request = "ab.txt:0:12"
    ret_status = client.chunk_stubs[0].read_file(hybrid_dfs_pb2.String(str=request))
    print(ret_status.message)


def create_file(client: Client):
    request = "ab1.txt:3"
    ret_status = client.master_stub.create_file(hybrid_dfs_pb2.String(str=request))
    if ret_status.code != 0:
        print(ret_status.message)
        return


def run():
    with Client() as client:
        client.create_file("./client.py", "client.py")


if __name__ == '__main__':
    logging.basicConfig()
    run()
