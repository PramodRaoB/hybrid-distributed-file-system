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


def stream_chunk(file_path: str, chunk_index: int, chunk_handle: str, loc_list):
    try:
        with open(file_path, "r", buffering=cfg.PACKET_SIZE) as f:
            f.seek(chunk_index * cfg.CHUNK_SIZE)
            yield hybrid_dfs_pb2.String(str=chunk_handle)
            yield hybrid_dfs_pb2.String(str=jsonpickle.encode(loc_list))
            for _ in range(cfg.CHUNK_SIZE // cfg.PACKET_SIZE):
                packet = f.read(cfg.PACKET_SIZE)
                if len(packet) == 0:
                    break
                yield hybrid_dfs_pb2.String(str=packet)
    except EnvironmentError as e:
        print(e)


class Client:
    def __init__(self):
        self.master_channel = grpc.insecure_channel(cfg.MASTER_IP + ":" + cfg.MASTER_PORT)
        self.master_stub = hybrid_dfs_pb2_grpc.MasterToClientStub(self.master_channel)
        self.chunk_channels = [grpc.insecure_channel(cfg.CHUNK_IPS[i] + ":" + cfg.CHUNK_PORTS[i]) for i in
                               range(cfg.NUM_CHUNK_SERVERS)]
        self.chunk_stubs = [hybrid_dfs_pb2_grpc.ChunkToClientStub(channel) for channel in self.chunk_channels]
        self.chunk_stubs = {cfg.CHUNK_LOCS[i]: hybrid_dfs_pb2_grpc.ChunkToClientStub(self.chunk_channels[i]) for i in
                            range(cfg.NUM_CHUNK_SERVERS)}

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
        try:
            ret = self.master_stub.create_file(hybrid_dfs_pb2.String(str=request), timeout=cfg.CLIENT_RPC_TIMEOUT)
        except grpc.RpcError as e:
            print(e)
            return
        if ret.code != 0:
            print(ret.message)
            return
        chunk_details = jsonpickle.decode(ret.message)
        seq_no = 0
        try_count = 0
        while seq_no < len(chunk_details):
            chunk = chunk_details[seq_no]
            request_iterator = stream_chunk(local_file_path, seq_no, chunk.handle, chunk.locs)
            ret_status = self.chunk_stubs[chunk.locs[0]].create_chunk(request_iterator, timeout=cfg.CLIENT_RPC_TIMEOUT)
            print(ret_status.message)
            if ret_status.code != 0:
                if try_count == cfg.CLIENT_RETRY_LIMIT:
                    print("Error: Could not create file. Abandoning.")
                    return
                try_count += 1
                print(f"Retrying: {try_count}")
                request = dfs_file_path + ":" + chunk.handle
                ret_status = self.master_stub.retry_chunk(hybrid_dfs_pb2.String(str=request))
                print(ret_status.message)
                chunk.locs = jsonpickle.decode(ret_status.message)
            else:
                try_count = 0
                seq_no += 1

    def delete_file(self, file_path: str):
        ret_status = self.master_stub.delete_file(hybrid_dfs_pb2.String(str=file_path))
        print(ret_status.message)

    def list_files(self, hidden: int):
        ret_status = self.master_stub.list_files(hybrid_dfs_pb2.String(str=str(hidden)))
        files = jsonpickle.decode(ret_status.message)
        print(files)


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
        # client.list_files(1)
        # client.delete_file("client.py")


if __name__ == '__main__':
    logging.basicConfig()
    run()
