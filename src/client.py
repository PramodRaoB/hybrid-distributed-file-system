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

    def create_file(self, local_file_path: str, dfs_file_path: str):
        try:
            num_bytes = os.path.getsize(local_file_path)
        except OSError as e:
            print(e)
            return
        num_chunks = num_bytes // cfg.CHUNK_SIZE + int(num_bytes % cfg.CHUNK_SIZE != 0)
        request = dfs_file_path
        try:
            ret = self.master_stub.create_file(hybrid_dfs_pb2.String(str=request), timeout=cfg.CLIENT_RPC_TIMEOUT)
        except grpc.RpcError as e:
            print(e)
            return
        if ret.code != 0:
            print(ret.message)
            return
        seq_no = 0
        try_count = 0
        success = True
        curr_chunk_handle = ""
        while seq_no < num_chunks:
            request = dfs_file_path + ":" + curr_chunk_handle
            ret_status = self.master_stub.get_chunk_locs(hybrid_dfs_pb2.String(str=request))
            if ret_status.code != 0:
                print(ret_status.message)
                success = False
                break
            chunk = jsonpickle.decode(ret_status.message)
            curr_chunk_handle = chunk.handle
            request_iterator = stream_chunk(local_file_path, seq_no, chunk.handle, chunk.locs)
            ret_status = self.chunk_stubs[chunk.locs[0]].create_chunk(request_iterator, timeout=cfg.CLIENT_RPC_TIMEOUT)
            print(ret_status.message)
            if ret_status.code != 0:
                if try_count == cfg.CLIENT_RETRY_LIMIT:
                    print("Error: Could not create file. Aborting.")
                    success = False
                    break
                try_count += 1
                print(f"Retrying: chunk {seq_no}, attempt {try_count}")
            else:
                request = jsonpickle.encode(chunk)
                ret_status = self.master_stub.commit_chunk(hybrid_dfs_pb2.String(str=request))
                try_count = 0
                seq_no += 1
                curr_chunk_handle = ""
        if success:
            request = dfs_file_path + ":0"
            ret_status = self.master_stub.file_create_status(hybrid_dfs_pb2.String(str=request))
            if ret_status.code != 0:
                print("Could not create file: ", end='')
                print(ret_status.message)
            else:
                print(ret_status.message)
        else:
            request = dfs_file_path + ":1"
            self.master_stub.file_create_status(hybrid_dfs_pb2.String(str=request))
            print("Error: Could not create file")

    def delete_file(self, file_path: str):
        ret_status = self.master_stub.delete_file(hybrid_dfs_pb2.String(str=file_path))
        print(ret_status.message)

    def list_files(self, hidden: int):
        ret_status = self.master_stub.list_files(hybrid_dfs_pb2.String(str=str(hidden)))
        files = jsonpickle.decode(ret_status.message)
        print(files)


def run():
    with Client() as client:
        client.create_file("/home/jade/temp2.txt", "temp2.txt")
        # client.list_files(1)
        # client.delete_file("client.py")


if __name__ == '__main__':
    logging.basicConfig()
    run()
