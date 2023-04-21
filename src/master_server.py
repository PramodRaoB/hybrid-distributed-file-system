import random
import time
import uuid
from collections import OrderedDict
from concurrent import futures
import logging
import json
from copy import deepcopy

import jsonpickle

import grpc
import hybrid_dfs_pb2
import hybrid_dfs_pb2_grpc
from utils import Status, Chunk, File
import config as cfg


def get_new_handle():
    return str(uuid.uuid4())


class MetaData:
    def __init__(self):
        self.files = {}
        self.to_delete = set()
        self.uploading = set()

    def does_exist(self, file_path: str):
        if file_path in self.files.keys():
            return True
        return False


class MasterServer:
    def __init__(self):
        self.meta = MetaData()
        self.all_chunk_servers = cfg.CHUNK_LOCS
        self.available_chunk_servers = cfg.CHUNK_LOCS

    def __get_new_locs(self):
        return random.sample(self.available_chunk_servers,
                             min(cfg.REPLICATION_FACTOR, len(self.available_chunk_servers)))

    def create_file(self, file_path: str, num_chunks: int):
        if self.meta.does_exist(file_path):
            return Status(-1, "File already exists")
        # TODO: Check available space before creating
        new_file = File(file_path, time.time())
        for ch in range(num_chunks):
            handle = get_new_handle()
            locs = self.__get_new_locs()
            if len(locs) == 0:
                return Status(-1, "No available chunk servers")
            new_chunk = Chunk(handle, locs)
            new_file.chunks[handle] = new_chunk
        self.meta.files[file_path] = new_file
        return Status(0, jsonpickle.encode(list(new_file.chunks.values())))

    def delete_file(self, file_path: str):
        if not self.meta.does_exist(file_path):
            return Status(-1, "File does not exist")
        new_file = deepcopy(self.meta.files[file_path])
        new_file.path = "." + new_file.path
        new_file.is_deleted = True
        self.meta.files[new_file.path] = new_file
        self.meta.files.pop(file_path, None)
        return Status(0, "Successfully marked for deletion")

    def list_files(self, hidden: int):
        ret = []
        for k, v in self.meta.files.items():
            ret.append(v)
        return Status(0, jsonpickle.encode(ret))


class MasterToClientServicer(hybrid_dfs_pb2_grpc.MasterToClientServicer):
    """Provides methods that implements functionality of HybridDFS Master server"""

    def __init__(self, server: MasterServer):
        self.master = server

    def create_file(self, request, context):
        file_path, num_chunks = request.str.split(':')
        num_chunks = int(num_chunks)
        ret_status = self.master.create_file(file_path, num_chunks)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def delete_file(self, request, context):
        file_path = request.str
        ret_status = self.master.delete_file(file_path)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def list_files(self, request, context):
        hidden = int(request.str)
        ret_status = self.master.list_files(hidden)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)


def serve():
    master_server = MasterServer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    hybrid_dfs_pb2_grpc.add_MasterToClientServicer_to_server(
        MasterToClientServicer(master_server), server)
    # server.add_insecure_port('[::]:50051')
    server.add_insecure_port(cfg.MASTER_IP + ":" + cfg.MASTER_SERVER)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
