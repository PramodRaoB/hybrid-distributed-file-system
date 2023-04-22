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
from utils import Status, Chunk, File, stream_list, ChunkStatus, FileStatus
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


def chunks_to_locs(chunks):
    locs_list = {}
    for chunk in chunks:
        for loc in chunk.locs:
            if loc not in locs_list.keys():
                locs_list[loc] = []
            locs_list[loc].append(chunk.handle)
    return locs_list


class MasterServer:
    def __init__(self):
        self.meta = MetaData()
        self.all_chunk_servers = cfg.CHUNK_LOCS
        self.available_chunk_servers = cfg.CHUNK_LOCS

    def __get_new_locs(self):
        return random.sample(self.available_chunk_servers,
                             min(cfg.REPLICATION_FACTOR, len(self.available_chunk_servers)))

    def create_file(self, file_path: str):
        if self.meta.does_exist(file_path):
            return Status(-1, "File already exists")
        # TODO: Check available space before creating
        new_file = File(file_path, time.time())
        self.meta.files[file_path] = new_file
        return Status(0, "File created")

    def get_chunk_locs(self, file_path: str, chunk_handle: str):
        if not self.meta.does_exist(file_path):
            return Status(-1, "Requested file does not exist")
        file = self.meta.files[file_path]
        if not chunk_handle:
            chunk_handle = get_new_handle()
            file.chunks[chunk_handle] = Chunk(chunk_handle, [])
        if chunk_handle not in file.chunks.keys():
            return Status(-1, "Requested chunk does not exist")
        chunk = file.chunks[chunk_handle]
        chunk.locs = self.__get_new_locs()
        return Status(0, jsonpickle.encode(chunk))

    def commit_chunk(self, file_handle: str, chunk_handle: str):
        if not self.meta.does_exist(file_handle):
            return Status(-1, "File not found")
        file = self.meta.files[file_handle]
        if chunk_handle not in file.chunks.keys():
            return Status(-1, "Chunk not found")
        chunk = file.chunks[chunk_handle]
        chunk.status = ChunkStatus.FINISHED
        for loc in chunk.locs:
            with grpc.insecure_channel(loc) as channel:
                chunk_stub = hybrid_dfs_pb2_grpc.ChunkToMasterStub(channel)
                try:
                    ret_status = chunk_stub.commit_chunk(hybrid_dfs_pb2.String(str=chunk.handle))
                    print(ret_status.message)
                except grpc.RpcError as e:
                    print(e)
        return Status(0, "Committed chunks")

    def file_create_status(self, file_path: str, status: int):
        if status:
            return self.delete_file(file_path, 0)
        else:
            file = self.meta.files[file_path]
            file.status = FileStatus.COMMITTED
            return Status(0, "File committed")

    def delete_chunks(self, loc: str, chunk_handles):
        with grpc.insecure_channel(loc) as channel:
            chunk_stub = hybrid_dfs_pb2_grpc.ChunkToMasterStub(channel)
            try:
                ret_status = chunk_stub.delete_chunks(stream_list(chunk_handles))
                print(ret_status.message)
            except grpc.RpcError as e:
                print(e)
        return Status(0, "Chunk deletion handled")

    def delete_file(self, file_path: str, check_for_commit: int):
        if not self.meta.does_exist(file_path):
            return Status(-1, "File does not exist")
        file = self.meta.files[file_path]
        if check_for_commit and file.status != FileStatus.COMMITTED:
            return Status(-1, "File currently being deleted or written to")
        file.status = FileStatus.DELETING
        for k, v in file.chunks.items():
            v.status = ChunkStatus.TEMPORARY
        loc_list = chunks_to_locs(list(file.chunks.values()))
        for k, v in loc_list.items():
            ret_status = self.delete_chunks(k, v)
            print(ret_status.message)
        self.meta.files.pop(file_path, None)
        return Status(0, "File deletion successful")

    def list_files(self, temporary: int):
        ret = []
        for file in self.meta.files.values():
            if file.status == FileStatus.COMMITTED:
                ret.append(file)
            elif file.status == FileStatus.WRITING and temporary:
                ret.append(file)
        return stream_list(ret)

    def get_chunk_details(self, file_path: str, chunk_index: int):
        if not self.meta.does_exist(file_path):
            return Status(-1, "File not found")
        file = self.meta.files[file_path]
        if file.status == FileStatus.DELETING:
            return Status(-1, "File being deleted. Cannot read.")
        if chunk_index >= len(file.chunks):
            return Status(-1, "EOF reached")
        return Status(0, jsonpickle.encode(list(file.chunks.items())[chunk_index][1]))


class MasterToClientServicer(hybrid_dfs_pb2_grpc.MasterToClientServicer):
    """Provides methods that implements functionality of HybridDFS Master server"""

    def __init__(self, server: MasterServer):
        self.master = server

    def create_file(self, request, context):
        file_path = request.str
        ret_status = self.master.create_file(file_path)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def delete_file(self, request, context):
        file_path = request.str
        ret_status = self.master.delete_file(file_path, 1)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def list_files(self, request, context):
        temporary = int(request.str)
        return self.master.list_files(temporary)

    def get_chunk_locs(self, request, context):
        file_path, chunk_handle = request.str.split(':')
        ret_status = self.master.get_chunk_locs(file_path, chunk_handle)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def commit_chunk(self, request, context):
        file_handle, chunk_handle = request.str.split(':')
        ret_status = self.master.commit_chunk(file_handle, chunk_handle)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def file_create_status(self, request, context):
        file_path, status = request.str.split(':')
        status = int(status)
        ret_status = self.master.file_create_status(file_path, status)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def get_chunk_details(self, request, context):
        file_path, chunk_index = request.str.split(':')
        chunk_index = int(chunk_index)
        ret_status = self.master.get_chunk_details(file_path, chunk_index)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)


def serve():
    master_server = MasterServer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    hybrid_dfs_pb2_grpc.add_MasterToClientServicer_to_server(
        MasterToClientServicer(master_server), server)
    server.add_insecure_port(cfg.MASTER_LOC)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
