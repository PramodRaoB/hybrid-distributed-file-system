import os.path
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
from utils import Status, Chunk, File, stream_list, ChunkStatus, FileStatus, Logger
import config as cfg


def get_new_handle():
    return str(uuid.uuid4())


class MetaData:
    def __init__(self, log_file):
        self.files = {}
        self.chunk_to_file = {}
        self.wake_up(log_file)
        print("Master metadata:")
        print(self.files)

    def wake_up(self, log_file):
        print(f'Starting Master server. Reading log from {log_file}')
        if os.path.exists(log_file):
            try:
                with open(log_file, 'r') as f:
                    while True:
                        line = f.readline().strip()
                        if len(line) == 0:
                            break
                        print(line)
                        line = line.split()
                        op = line.pop(0)
                        if op == 'add_file':
                            file_path, create_time = line
                            self.files[file_path] = File(file_path, float(create_time))
                        elif op == 'add_chunk':
                            file_path, chunk_handle = line
                            file = self.files[file_path]
                            file.chunks[chunk_handle] = Chunk(chunk_handle, [])
                            self.chunk_to_file[chunk_handle] = file.path
                        elif op == 'change_chunk_locs':
                            file_path = line.pop(0)
                            chunk_handle = line.pop(0)
                            new_locs = ' '.join(line)
                            new_locs = jsonpickle.decode(new_locs)
                            file = self.files[file_path]
                            chunk = file.chunks[chunk_handle]
                            chunk.locs = new_locs
                        elif op == 'commit_chunk':
                            file_path, chunk_handle = line
                            file = self.files[file_path]
                            chunk = file.chunks[chunk_handle]
                            chunk.status = ChunkStatus.FINISHED
                        elif op == 'commit_file':
                            file_path = line[0]
                            self.files[file_path].status = FileStatus.COMMITTED
                        elif op == 'delete_file':
                            file_path = str(line[0])
                            for chunk in self.files[file_path].chunks.keys():
                                self.chunk_to_file.pop(chunk, None)
                            self.files.pop(file_path, None)
                        else:
                            print(f"Error reading log: Invalid entry: {op}")
            except OSError as e:
                print(e)
                print("Could not read log file")

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
        self.meta = MetaData(cfg.MASTER_LOG)
        self.logger = Logger(cfg.MASTER_LOG)
        self.all_chunk_servers = cfg.CHUNK_LOCS
        self.available_chunk_servers = []

    def __get_new_locs(self):
        return random.sample(self.available_chunk_servers,
                             min(cfg.REPLICATION_FACTOR, len(self.available_chunk_servers)))

    def create_file(self, file_path: str):
        if self.meta.does_exist(file_path):
            return Status(-1, "File already exists")
        # TODO: Check available space before creating
        new_file = File(file_path, time.time())
        self.logger.add_file(new_file)
        self.meta.files[file_path] = new_file
        return Status(0, "File created")

    def get_chunk_locs(self, file_path: str, chunk_handle: str):
        if not self.meta.does_exist(file_path):
            return Status(-1, "Requested file does not exist")
        file = self.meta.files[file_path]
        if not chunk_handle:
            chunk_handle = get_new_handle()
            self.logger.add_chunk(file_path, chunk_handle)
            file.chunks[chunk_handle] = Chunk(chunk_handle, [])
        if chunk_handle not in file.chunks.keys():
            return Status(-1, "Requested chunk does not exist")
        chunk = file.chunks[chunk_handle]
        new_locs = self.__get_new_locs()
        # if len(new_locs) != cfg.REPLICATION_FACTOR:
        #     return Status(0, "Too few chunkservers. Could not replicate chunk")
        self.logger.change_chunk_locs(file_path, chunk_handle, new_locs)
        chunk.locs = new_locs
        self.meta.chunk_to_file[chunk.handle] = file.path
        return Status(0, jsonpickle.encode(chunk))

    def commit_chunk(self, file_path: str, chunk_handle: str):
        if not self.meta.does_exist(file_path):
            return Status(-1, "File not found")
        file = self.meta.files[file_path]
        if chunk_handle not in file.chunks.keys():
            return Status(-1, "Chunk not found")
        chunk = file.chunks[chunk_handle]
        self.logger.commit_chunk(file_path, chunk_handle)
        chunk.status = ChunkStatus.FINISHED
        for loc in chunk.locs:
            with grpc.insecure_channel(loc) as channel:
                chunk_stub = hybrid_dfs_pb2_grpc.ChunkToMasterStub(channel)
                try:
                    ret_status = chunk_stub.commit_chunk(hybrid_dfs_pb2.String(str=chunk.handle))
                except grpc.RpcError as e:
                    self.logger.log.error(e)
        return Status(0, "Committed chunks")

    def file_create_status(self, file_path: str, status: int):
        if status:
            return self.delete_file(file_path, 0)
        else:
            file = self.meta.files[file_path]
            self.logger.commit_file(file_path)
            file.status = FileStatus.COMMITTED
            return Status(0, "File committed")

    def delete_chunks(self, loc: str, chunk_handles):
        with grpc.insecure_channel(loc) as channel:
            chunk_stub = hybrid_dfs_pb2_grpc.ChunkToMasterStub(channel)
            try:
                ret_status = chunk_stub.delete_chunks(stream_list(chunk_handles))
                self.logger.log.debug(ret_status.message)
            except grpc.RpcError as e:
                self.logger.log.error(e)
        return Status(0, "Chunk deletion handled")

    def delete_file(self, file_path: str, check_for_commit: int):
        if not self.meta.does_exist(file_path):
            return Status(-1, "File does not exist")
        file = self.meta.files[file_path]
        if check_for_commit and file.status != FileStatus.COMMITTED:
            return Status(-1, "File currently being deleted or written to")
        self.logger.delete_file(file_path)
        file.status = FileStatus.DELETING
        for k, v in file.chunks.items():
            v.status = ChunkStatus.TEMPORARY
            self.meta.chunk_to_file.pop(v.handle, None)
        loc_list = chunks_to_locs(list(file.chunks.values()))
        for k, v in loc_list.items():
            ret_status = self.delete_chunks(k, v)
            self.logger.log.debug(ret_status.message)
        self.meta.files.pop(file_path, None)
        return Status(0, "File deletion successful")

    def list_files(self, temporary: int):
        ret = []
        for file in self.meta.files.values():
            if file.status == FileStatus.COMMITTED:
                ret.append(file.display())
            elif file.status == FileStatus.WRITING and temporary:
                ret.append(file.display())
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

    def query_chunks(self, loc: str, request_iterator):
        for request in request_iterator:
            chunk_handle = request.str
            send_request = chunk_handle + ":" + str(ChunkStatus.DELETED.value)
            if chunk_handle not in self.meta.chunk_to_file.keys():
                # instruct to delete
                yield hybrid_dfs_pb2.String(str=send_request)
                continue
            file_path = self.meta.chunk_to_file[chunk_handle]
            if file_path not in self.meta.files.keys():
                # instruct to delete
                yield hybrid_dfs_pb2.String(str=send_request)
                continue
            file = self.meta.files[file_path]
            chunk = file.chunks[chunk_handle]
            if loc not in chunk.locs:
                yield hybrid_dfs_pb2.String(str=send_request)
                continue
            send_request = chunk_handle + ":" + str(chunk.status.value)
            yield hybrid_dfs_pb2.String(str=send_request)

    def send_heartbeat(self):
        for loc in self.all_chunk_servers:
            with grpc.insecure_channel(loc) as channel:
                chunk_stub = hybrid_dfs_pb2_grpc.ChunkToMasterStub(channel)
                try:
                    ret_status = chunk_stub.heartbeat(hybrid_dfs_pb2.String(str=""), timeout=1)
                    if loc not in self.available_chunk_servers:
                        self.available_chunk_servers.append(loc)
                except grpc.RpcError as e:
                    if loc in self.available_chunk_servers:
                        self.available_chunk_servers.remove(loc)


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


class MasterToChunkServicer(hybrid_dfs_pb2_grpc.MasterToChunkServicer):
    def __init__(self, server: MasterServer):
        self.master = server

    def query_chunks(self, request_iterator, context):
        loc = None
        for request in request_iterator:
            loc = request.str
            break
        return self.master.query_chunks(loc, request_iterator)


def serve():
    master_server = MasterServer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    hybrid_dfs_pb2_grpc.add_MasterToClientServicer_to_server(MasterToClientServicer(master_server), server)
    hybrid_dfs_pb2_grpc.add_MasterToChunkServicer_to_server(MasterToChunkServicer(master_server), server)
    server.add_insecure_port(cfg.MASTER_LOC)
    server.start()
    # server.wait_for_termination()
    try:
        while True:
            time.sleep(cfg.HEARTBEAT_INTERVAL)
            master_server.send_heartbeat()
    except KeyboardInterrupt as e:
        pass


if __name__ == '__main__':
    serve()
