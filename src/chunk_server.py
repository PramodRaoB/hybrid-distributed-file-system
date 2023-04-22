import os.path
import sys
from concurrent import futures
import logging
from pathlib import Path
from sys import stderr

import grpc
import jsonpickle

import hybrid_dfs_pb2
import hybrid_dfs_pb2_grpc
from utils import Status, Chunk, stream_list, ChunkStatus
import config as cfg


def stream_chunk(file_path: str, offset: int, num_bytes: int):
    try:
        bytes_read = 0
        with open(file_path, "r", buffering=cfg.PACKET_SIZE) as f:
            f.seek(offset)
            while bytes_read < num_bytes:
                packet = f.read(min(cfg.PACKET_SIZE, num_bytes - bytes_read))
                bytes_read += cfg.PACKET_SIZE
                if len(packet) == 0:
                    break
                yield hybrid_dfs_pb2.String(str=packet)
    except OSError as e:
        # print(e)
        raise OSError(e)


class ChunkServer:
    def __init__(self, port, root_dir):
        self.port = port
        self.root_dir = root_dir
        self.is_visible = {}
        try:
            Path(self.root_dir).mkdir(parents=True, exist_ok=True)
        except FileExistsError as e:
            print(e)
            exit(1)
        self.wake_up()

    def wake_up(self):
        curr_dir = os.fsencode(self.root_dir)
        chunks = []
        try:
            for chunk in os.listdir(curr_dir):
                chunk_handle = os.fsdecode(chunk)
                chunks.append(chunk_handle)
        except OSError as e:
            print(e)
            exit(1)
        to_delete = []
        with grpc.insecure_channel(cfg.MASTER_LOC) as channel:
            master_stub = hybrid_dfs_pb2_grpc.MasterToChunkStub(channel)
            try:
                resp = master_stub.query_chunks(stream_list(chunks), timeout=cfg.CLIENT_RPC_TIMEOUT)
            except grpc.RpcError as e:
                print(e)
                print("Cannot wake up when master is dead")
                exit(1)
            try:
                for request in resp:
                    chunk_handle, status = request.str.split(':')
                    status = ChunkStatus(int(status))
                    if status == ChunkStatus.DELETED:
                        to_delete.append(chunk_handle)
                    else:
                        self.is_visible[chunk_handle] = False
                        if status == ChunkStatus.FINISHED:
                            self.is_visible[chunk_handle] = True
            except grpc.RpcError as e:
                print(e)
                print("Cannot wake up when master is dead")
                exit(1)
        self.delete_chunks(stream_list(to_delete))

    def read_chunk(self, chunk_handle, offset: int, num_bytes: int):
        if chunk_handle not in self.is_visible.keys():
            raise EnvironmentError("Chunk not found")
        if not self.is_visible[chunk_handle]:
            raise EnvironmentError("Chunk currently being modified")
        data_iterator = stream_chunk(os.path.join(self.root_dir, chunk_handle), offset, num_bytes)
        return data_iterator

    def write_and_yield_chunk(self, chunk_handle: str, loc_list, data_iterator):
        yield hybrid_dfs_pb2.String(str=chunk_handle)
        yield hybrid_dfs_pb2.String(str=jsonpickle.encode(loc_list))
        try:
            with open(os.path.join(self.root_dir, chunk_handle), "w") as f:
                self.is_visible[chunk_handle] = False
                for data in data_iterator:
                    f.write(data.str)
                    yield data
        except (EnvironmentError, grpc.RpcError) as e:
            print(e)
            raise Exception(e)

    def write_chunk(self, chunk_handle: str, data_iterator):
        try:
            with open(os.path.join(self.root_dir, chunk_handle), "w") as f:
                self.is_visible[chunk_handle] = False
                for data in data_iterator:
                    f.write(data.str)
            return Status(0, "Chunk created")
        except (EnvironmentError, grpc.RpcError) as e:
            print(e)
            return Status(-1, "Chunk creation pipeline failed")

    def create_chunk(self, chunk_handle: str, loc_list, data_iterator):
        loc_list.pop(0)
        print(f"Request to create chunk {chunk_handle}")
        if not loc_list:
            return self.write_chunk(chunk_handle, data_iterator)
        else:
            with grpc.insecure_channel(loc_list[0]) as channel:
                destination_stub = hybrid_dfs_pb2_grpc.ChunkToChunkStub(channel)
                return destination_stub.create_chunk(self.write_and_yield_chunk(chunk_handle, loc_list, data_iterator))

    def commit_chunk(self, chunk_handle: str):
        if chunk_handle in self.is_visible.keys():
            self.is_visible[chunk_handle] = True
        else:
            # TODO: Cant happen?
            pass
        return Status(0, "Committed")

    def delete_chunks(self, request_iterator):
        for request in request_iterator:
            try:
                chunk_handle = request.str
                os.remove(os.path.join(self.root_dir, chunk_handle))
                if chunk_handle in self.is_visible.keys():
                    self.is_visible.pop(chunk_handle, None)
            except EnvironmentError as e:
                print(e)
        return Status(0, "Chunk(s) deleted")


class ChunkToClientServicer(hybrid_dfs_pb2_grpc.ChunkToClientServicer):
    """Provides methods that implements functionality of HybridDFS Chunk server"""

    def __init__(self, server: ChunkServer):
        self.server = server

    def create_chunk(self, request_iterator, context):
        chunk_handle = None
        loc_list = None
        for request in request_iterator:
            chunk_handle = request.str
            break
        for request in request_iterator:
            loc_list = jsonpickle.decode(request.str)
            break
        ret_status = self.server.create_chunk(chunk_handle, loc_list, request_iterator)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def read_chunk(self, request, context):
        chunk_handle, offset, num_bytes = request.str.split(':')
        offset = int(offset)
        num_bytes = int(num_bytes)
        return self.server.read_chunk(chunk_handle, offset, num_bytes)


class ChunkToChunkServicer(hybrid_dfs_pb2_grpc.ChunkToChunkServicer):
    def __init__(self, server: ChunkServer):
        self.server = server

    def create_chunk(self, request_iterator, context):
        chunk_handle = None
        loc_list = None
        for request in request_iterator:
            chunk_handle = request.str
            break
        for request in request_iterator:
            loc_list = jsonpickle.decode(request.str)
            break
        ret_status = self.server.create_chunk(chunk_handle, loc_list, request_iterator)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)


class ChunkToMasterServicer(hybrid_dfs_pb2_grpc.ChunkToMasterServicer):
    def __init__(self, server: ChunkServer):
        self.server = server

    def commit_chunk(self, request, context):
        ret_status = self.server.commit_chunk(request.str)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)

    def delete_chunks(self, request_iterator, context):
        ret_status = self.server.delete_chunks(request_iterator)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)


def serve():
    try:
        server_index = int(sys.argv[1])
    except (ValueError, IndexError) as e:
        print(e)
        print(f"Enter a valid server index in the range [0, {cfg.NUM_CHUNK_SERVERS - 1}]")
        exit(1)
    chunk_server = ChunkServer(cfg.CHUNK_PORTS[server_index], cfg.CHUNK_ROOT_DIRS[server_index])
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    hybrid_dfs_pb2_grpc.add_ChunkToClientServicer_to_server(ChunkToClientServicer(chunk_server), server)
    hybrid_dfs_pb2_grpc.add_ChunkToChunkServicer_to_server(ChunkToChunkServicer(chunk_server), server)
    hybrid_dfs_pb2_grpc.add_ChunkToMasterServicer_to_server(ChunkToMasterServicer(chunk_server), server)
    server.add_insecure_port(cfg.CHUNK_LOCS[server_index])
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
