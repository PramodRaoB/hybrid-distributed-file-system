import os.path
from concurrent import futures
import logging
from sys import stderr

import grpc
import hybrid_dfs_pb2
import hybrid_dfs_pb2_grpc
from utils import Status


class ChunkServer:
    def __init__(self, port, root_dir):
        self.port = port
        self.root_dir = root_dir

    def read_file(self, chunk_handle, offset: int, num_bytes: int):
        try:
            with open(os.path.join(self.root_dir, chunk_handle), "r") as f:
                f.seek(offset)
                ret = f.read(num_bytes)
            return Status(0, ret)
        except OSError as e:
            return Status(-1, e.strerror)


class ChunkToClientServicer(hybrid_dfs_pb2_grpc.ChunkToClientServicer):
    """Provides methods that implements functionality of HybridDFS Chunk server"""

    def __init__(self, server: ChunkServer):
        self.server = server

    def read_file(self, request, context):
        chunk_handle, offset, num_bytes = request.str.split(':')
        offset = int(offset)
        num_bytes = int(num_bytes)
        ret_status = self.server.read_file(chunk_handle, offset, num_bytes)
        return hybrid_dfs_pb2.Status(code=ret_status.code, message=ret_status.message)


def serve():
    chunk_server = ChunkServer("50052", "/tmp/chunk1")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    hybrid_dfs_pb2_grpc.add_ChunkToClientServicer_to_server(
        ChunkToClientServicer(chunk_server), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
