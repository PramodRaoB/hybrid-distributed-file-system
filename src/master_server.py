from concurrent import futures
import logging

import grpc
import hybrid_dfs_pb2
import hybrid_dfs_pb2_grpc


class MetaData:
    def __init__(self):
        pass


class MasterServer:
    def __init__(self):
        self.meta = MetaData()

    def create_file(self, file_path: str):
        pass


class MasterToClientServicer(hybrid_dfs_pb2_grpc.MasterToClientServicer):
    """Provides methods that implements functionality of HybridDFS Master server"""


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    hybrid_dfs_pb2_grpc.add_MasterToClientServicer_to_server(
        MasterToClientServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
