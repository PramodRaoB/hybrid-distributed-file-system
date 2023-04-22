from collections import OrderedDict
from enum import Enum
import hybrid_dfs_pb2_grpc
import hybrid_dfs_pb2


class Status:
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message


class ChunkStatus(Enum):
    TEMPORARY = 0
    FINISHED = 1


class Chunk:
    def __init__(self, chunk_handle: str, chunk_locs):
        self.handle = chunk_handle
        self.locs = chunk_locs
        self.status = ChunkStatus.TEMPORARY

    def __repr__(self):
        res = self.handle + ": " + str(self.locs)
        return res

    def __str__(self):
        return self.__repr__()


class FileStatus(Enum):
    DELETING = 0
    WRITING = 1
    COMMITTED = 2


class File:
    def __init__(self, file_path: str, creation_time):
        self.path = file_path
        self.creation_time = creation_time
        self.chunks = OrderedDict()
        self.status = FileStatus.WRITING

    def __repr__(self):
        res = "{File path: " + self.path
        res += ", Creation time: " + str(self.creation_time)
        res += ", Chunks: " + str(self.chunks)
        res += ", Status: " + str(self.status.name)
        res += "}\n"
        return res

    def __str__(self):
        return self.__repr__()


def stream_list(arr):
    for i in arr:
        yield hybrid_dfs_pb2.String(str=str(i))
