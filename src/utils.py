from collections import OrderedDict
from enum import Enum


class Status:
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message


class Chunk:
    def __init__(self, chunk_handle: str, chunk_locs):
        self.handle = chunk_handle
        self.locs = chunk_locs
        self.status = Enum('Status', ['TEMPORARY', 'FINISHED'])

    def __repr__(self):
        res = self.handle + ": " + str(self.locs)
        return res

    def __str__(self):
        return self.__repr__()


class File:
    def __init__(self, file_path: str, creation_time):
        self.path = file_path
        self.creation_time = creation_time
        self.chunks = OrderedDict()

    def __repr__(self):
        res = "{File path: " + self.path
        res += ", Creation time: " + str(self.creation_time)
        res += ", Chunks: " + str(self.chunks)
        # res += ", Status: " + str(self.status)
        res += "}\n"
        return res

    def __str__(self):
        return self.__repr__()
