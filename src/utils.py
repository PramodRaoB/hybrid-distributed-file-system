class Status:
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message


class Chunk:
    def __init__(self, chunk_handle: str, chunk_locs):
        self.handle = chunk_handle
        self.locs = chunk_locs

    def __str__(self):
        res = self.handle + ": " + str(self.locs)
        return res
