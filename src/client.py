from __future__ import print_function

import os.path
import sys
import time

import grpc
import jsonpickle

import config as cfg
import hybrid_dfs_pb2
import hybrid_dfs_pb2_grpc


def stream_chunk(file_path: str, chunk_index: int, chunk_handle: str, loc_list):
    yield hybrid_dfs_pb2.String(str=chunk_handle)
    yield hybrid_dfs_pb2.String(str=jsonpickle.encode(loc_list))
    try:
        with open(file_path, "r", buffering=cfg.PACKET_SIZE) as f:
            f.seek(chunk_index * cfg.CHUNK_SIZE)
            for _ in range(cfg.CHUNK_SIZE // cfg.PACKET_SIZE):
                packet = f.read(cfg.PACKET_SIZE)
                if len(packet) == 0:
                    break
                yield hybrid_dfs_pb2.String(str=packet)
    except EnvironmentError as e:
        print(e)
        raise Exception(e)


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
        if offset < 0 or num_bytes < -1:
            print("Invalid input")
            return
        if num_bytes == 0:
            print("")
            return
        start = offset // cfg.CHUNK_SIZE
        end = -1
        if num_bytes != -1:
            end = (offset + num_bytes - 1) // cfg.CHUNK_SIZE
        index = start
        while end == -1 or index <= end:
            request = file_path + ":" + str(index)
            try:
                ret_status = self.master_stub.get_chunk_details(hybrid_dfs_pb2.String(str=request),
                                                                timeout=cfg.CLIENT_RPC_TIMEOUT)
            except grpc.RpcError as e:
                print(e)
                return
            if ret_status.code != 0:
                if end != -1 or index == start:
                    print("Error: Failed to fetch file details")
                    print(ret_status.message)
                print("")
                return
            chunk = jsonpickle.decode(ret_status.message)
            chunk_start = 0
            if index == start:
                chunk_start = offset % cfg.CHUNK_SIZE
            chunk_end = cfg.CHUNK_SIZE
            if index == end:
                chunk_end = (offset + num_bytes - 1) % cfg.CHUNK_SIZE
            request = chunk.handle + ":" + str(chunk_start) + ":" + str(chunk_end - chunk_start + 1)
            success = False
            for loc in chunk.locs:
                chunk_data = ""
                try:
                    data_iterator = self.chunk_stubs[loc].read_chunk(hybrid_dfs_pb2.String(str=request),
                                                                     timeout=cfg.CLIENT_RPC_TIMEOUT)
                    for data in data_iterator:
                        chunk_data += data.str
                    print(chunk_data, end='')
                    success = True
                    break
                except (OSError, grpc.RpcError):
                    pass
            if not success:
                print("Error: Failed to fetch chunk. Aborting")
                return
            index += 1
        print("")

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
            try:
                ret_status = self.master_stub.get_chunk_locs(hybrid_dfs_pb2.String(str=request),
                                                             timeout=cfg.CLIENT_RPC_TIMEOUT)
            except grpc.RpcError as e:
                print(e)
                success = False
                break
            if ret_status.code != 0:
                print(ret_status.message)
                success = False
                break
            chunk = jsonpickle.decode(ret_status.message)
            curr_chunk_handle = chunk.handle
            request_iterator = stream_chunk(local_file_path, seq_no, chunk.handle, chunk.locs)
            ret_status = None
            try:
                ret_status = self.chunk_stubs[chunk.locs[0]].create_chunk(request_iterator,
                                                                          timeout=cfg.CLIENT_RPC_TIMEOUT)
                print(ret_status.message)
            except grpc.RpcError as e:
                print(e)
            if not ret_status or ret_status.code != 0:
                if try_count == cfg.CLIENT_RETRY_LIMIT:
                    print("Error: Could not create file. Aborting.")
                    success = False
                    break
                try_count += 1
                print(f"Retrying: chunk {seq_no}, attempt {try_count}")
                time.sleep(cfg.CLIENT_RETRY_INTERVAL)
            else:
                request = dfs_file_path + ":" + curr_chunk_handle
                try:
                    ret_status = self.master_stub.commit_chunk(hybrid_dfs_pb2.String(str=request),
                                                               timeout=cfg.CLIENT_RPC_TIMEOUT)
                except grpc.RpcError as e:
                    print(e)
                    success = False
                    break
                if ret_status.code != 0:
                    success = False
                    break
                try_count = 0
                seq_no += 1
                curr_chunk_handle = ""
        if success:
            request = dfs_file_path + ":0"
            try:
                ret_status = self.master_stub.file_create_status(hybrid_dfs_pb2.String(str=request),
                                                                 timeout=cfg.CLIENT_RPC_TIMEOUT)
            except grpc.RpcError as e:
                print(e)
                print("Cannot connect to master. Check with the master to see if the file was successfully created")
                return
            print(ret_status.message)
        else:
            request = dfs_file_path + ":1"
            try:
                self.master_stub.file_create_status(hybrid_dfs_pb2.String(str=request), timeout=cfg.CLIENT_RPC_TIMEOUT)
            except grpc.RpcError as e:
                print(e)
            print("Error: Could not create file")

    def delete_file(self, file_path: str):
        try:
            ret_status = self.master_stub.delete_file(hybrid_dfs_pb2.String(str=file_path),
                                                      timeout=cfg.CLIENT_RPC_TIMEOUT)
        except grpc.RpcError as e:
            print(e)
            return
        print(ret_status.message)

    def list_files(self, hidden: int):
        try:
            data_iterator = self.master_stub.list_files(hybrid_dfs_pb2.String(str=str(hidden)),
                                                        timeout=cfg.CLIENT_RPC_TIMEOUT)
            for file in data_iterator:
                print(file.str)
        except (OSError, grpc.RpcError) as e:
            print(e)


def run(command, args):
    with Client() as client:
        # client.create_file("/home/jade/use.txt", "use")
        # client.list_files(1)
        # client.read_file("use", 0, 99)
        # client.delete_file("use")
        # client.read_file("test.py", 3, 20)
        if command == "create":
            client.create_file(args[0], args[1])
        elif command == "ls":
            client.list_files(int(args[0]))
        elif command == "read":
            client.read_file(args[0], int(args[1]), int(args[2]))
        elif command == "delete":
            client.delete_file(args[0])
        else:
            print("Invalid Command")


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2:])
