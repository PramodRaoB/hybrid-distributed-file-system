# In bytes
CLIENT_RPC_TIMEOUT = 5
CLIENT_RETRY_LIMIT = 5
CHUNK_SIZE = 1024
PACKET_SIZE = 128
REPLICATION_FACTOR = 3
CHUNK_PORTS = ["50052"]
CHUNK_IPS = ["localhost"]
NUM_CHUNK_SERVERS = len(CHUNK_IPS)
CHUNK_LOCS = [CHUNK_IPS[i] + ":" + CHUNK_PORTS[i] for i in range(NUM_CHUNK_SERVERS)]
CHUNK_ROOT_DIRS = ["/tmp/chunk1"]
MASTER_PORT = "50051"
MASTER_IP = "localhost"
MASTER_LOC = MASTER_IP + ":" + MASTER_PORT
