# In bytes
CLIENT_RPC_TIMEOUT = 100
CLIENT_RETRY_LIMIT = 5
CHUNK_SIZE = 1024
PACKET_SIZE = 128
REPLICATION_FACTOR = 3
CHUNK_PORTS = ["50052", "50053"]
CHUNK_IPS = ["localhost", "localhost"]
CHUNK_ROOT_DIRS = ["/tmp/chunk1", "/tmp/chunk2"]
NUM_CHUNK_SERVERS = len(CHUNK_IPS)
CHUNK_LOCS = [CHUNK_IPS[i] + ":" + CHUNK_PORTS[i] for i in range(NUM_CHUNK_SERVERS)]
MASTER_PORT = "50051"
MASTER_IP = "localhost"
MASTER_LOC = MASTER_IP + ":" + MASTER_PORT
MASTER_LOG = '/tmp/master.log'
LOGGER_CONFIG = '''
{
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {
        "simple": {
            "format": "%(levelname)-8s - %(message)s"
        }
    },
    "filters": {
        "info_and_below": {
            "()" : "__main__.filter_maker",
            "level": "INFO"
        }
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "simple",
            "filename": "app.log",
            "mode": "a",
            "level": "INFO",
            "filters": ["info_and_below"]
        }
    },
    "root": {
        "level": "DEBUG",
        "handlers": [
            "stdout",
            "file"
        ]
    }
}
'''
