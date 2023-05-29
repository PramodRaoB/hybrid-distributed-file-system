# hybrid-distributed-file-system

A distributed file system with semantics similar to both google file system and hadoop file system

## Instructions to set up the project

1. Git clone the repository using the command `git clone https://github.com/PramodRaoB/hybrid-distributed-file-system/`
2. Enter into the project directory using the command `cd hybrid-distributed-file-system`
3. Optionally set up a python virtual environment
4. Install the required python packages: `pip install -r requirements.txt`
5. Compile the proto headers by running the command `cd src && make`
6. Done!

## Running the distributed file system

The file system consists of three components, the master server, one or more chunk servers and one or more clients

### Master server

To run the master server, run the command `python master_server.py`

### Chunk server(s)

To run the chunk servers, run the command `python chunk_server.py <0-3>`.

The file system supports multiple chunk-servers. The `<0-3>` option is to be relaced by an integer between 0 and 3
supporting 4 simultaneous chunk servers. This can be increased in `config.py`.

### Client

The client supports multiple commands all of which can be executed simultaneously across multiple client applications.

The syntax to run a client command looks like: `python client.py command args...`

- Copying a new file onto hybrid-DFS
    - `python client.py create <local-file-path> <hybrid-dfs-file-path>`
- Listing the files on hybrid-DFS. The option `1` lists even the files being currently processed (copy/deletion).
    - `python client.py ls <0-1>`
- Reading a file. Setting the last option to `-1` reads until the end of file
    - `python client.py read <file-path> <start-position> <number-of-bytes-to-read>`
- Deleting a file
    - `python client.py delete <file-path>`