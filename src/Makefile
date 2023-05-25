all: compile

compile: hybrid_dfs.proto
	python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. hybrid_dfs.proto

clean:
	rm -rf __pychache__
	rm -rf hybrid_dfs_pb2_grpc.py
	rm -rf hybrid_dfs_pb2.py
	rm -rf hybrid_dfs_pb2.pyi
