from setuptools import setup, find_packages

setup(
    name="data-loader_cli",
    version="0.1",
    py_modules=["cli","grpc_client","dataloader_pb2","dataloader_pb2_grpc"],
    packages=find_packages(),
    include_package_data=True,
    install_requires=["click","grpcio","tqdm"],
    entry_points='''
        [console_scripts]
        loader=cli:cli
    ''',
)