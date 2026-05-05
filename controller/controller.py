from concurrent import futures
import threading

import grpc
from project2_pb2 import *
import project3_pb2_grpc
from utils.config import CONTROLLER_PORT, NODE_PORT
from utils.utils import choose_closest_node, create_storage_node


class ControllerService(project2_grpc.ControllerServiceServicer):
    def Create(self, request: CreateRequest, context: grpc.ServicerContext) -> CreateResponse:
      return None

    def Get(self, request: GetRequest, context: grpc.ServicerContext
    ) -> GetResponse:
        return None

    def Update(self, request: UpdateRequest, context: grpc.ServicerContext
    ) -> UpdateResponse:
        return None
    

    def StoreBid(self, request: UpdateRequest, context: grpc.ServicerContext
    ) -> StoreBidResponse:
        return None
    
    def ChangeAuction(self, request: ChangeAuctionRequest, context: grpc.ServicerContext
    ) -> ChangeAuctionResponse:
        return None

def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    project2_pb2_grpc.add_ControllerServiceServicer_to_server(ControllerService(), server)
    server.add_insecure_port(f"[::]:{CONTROLLER_PORT}")
    server.start()
    print(f"Controller listening on {CONTROLLER_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()