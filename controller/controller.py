from concurrent import futures
import threading
import uuid
from datetime import datetime, timezone

import grpc
import project3_pb2 as pb          # fixed: was project2_pb2
import project3_pb2_grpc as pb_grpc  # consistent alias

from utils.config import CONTROLLER_PORT, NODE_PORT
from utils.utils import choose_closest_node, create_storage_node


class ControllerService(pb_grpc.ControllerServiceServicer):  # fixed: was project2_grpc

    def Create(
        self, request: pb.CreateRequest, context: grpc.ServicerContext
    ) -> pb.CreateResponse:
        # TODO: route to storage replica; stub returns an empty item
        item = pb.Item(
            id=str(uuid.uuid4()),
            seller_id=request.seller_id,
            title=request.title,
            description=request.description,
            category=request.category,
            quantity=request.quantity,
            starting_price=request.starting_price,
            current_price=request.starting_price,
            status=pb.ITEM_AVAILABLE,
            version="1",
        )
        return pb.CreateResponse(item=item)

    def Get(
        self, request: pb.GetRequest, context: grpc.ServicerContext
    ) -> pb.GetResponse:
        # TODO: read from storage replica
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details("Item not found")
        return pb.GetResponse()

    def Search(
        self, request: pb.SearchRequest, context: grpc.ServicerContext
    ) -> pb.SearchResponse:
        # TODO: fan-out to storage replicas and merge results
        return pb.SearchResponse(items=[], next_page_token="", total_count=0)

    def Update(
        self, request: pb.UpdateRequest, context: grpc.ServicerContext
    ) -> pb.UpdateResponse:
        # TODO: write to primary replica, propagate to backups
        return pb.UpdateResponse()

    def StoreBid(
        self, request: pb.StoreBidRequest, context: grpc.ServicerContext  # fixed: was UpdateRequest
    ) -> pb.StoreBidResponse:
        # TODO: persist bid and update item's current_price
        bid = pb.Bid(
            bid_id=str(uuid.uuid4()),
            item_id=request.item_id,
            bidder_id=request.bidder_id,
            amount=request.amount,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        return pb.StoreBidResponse(bid=bid, is_winning_bid=False)

    def ChangeAuction(
        self, request: pb.ChangeAuctionRequest, context: grpc.ServicerContext
    ) -> pb.ChangeAuctionResponse:
        # TODO: broadcast bid/join events to all auction subscribers
        return pb.ChangeAuctionResponse(item_id=request.item_id)


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_ControllerServiceServicer_to_server(  # fixed: was project2_pb2_grpc
        ControllerService(), server
    )
    server.add_insecure_port(f"[::]:{CONTROLLER_PORT}")
    server.start()
    print(f"Controller listening on port {CONTROLLER_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()