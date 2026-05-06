from concurrent import futures
import threading
import uuid
import os
from datetime import datetime, timezone

import grpc
from project3_pb2 import *
import project3_pb2_grpc

GRPC_SERVER_PORT = os.environ.get("GRPC_SERVER_PORT", "50051")
NODE_TARGET      = os.environ.get("NODE_TARGET", f"storage-node:{GRPC_SERVER_PORT}")


class StorageService(project3_pb2_grpc.StorageServiceServicer):

    def __init__(self) -> None:
        self.lock  = threading.Lock()
        self.items: dict[str, Item] = {}
        self.bids:  dict[str, list[Bid]] = {}

    def Heartbeat(self, request: HeartbeatRequest, context: grpc.ServicerContext) -> HeartbeatResponse:
        return HeartbeatResponse(alive=True)

    def Create(self, request: CreateRequest, context: grpc.ServicerContext) -> CreateResponse:
        item = Item(
            id=str(uuid.uuid4()),
            seller_id=request.seller_id,
            title=request.title,
            description=request.description,
            category=request.category,
            quantity=request.quantity,
            starting_price=request.starting_price,
            current_price=request.starting_price,
            status=ITEM_AVAILABLE,
            version="1",
        )
        with self.lock:
            self.items[item.id] = item
        print(f"[storage:{NODE_TARGET}] Create id={item.id}")
        return CreateResponse(item=item)

    def Get(self, request: GetRequest, context: grpc.ServicerContext) -> GetResponse:
        with self.lock:
            item = self.items.get(request.item_id)
        if item is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Item {request.item_id!r} not found")
            return GetResponse()
        return GetResponse(item=item)

    def Search(self, request: SearchRequest, context: grpc.ServicerContext) -> SearchResponse:
        with self.lock:
            results = list(self.items.values())

        if request.keyword:
            kw = request.keyword.lower()
            results = [i for i in results if kw in i.title.lower() or kw in i.description.lower()]
        if request.category:
            results = [i for i in results if i.category == request.category]
        if request.status != ITEM_UNKNOWN:
            results = [i for i in results if i.status == request.status]

        page_size = request.page_size if request.page_size > 0 else 20
        return SearchResponse(items=results[:page_size], total_count=len(results))

    def Update(self, request: UpdateRequest, context: grpc.ServicerContext) -> UpdateResponse:
        with self.lock:
            item = self.items.get(request.item_id)
            if item is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return UpdateResponse()
            patch = request.item
            if patch.title:       item.title       = patch.title
            if patch.description: item.description = patch.description
            if patch.category:    item.category    = patch.category
            if patch.quantity:    item.quantity     = patch.quantity
            if patch.status:      item.status       = patch.status
            item.version = str(int(item.version) + 1)
            self.items[request.item_id] = item
        print(f"[storage:{NODE_TARGET}] Update id={request.item_id} version={item.version}")
        return UpdateResponse(item=item)

    def StoreBid(self, request: StoreBidRequest, context: grpc.ServicerContext) -> StoreBidResponse:
        with self.lock:
            item = self.items.get(request.item_id)
            if item is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return StoreBidResponse()

            is_winning = request.amount.amount_small > item.current_price.amount_small
            bid = Bid(
                bid_id=str(uuid.uuid4()),
                item_id=request.item_id,
                bidder_id=request.bidder_id,
                amount=request.amount,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

            if is_winning:
                item.current_price.CopyFrom(request.amount)
                item.version = str(int(item.version) + 1)

        return StoreBidResponse(bid=bid, updated_item=item, is_winning_bid=is_winning)

    def ChangeAuction(self, request: ChangeAuctionRequest, context: grpc.ServicerContext) -> ChangeAuctionResponse:
        status = "joined" if request.HasField("join") else "bid_received"
        return ChangeAuctionResponse(item_id=request.item_id, status_update=status)


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    project3_pb2_grpc.add_StorageServiceServicer_to_server(StorageService(), server)
    server.add_insecure_port(f"[::]:{GRPC_SERVER_PORT}")
    server.start()
    print(f"[storage:{NODE_TARGET}] listening on port {GRPC_SERVER_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()