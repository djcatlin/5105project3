from concurrent import futures
import threading
import time
import os

import grpc
from project3_pb2 import *
import project3_pb2_grpc

GRPC_SERVER_PORT   = os.environ.get("GRPC_SERVER_PORT", "50060")
NODE_TARGET        = os.environ.get("NODE_TARGET", f"service-node:{GRPC_SERVER_PORT}")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))

STORAGE_NODES = [
    "storage-node-1:50051",
    "storage-node-2:50052",
    "storage-node-3:50053",
]


class StorageRegistry:
    """
    Thread-safe primary-backup registry for storage replicas.
    Stubs are created once at init and reused for all calls — storage
    targets are static so there is no need to ever recreate them.
    """

    def __init__(self, targets: list[str]) -> None:
        self.lock           = threading.Lock()
        self.targets        = list(targets)
        self.healthy        = set()
        self.primary_target = None
        self.stubs          = {
            t: project3_pb2_grpc.StorageServiceStub(grpc.insecure_channel(t))
            for t in targets
        }

    def mark_healthy(self, target: str) -> None:
        with self.lock:
            self.healthy.add(target)
            if self.primary_target is None:
                self.primary_target = target
                print(f"[service:{NODE_TARGET}] primary storage = {target}")

    def mark_dead(self, target: str) -> None:
        with self.lock:
            if target in self.healthy:
                print(f"[service:{NODE_TARGET}] storage {target} marked dead")
            self.healthy.discard(target)
            if self.primary_target == target:
                self.primary_target = next(
                    (t for t in self.targets if t in self.healthy), None
                )
                print(f"[service:{NODE_TARGET}] primary failed over to {self.primary_target}")

    def get_primary_stub(self) -> project3_pb2_grpc.StorageServiceStub | None:
        with self.lock:
            if self.primary_target and self.primary_target in self.healthy:
                return self.stubs[self.primary_target]
            return None

    def get_backup_stubs(self) -> list[project3_pb2_grpc.StorageServiceStub]:
        with self.lock:
            return [
                self.stubs[t] for t in self.targets
                if t in self.healthy and t != self.primary_target
            ]

    def all_targets(self) -> list[str]:
        with self.lock:
            return list(self.targets)


storage: StorageRegistry = None


def heartbeat_loop() -> None:
    while True:
        for target in storage.all_targets():
            try:
                storage.stubs[target].Heartbeat(HeartbeatRequest(), timeout=2)
                storage.mark_healthy(target)
            except grpc.RpcError:
                storage.mark_dead(target)
        time.sleep(HEARTBEAT_INTERVAL)


def replicate(method: str, request) -> None:
    def _send(stub: project3_pb2_grpc.StorageServiceStub) -> None:
        try:
            getattr(stub, method)(request)
        except grpc.RpcError as e:
            print(f"[service:{NODE_TARGET}] replication error {method}: {e.details()}")

    threads = [
        threading.Thread(target=_send, args=(s,), daemon=True)
        for s in storage.get_backup_stubs()
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)


class ServiceNodeService(project3_pb2_grpc.ServiceNodeServiceServicer):

    def _get_primary_stub(self, context: grpc.ServicerContext) -> project3_pb2_grpc.StorageServiceStub | None:
        stub = storage.get_primary_stub()
        if stub is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("No healthy storage nodes available")
        return stub

    def Heartbeat(self, request: HeartbeatRequest, context: grpc.ServicerContext) -> HeartbeatResponse:
        return HeartbeatResponse(alive=True)

    def HandleCreate(self, request: CreateRequest, context: grpc.ServicerContext) -> CreateResponse:
        stub = self._get_primary_stub(context)
        if stub is None:
            return CreateResponse()
        resp: CreateResponse = stub.Create(request)
        replicate("Create", request)
        print(f"[service:{NODE_TARGET}] HandleCreate id={resp.item.id}")
        return resp

    def HandleGet(self, request: GetRequest, context: grpc.ServicerContext) -> GetResponse:
        stub = self._get_primary_stub(context)
        if stub is None:
            return GetResponse()
        return stub.Get(request)

    def HandleSearch(self, request: SearchRequest, context: grpc.ServicerContext) -> SearchResponse:
        stub = self._get_primary_stub(context)
        if stub is None:
            return SearchResponse()
        return stub.Search(request)

    def HandleUpdate(self, request: UpdateRequest, context: grpc.ServicerContext) -> UpdateResponse:
        stub = self._get_primary_stub(context)
        if stub is None:
            return UpdateResponse()
        resp: UpdateResponse = stub.Update(request)
        replicate("Update", request)
        print(f"[service:{NODE_TARGET}] HandleUpdate id={request.item_id}")
        return resp

    def HandleStoreBid(self, request: StoreBidRequest, context: grpc.ServicerContext) -> StoreBidResponse:
        stub = self._get_primary_stub(context)
        if stub is None:
            return StoreBidResponse()
        resp: StoreBidResponse = stub.StoreBid(request)
        replicate("StoreBid", request)
        print(f"[service:{NODE_TARGET}] HandleStoreBid item={request.item_id} winning={resp.is_winning_bid}")
        return resp

    def HandleAuction(self, request: ChangeAuctionRequest, context: grpc.ServicerContext) -> ChangeAuctionResponse:
        stub = self._get_primary_stub(context)
        if stub is None:
            return ChangeAuctionResponse()
        return stub.ChangeAuction(request)


def serve() -> None:
    global storage
    storage = StorageRegistry(STORAGE_NODES)

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    print(f"[service:{NODE_TARGET}] heartbeat thread started")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    project3_pb2_grpc.add_ServiceNodeServiceServicer_to_server(ServiceNodeService(), server)
    server.add_insecure_port(f"[::]:{GRPC_SERVER_PORT}")
    server.start()
    print(f"[service:{NODE_TARGET}] listening on port {GRPC_SERVER_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()