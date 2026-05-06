from concurrent import futures
import threading
import time
import os

import docker
import grpc
from project3_pb2 import *
import project3_pb2_grpc

CONTROLLER_PORT      = os.environ.get("CONTROLLER_PORT", "50050")
SCALE_UP_THRESHOLD   = int(os.environ.get("SCALE_UP_THRESHOLD", "10"))
SCALE_DOWN_THRESHOLD = int(os.environ.get("SCALE_DOWN_THRESHOLD", "2"))
HEARTBEAT_INTERVAL   = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))
COOLDOWN_SECONDS     = int(os.environ.get("COOLDOWN_SECONDS", "30"))
NETWORK_NAME         = os.environ.get("NETWORK_NAME", "project3_net")
IMAGE_NAME           = os.environ.get("IMAGE_NAME", "project3-image:latest")

SERVICE_NODES_INITIAL = [
    "service-node-1:50060",
    "service-node-2:50061",
]


class ServiceNodeRegistry:
    """
    Thread-safe registry of live service nodes.
    Supports round-robin dispatch, heartbeat-based failure detection,
    and dynamic registration of new nodes spun up via Docker.
    Stubs are created at registration time and reused — including for
    nodes added dynamically by autoscaling.
    """

    def __init__(self, targets: list[str]) -> None:
        self.lock    = threading.Lock()
        self.targets = list(targets)
        self.healthy = set()
        self.rr_idx  = 0
        self.stubs   = {
            t: project3_pb2_grpc.ServiceNodeServiceStub(grpc.insecure_channel(t))
            for t in targets
        }
        for target in targets:
            print(f"[registry] registered {target}")

    def register(self, target: str) -> None:
        with self.lock:
            if target not in self.stubs:
                self.stubs[target] = project3_pb2_grpc.ServiceNodeServiceStub(
                    grpc.insecure_channel(target)
                )
                self.targets.append(target)
                print(f"[registry] registered {target}")

    def mark_healthy(self, target: str) -> None:
        with self.lock:
            self.healthy.add(target)

    def mark_dead(self, target: str) -> None:
        with self.lock:
            if target in self.healthy:
                print(f"[registry] {target} marked dead")
                self.healthy.discard(target)

    def pick_stub(self) -> project3_pb2_grpc.ServiceNodeServiceStub | None:
        """Round-robin over healthy nodes; returns a persistent stub or None."""
        with self.lock:
            healthy_targets = [t for t in self.targets if t in self.healthy]
            if not healthy_targets:
                return None
            target = healthy_targets[self.rr_idx % len(healthy_targets)]
            self.rr_idx += 1
            return self.stubs[target]

    def healthy_count(self) -> int:
        with self.lock:
            return len(self.healthy)

    def all_targets(self) -> list[str]:
        with self.lock:
            return list(self.targets)


registry: ServiceNodeRegistry = None
docker_client: docker.DockerClient = None
inflight      = 0
inflight_lock = threading.Lock()
last_scale    = 0.0


def scale_up() -> None:
    global last_scale
    now = time.time()
    if now - last_scale < COOLDOWN_SECONDS:
        return
    last_scale = now

    existing   = docker_client.containers.list(filters={"name": "service-node-"})
    node_num   = len(existing) + 1
    new_port   = 50060 + node_num - 1
    new_name   = f"service-node-{node_num}"
    new_target = f"{new_name}:{new_port}"

    print(f"[autoscale] scaling up → {new_name} on port {new_port}")
    docker_client.containers.run(
        image=IMAGE_NAME,
        name=new_name,
        hostname=new_name,
        command=["python", "-u", "service/service.py"],
        environment={
            "PYTHONPATH": "/app:/app/proto/src",
            "GRPC_SERVER_PORT": str(new_port),
            "NODE_TARGET": new_target,
        },
        network=NETWORK_NAME,
        working_dir="/app",
        detach=True,
    )

    registry.register(new_target)


def scale_down() -> None:
    global last_scale
    now = time.time()
    if now - last_scale < COOLDOWN_SECONDS:
        return
    if registry.healthy_count() <= 1:
        return
    last_scale = now

    running = sorted(
        docker_client.containers.list(filters={"name": "service-node-"}),
        key=lambda c: c.name,
        reverse=True,
    )
    if not running:
        return

    target_container = running[0]
    print(f"[autoscale] scaling down → stopping {target_container.name}")
    target_container.stop(timeout=5)


def autoscale_loop() -> None:
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with inflight_lock:
            current = inflight

        if current >= SCALE_UP_THRESHOLD:
            scale_up()
        elif current <= SCALE_DOWN_THRESHOLD and registry.healthy_count() > 2:
            scale_down()


def heartbeat_loop() -> None:
    while True:
        for target in registry.all_targets():
            try:
                registry.stubs[target].Heartbeat(HeartbeatRequest(), timeout=2)
                registry.mark_healthy(target)
            except grpc.RpcError:
                registry.mark_dead(target)
        time.sleep(HEARTBEAT_INTERVAL)


class InFlight:
    """Context manager to count in-flight requests."""
    def __enter__(self):
        global inflight
        with inflight_lock:
            inflight += 1

    def __exit__(self, *_):
        global inflight
        with inflight_lock:
            inflight -= 1


class MarketService(project3_pb2_grpc.MarketServiceServicer):

    def _pick_stub(self, context: grpc.ServicerContext) -> project3_pb2_grpc.ServiceNodeServiceStub | None:
        stub = registry.pick_stub()
        if stub is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("No healthy service nodes available")
        return stub

    def CreateItem(self, request: CreateItemRequest, context: grpc.ServicerContext) -> CreateItemResponse:
        with InFlight():
            stub = self._pick_stub(context)
            if stub is None:
                return CreateItemResponse()
            resp: CreateResponse = stub.HandleCreate(CreateRequest(
                seller_id=request.seller_id,
                title=request.title,
                description=request.description,
                category=request.category,
                quantity=request.quantity,
                starting_price=request.starting_price,
            ))
            return CreateItemResponse(item=resp.item)

    def GetItem(self, request: GetItemRequest, context: grpc.ServicerContext) -> GetItemResponse:
        with InFlight():
            stub = self._pick_stub(context)
            if stub is None:
                return GetItemResponse()
            print(f"[controller] GetItem item_id={request.item_id}")
            resp: GetResponse = stub.HandleGet(GetRequest(item_id=request.item_id))
            return GetItemResponse(item=resp.item)

    def SearchItems(self, request: SearchItemsRequest, context: grpc.ServicerContext) -> SearchItemsResponse:
        with InFlight():
            stub = self._pick_stub(context)
            if stub is None:
                return SearchItemsResponse()
            print(f"[controller] SearchItems keyword={request.keyword!r}")
            resp: SearchResponse = stub.HandleSearch(SearchRequest(
                keyword=request.keyword,
                category=request.category,
                status=request.status,
                seller_id=request.seller_id,
                page_size=request.page_size,
                page_token=request.page_token,
            ))
            return SearchItemsResponse(
                items=resp.items,
                next_page_token=resp.next_page_token,
                total_count=resp.total_count,
            )

    def UpdateItem(self, request: UpdateItemRequest, context: grpc.ServicerContext) -> UpdateItemResponse:
        with InFlight():
            stub = self._pick_stub(context)
            if stub is None:
                return UpdateItemResponse()
            print(f"[controller] UpdateItem item_id={request.item_id}")
            resp: UpdateResponse = stub.HandleUpdate(UpdateRequest(
                item_id=request.item_id,
                item=request.item,
            ))
            return UpdateItemResponse(item=resp.item)

    def PlaceBid(self, request: PlaceBidRequest, context: grpc.ServicerContext) -> PlaceBidResponse:
        with InFlight():
            stub = self._pick_stub(context)
            if stub is None:
                return PlaceBidResponse()
            print(f"[controller] PlaceBid item_id={request.item_id} bidder={request.bidder_id}")
            resp: StoreBidResponse = stub.HandleStoreBid(StoreBidRequest(
                item_id=request.item_id,
                bidder_id=request.bidder_id,
                amount=request.amount,
            ))
            return PlaceBidResponse(
                bid=resp.bid,
                updated_item=resp.updated_item,
                is_winning_bid=resp.is_winning_bid,
            )

    def JoinAuction(self, request_iterator, context: grpc.ServicerContext):
        with InFlight():
            stub = self._pick_stub(context)
            if stub is None:
                return
            for client_msg in request_iterator:
                req = ChangeAuctionRequest(item_id=client_msg.item_id)
                if client_msg.HasField("join"):
                    req.join = client_msg.join
                elif client_msg.HasField("bid_amount"):
                    req.bid_amount.CopyFrom(client_msg.bid_amount)
                resp: ChangeAuctionResponse = stub.HandleAuction(req)
                server_msg = AuctionServerMessage(item_id=resp.item_id)
                if resp.HasField("new_bid"):
                    server_msg.new_bid.CopyFrom(resp.new_bid)
                elif resp.HasField("status_update"):
                    server_msg.status_update = resp.status_update
                yield server_msg


def serve() -> None:
    global registry, docker_client

    docker_client = docker.from_env()
    registry      = ServiceNodeRegistry(SERVICE_NODES_INITIAL)

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=autoscale_loop, daemon=True).start()
    print("[controller] heartbeat and autoscale threads started")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    project3_pb2_grpc.add_MarketServiceServicer_to_server(MarketService(), server)
    server.add_insecure_port(f"[::]:{CONTROLLER_PORT}")
    server.start()
    print(f"[controller] listening on port {CONTROLLER_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()