from concurrent import futures
import threading
import time
import os

import docker
import grpc
import project3_pb2 as pb
import project3_pb2_grpc as pb_grpc

CONTROLLER_PORT    = os.environ.get("CONTROLLER_PORT", "50050")
SCALE_UP_THRESHOLD = int(os.environ.get("SCALE_UP_THRESHOLD", "10"))   # requests in flight
SCALE_DOWN_THRESHOLD = int(os.environ.get("SCALE_DOWN_THRESHOLD", "2"))
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))
COOLDOWN_SECONDS   = int(os.environ.get("COOLDOWN_SECONDS", "30"))
NETWORK_NAME       = os.environ.get("NETWORK_NAME", "project3_net")
IMAGE_NAME         = os.environ.get("IMAGE_NAME", "project3-image:latest")

SERVICE_NODES_INITIAL = [
    "service-node-1:50060",
    "service-node-2:50061",
]


class ServiceNodeRegistry:
    """
    Thread-safe registry of live service nodes.
    Supports round-robin dispatch, heartbeat-based failure detection,
    and dynamic registration of new nodes spun up via Docker.
    """

    def __init__(self, targets: list[str]) -> None:
        self._lock     = threading.Lock()
        self._nodes    = {}
        self._targets  = []
        self._rr_idx   = 0

        for target in targets:
            self._register(target)

    def _register(self, target: str) -> None:
        """Add a node entry (must hold lock or call from __init__)."""
        channel = grpc.insecure_channel(target)
        stub    = pb_grpc.ServiceNodeServiceStub(channel)
        self._nodes[target]  = {"healthy": False, "stub": stub}
        self._targets.append(target)
        print(f"[registry] registered {target}")

    def register(self, target: str) -> None:
        with self._lock:
            if target not in self._nodes:
                self._register(target)

    def mark_healthy(self, target: str) -> None:
        with self._lock:
            if target in self._nodes:
                self._nodes[target]["healthy"] = True

    def mark_dead(self, target: str) -> None:
        with self._lock:
            if target in self._nodes and self._nodes[target]["healthy"]:
                print(f"[registry] {target} marked dead")
                self._nodes[target]["healthy"] = False

    def pick(self) -> pb_grpc.ServiceNodeServiceStub | None:
        """Round-robin over healthy nodes."""
        with self._lock:
            healthy = [t for t in self._targets if self._nodes[t]["healthy"]]
            if not healthy:
                return None
            target = healthy[self._rr_idx % len(healthy)]
            self._rr_idx += 1
            return self._nodes[target]["stub"]

    def healthy_count(self) -> int:
        with self._lock:
            return sum(1 for info in self._nodes.values() if info["healthy"])

    def all_targets(self) -> list[str]:
        with self._lock:
            return list(self._targets)


# ── Globals ────────────────────────────────────────────────────────────────────

registry: ServiceNodeRegistry = None
docker_client: docker.DockerClient = None
_inflight      = 0
_inflight_lock = threading.Lock()
_last_scale    = 0.0


# ── Autoscaling ────────────────────────────────────────────────────────────────

def scale_up() -> None:
    global _last_scale
    now = time.time()
    if now - _last_scale < COOLDOWN_SECONDS:
        return
    _last_scale = now

    existing = docker_client.containers.list(filters={"name": "service-node-"})
    node_num  = len(existing) + 1
    new_port  = 50060 + node_num - 1
    new_name  = f"service-node-{node_num}"
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
    global _last_scale
    now = time.time()
    if now - _last_scale < COOLDOWN_SECONDS:
        return
    if registry.healthy_count() <= 1:
        return   # always keep at least one node
    _last_scale = now

    # Remove the highest-numbered service node
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
        with _inflight_lock:
            current = _inflight

        if current >= SCALE_UP_THRESHOLD:
            scale_up()
        elif current <= SCALE_DOWN_THRESHOLD and registry.healthy_count() > 2:
            scale_down()


# ── Heartbeat ──────────────────────────────────────────────────────────────────

def heartbeat_loop() -> None:
    while True:
        for target in registry.all_targets():
            try:
                stub = pb_grpc.ServiceNodeServiceStub(grpc.insecure_channel(target))
                stub.Heartbeat(pb.HeartbeatRequest(), timeout=2)
                registry.mark_healthy(target)
            except grpc.RpcError:
                registry.mark_dead(target)
        time.sleep(HEARTBEAT_INTERVAL)


# ── Request tracking helpers ───────────────────────────────────────────────────

class _track:
    """Context manager to count in-flight requests."""
    def __enter__(self):
        global _inflight
        with _inflight_lock:
            _inflight += 1

    def __exit__(self, *_):
        global _inflight
        with _inflight_lock:
            _inflight -= 1


# ── MarketService ──────────────────────────────────────────────────────────────

class MarketService(pb_grpc.MarketServiceServicer):

    def _stub(self, context: grpc.ServicerContext):
        stub = registry.pick()
        if stub is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("No healthy service nodes available")
        return stub

    def CreateItem(self, request: pb.CreateItemRequest, context: grpc.ServicerContext) -> pb.CreateItemResponse:
        with _track():
            stub = self._stub(context)
            if stub is None:
                return pb.CreateItemResponse()
            print(f"[controller] CreateItem title={request.title}")
            resp = stub.HandleCreate(pb.CreateRequest(
                seller_id=request.seller_id,
                title=request.title,
                description=request.description,
                category=request.category,
                quantity=request.quantity,
                starting_price=request.starting_price,
            ))
            return pb.CreateItemResponse(item=resp.item)

    def GetItem(self, request: pb.GetItemRequest, context: grpc.ServicerContext) -> pb.GetItemResponse:
        with _track():
            stub = self._stub(context)
            if stub is None:
                return pb.GetItemResponse()
            print(f"[controller] GetItem item_id={request.item_id}")
            resp = stub.HandleGet(pb.GetRequest(item_id=request.item_id))
            return pb.GetItemResponse(item=resp.item)

    def SearchItems(self, request: pb.SearchItemsRequest, context: grpc.ServicerContext) -> pb.SearchItemsResponse:
        with _track():
            stub = self._stub(context)
            if stub is None:
                return pb.SearchItemsResponse()
            print(f"[controller] SearchItems keyword={request.keyword!r}")
            resp = stub.HandleSearch(pb.SearchRequest(
                keyword=request.keyword,
                category=request.category,
                status=request.status,
                seller_id=request.seller_id,
                page_size=request.page_size,
                page_token=request.page_token,
            ))
            return pb.SearchItemsResponse(
                items=resp.items,
                next_page_token=resp.next_page_token,
                total_count=resp.total_count,
            )

    def UpdateItem(self, request: pb.UpdateItemRequest, context: grpc.ServicerContext) -> pb.UpdateItemResponse:
        with _track():
            stub = self._stub(context)
            if stub is None:
                return pb.UpdateItemResponse()
            print(f"[controller] UpdateItem item_id={request.item_id}")
            resp = stub.HandleUpdate(pb.UpdateRequest(
                item_id=request.item_id,
                item=request.item,
            ))
            return pb.UpdateItemResponse(item=resp.item)

    def PlaceBid(self, request: pb.PlaceBidRequest, context: grpc.ServicerContext) -> pb.PlaceBidResponse:
        with _track():
            stub = self._stub(context)
            if stub is None:
                return pb.PlaceBidResponse()
            print(f"[controller] PlaceBid item_id={request.item_id} bidder={request.bidder_id}")
            resp = stub.HandleStoreBid(pb.StoreBidRequest(
                item_id=request.item_id,
                bidder_id=request.bidder_id,
                amount=request.amount,
            ))
            return pb.PlaceBidResponse(
                bid=resp.bid,
                updated_item=resp.updated_item,
                is_winning_bid=resp.is_winning_bid,
            )

    def JoinAuction(self, request_iterator, context: grpc.ServicerContext):
        with _track():
            stub = self._stub(context)
            if stub is None:
                return
            for client_msg in request_iterator:
                req = pb.ChangeAuctionRequest(item_id=client_msg.item_id)
                if client_msg.HasField("join"):
                    req.join = client_msg.join
                elif client_msg.HasField("bid_amount"):
                    req.bid_amount.CopyFrom(client_msg.bid_amount)
                resp = stub.HandleAuction(req)
                server_msg = pb.AuctionServerMessage(item_id=resp.item_id)
                if resp.HasField("new_bid"):
                    server_msg.new_bid.CopyFrom(resp.new_bid)
                elif resp.HasField("status_update"):
                    server_msg.status_update = resp.status_update
                yield server_msg


# ── Serve ──────────────────────────────────────────────────────────────────────

def serve() -> None:
    global registry, docker_client

    docker_client = docker.from_env()
    registry      = ServiceNodeRegistry(SERVICE_NODES_INITIAL)

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=autoscale_loop, daemon=True).start()
    print("[controller] heartbeat and autoscale threads started")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_MarketServiceServicer_to_server(MarketService(), server)
    server.add_insecure_port(f"[::]:{CONTROLLER_PORT}")
    server.start()
    print(f"[controller] listening on port {CONTROLLER_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()