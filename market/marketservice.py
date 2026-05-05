import grpc
import project3_pb2
import project3_pb2_grpc
import os

CONTROLLER_HOST = os.environ.get("CONTROLLER_HOST", "host.docker.internal")
CONTROLLER_PORT = os.environ.get("CONTROLLER_PORT", 50052)
CONTROLLER_TARGET = f"{CONTROLLER_HOST}:{CONTROLLER_PORT}"


def create_item(seller_id, title, description, category, quantity, starting_price_amount, currency="USD"):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        stub = project3_pb2_grpc.ControllerServiceStub(channel)

        response = stub.Create(project3_pb2.CreateRequest(
            seller_id=seller_id,
            title=title,
            description=description,
            category=category,
            quantity=quantity,
            starting_price=project3_pb2.Money(
                currency_code=currency,
                amount_small=starting_price_amount,
            ),
        ))

        # print(
        #     f"create: id={response.item.id} "
        #     f"title={response.item.title} "
        #     f"status={response.item.status} "
        #     f"version={response.item.version}"
        # )

        return response.item


def get_item(item_id):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        stub = project3_pb2_grpc.ControllerServiceStub(channel)

        response = stub.Get(project3_pb2.GetRequest(item_id=item_id))

        # print(
        #     f"get: id={response.item.id} "
        #     f"title={response.item.title} "
        #     f"current_price={response.item.current_price.amount_small} "
        #     f"version={response.item.version}"
        # )

        return response.item


def search_items(keyword="", category="", seller_id="", page_size=20):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        stub = project3_pb2_grpc.ControllerServiceStub(channel)

        response = stub.Search(project3_pb2.SearchRequest(
            keyword=keyword,
            category=category,
            seller_id=seller_id,
            page_size=page_size,
        ))

        # print(f"search: keyword={keyword!r} total_count={response.total_count}")

        # for i, item in enumerate(response.items, start=1):
        #     print(
        #         f"  result {i}: id={item.id} "
        #         f"title={item.title} "
        #         f"price={item.current_price.amount_small}"
        #     )

        return response.items


def update_item(item_id, title="", description="", quantity=0):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        stub = project3_pb2_grpc.ControllerServiceStub(channel)

        response = stub.Update(project3_pb2.UpdateRequest(
            item_id=item_id,
            item=project3_pb2.Item(
                title=title,
                description=description,
                quantity=quantity,
            ),
        ))

        # print(
        #     f"update: id={response.item.id} "
        #     f"title={response.item.title} "
        #     f"version={response.item.version}"
        # )

        return response.item


def place_bid(item_id, bidder_id, amount_small, currency="USD"):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        stub = project3_pb2_grpc.ControllerServiceStub(channel)

        response = stub.StoreBid(project3_pb2.StoreBidRequest(
            item_id=item_id,
            bidder_id=bidder_id,
            amount=project3_pb2.Money(
                currency_code=currency,
                amount_small=amount_small,
            ),
        ))

        # print(
        #     f"bid: bid_id={response.bid.bid_id} "
        #     f"item_id={item_id} "
        #     f"bidder={bidder_id} "
        #     f"amount={response.bid.amount.amount_small} "
        #     f"is_winning={response.is_winning_bid}"
        # )

        return response


def join_auction(item_id, bidder_id, bid_amounts):
    """
    Join an auction stream, listen for server events, and submit bids.
    bid_amounts is a list of integer cent-amounts to bid sequentially.
    """

    def client_messages():
        # First message: join the auction room
        yield project3_pb2.AuctionClientMessage(
            item_id=item_id,
            join=True,
        )
        # Subsequent messages: place bids
        for amount in bid_amounts:
            yield project3_pb2.AuctionClientMessage(
                item_id=item_id,
                bid_amount=project3_pb2.Money(currency_code="USD", amount_small=amount),
            )

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:

        stub = project3_pb2_grpc.MarketServiceStub(channel)

        # for i, server_msg in enumerate(stub.JoinAuction(client_messages()), start=1):
        #     if server_msg.HasField("new_bid"):
        #         print(
        #             f"auction event {i}: new_bid "
        #             f"bidder={server_msg.new_bid.bidder_id} "
        #             f"amount={server_msg.new_bid.amount.amount_small}"
        #         )
        #     elif server_msg.HasField("status_update"):
        #         print(f"auction event {i}: status={server_msg.status_update}")


def main():

    # Create items
    item1 = create_item(
        seller_id="seller-001",
        title="Vintage Camera",
        description="A classic film camera in great condition.",
        category="Electronics",
        quantity=1,
        starting_price_amount=5000,  # $50.00 in cents
    )

    item2 = create_item(
        seller_id="seller-002",
        title="Leather Jacket",
        description="Genuine leather, size M.",
        category="Clothing",
        quantity=3,
        starting_price_amount=12000,  # $120.00 in cents
    )

    # Fetch one back
    get_item(item1.id)

    # Search by category
    search_items(category="Electronics")

    # Update metadata
    update_item(item1.id, description="A classic film camera — recently serviced.", quantity=1)

    # Place bids
    place_bid(item_id=item1.id, bidder_id="bidder-A", amount_small=5500)
    place_bid(item_id=item1.id, bidder_id="bidder-B", amount_small=6000)
    place_bid(item_id=item1.id, bidder_id="bidder-A", amount_small=6500)

    # Join auction stream (uncomment once controller supports streaming)
    # join_auction(item_id=item1.id, bidder_id="bidder-C", bid_amounts=[7000, 7500])


if __name__ == "__main__":
    main()