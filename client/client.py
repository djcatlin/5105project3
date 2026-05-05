import grpc
import project3_pb2 as pb
import project3_pb2_grpc as pb_grpc
import os

CONTROLLER_HOST   = os.environ.get("CONTROLLER_HOST", "host.docker.internal")
CONTROLLER_PORT   = os.environ.get("CONTROLLER_PORT", "50050")
CONTROLLER_TARGET = f"{CONTROLLER_HOST}:{CONTROLLER_PORT}"


def create_item(seller_id, title, description, category, quantity, price_cents, currency="USD"):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = pb_grpc.MarketServiceStub(channel)
        response = stub.CreateItem(pb.CreateItemRequest(
            seller_id=seller_id,
            title=title,
            description=description,
            category=category,
            quantity=quantity,
            starting_price=pb.Money(currency_code=currency, amount_small=price_cents),
        ))
        print(
            f"create: id={response.item.id} "
            f"title={response.item.title} "
            f"price={response.item.current_price.amount_small} "
            f"version={response.item.version}"
        )
        return response.item


def get_item(item_id):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = pb_grpc.MarketServiceStub(channel)
        response = stub.GetItem(pb.GetItemRequest(item_id=item_id))
        print(
            f"get: id={response.item.id} "
            f"title={response.item.title} "
            f"price={response.item.current_price.amount_small} "
            f"version={response.item.version}"
        )
        return response.item


def search_items(keyword="", category="", page_size=20):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = pb_grpc.MarketServiceStub(channel)
        response = stub.SearchItems(pb.SearchItemsRequest(
            keyword=keyword,
            category=category,
            page_size=page_size,
        ))
        print(f"search: keyword={keyword!r} total={response.total_count}")
        for i, item in enumerate(response.items, start=1):
            print(
                f"  {i}: id={item.id} "
                f"title={item.title} "
                f"price={item.current_price.amount_small}"
            )
        return response.items


def update_item(item_id, description="", quantity=0):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = pb_grpc.MarketServiceStub(channel)
        response = stub.UpdateItem(pb.UpdateItemRequest(
            item_id=item_id,
            item=pb.Item(description=description, quantity=quantity),
        ))
        print(
            f"update: id={response.item.id} "
            f"version={response.item.version}"
        )
        return response.item


def place_bid(item_id, bidder_id, amount_cents, currency="USD"):

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = pb_grpc.MarketServiceStub(channel)
        response = stub.PlaceBid(pb.PlaceBidRequest(
            item_id=item_id,
            bidder_id=bidder_id,
            amount=pb.Money(currency_code=currency, amount_small=amount_cents),
        ))
        print(
            f"bid: bid_id={response.bid.bid_id} "
            f"amount={response.bid.amount.amount_small} "
            f"is_winning={response.is_winning_bid}"
        )
        return response


def join_auction(item_id, bid_amounts):

    def messages():
        yield pb.AuctionClientMessage(item_id=item_id, join=True)
        for amount in bid_amounts:
            yield pb.AuctionClientMessage(
                item_id=item_id,
                bid_amount=pb.Money(currency_code="USD", amount_small=amount),
            )

    with grpc.insecure_channel(CONTROLLER_TARGET) as channel:
        stub = pb_grpc.MarketServiceStub(channel)
        for i, msg in enumerate(stub.JoinAuction(messages()), start=1):
            if msg.HasField("new_bid"):
                print(
                    f"auction {i}: new_bid "
                    f"bidder={msg.new_bid.bidder_id} "
                    f"amount={msg.new_bid.amount.amount_small}"
                )
            elif msg.HasField("status_update"):
                print(f"auction {i}: status={msg.status_update}")


def main():

    item = create_item(
        seller_id="seller-001",
        title="Vintage Camera",
        description="Classic film camera, excellent condition.",
        category="Electronics",
        quantity=1,
        price_cents=5000,
    )

    get_item(item.id)

    search_items(category="Electronics")

    update_item(item.id, description="Recently serviced.", quantity=1)

    place_bid(item_id=item.id, bidder_id="bidder-A", amount_cents=5500)
    place_bid(item_id=item.id, bidder_id="bidder-B", amount_cents=6000)
    place_bid(item_id=item.id, bidder_id="bidder-A", amount_cents=6500)

    join_auction(item_id=item.id, bid_amounts=[7000, 7500])


if __name__ == "__main__":
    main()