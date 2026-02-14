from datetime import time, datetime
import zmq
from order_service.ib.order_client import OrderMaster
from trading_util.network import message_pb2 as msg
from trading_util.network.conversion import PROTO_TO_IB_TYPE

def start_order_service(host, client_num):
    context = zmq.Context()
    order_socket = context.socket(zmq.ROUTER)
    order_socket.bind("tcp://*:5555")
    order_socket.setsockopt(zmq.RCVTIMEO, 10000)

    client = OrderMaster(host, 4002, client_num , order_socket)

    in_progress_orders = {} # if no one is subscribed anymore cancel this

    while True:
        now = datetime.now().time()
        if now > time(16, 0): # 4:00 PM
            break

        try:
            sender, empty, order_binary = order_socket.recv_multipart()
            order_msg = msg.TradeOrder()
            order_msg.ParseFromString(order_binary)

            if order_msg.order_type == msg.OrderType.MKT or msg.OrderType.LMT:
                client.send_order_single_order(
                    order_msg.ticker,
                    order_msg.qty,
                    PROTO_TO_IB_TYPE[order_msg.order_type],
                    PROTO_TO_IB_TYPE.get(order_msg.action, "MKT"),
                    sender
                )


            elif order_msg.order_type == msg.OrderType.BRKT:
                client.send_bracket_order(
                    order_msg.ticker,
                    order_msg.qty,
                    PROTO_TO_IB_TYPE[order_msg.order_type],
                    sender
                )

            elif order_msg.order_type == msg.OrderType.STP:
                client.adjust_stoploss(order_msg.order_id)

        except zmq.Again:
            # It just loops back up to check 'is_market_open' again
            continue

    client.disconnect()
    order_socket.close()
    context.term()