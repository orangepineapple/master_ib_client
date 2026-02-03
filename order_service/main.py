# Handles all orders going to IB


from datetime import time, datetime
import zmq
from config.constants import HOST, CLIENT_NUM
from order_service.ib.master_order_client import OrderMaster
import trading_util.network.message_pb2 as msg
from trading_util.network.conversion import PROTO_TO_IB_TYPE


def start_order_service():
    context = zmq.Context()
    order_socket = context.socket(zmq.REP)
    order_socket.bind("tcp://*:5555")

    # TODO figure out how to use Poller      
    poller = zmq.Poller()
    poller.register(order_socket, zmq.POLLIN)

    client = OrderMaster(HOST, 4002, CLIENT_NUM , order_socket)

    in_progress_orders = {} # if no one is subscribed anymore cancel this

    while True:
        now = datetime.now().time()
        if now > time(16, 0): # 4:00 PM
            break

        try:
            binary_data = order_socket.recv()
            order_msg = msg.TradeOrder()
            order_msg.ParseFromString(binary_data)

            if order_msg.order_type == msg.OrderType.MKT or msg.OrderType.LMT:
                client.send_order_single_order(
                    order_msg.symbol,
                    order_msg.qty,
                    order_msg.client_id,
                    PROTO_TO_IB_TYPE[order_msg.order_type],
                    PROTO_TO_IB_TYPE.get(order_msg.action, "MKT"),
                )
            elif order_msg.order_type == msg.OrderType.DYN_BRKT:
                # TODO IMPL
                client.send_braket_order()

            elif order_msg.order_type == msg.OrderType.STP:
                # TODO IMPL
                client.adjust_stoploss(order_msg.order_id)

        except zmq.Again:
            # This happens every 1 second if no message arrives
            # It just loops back up to check 'is_market_open' again
            continue
        
    client.disconnect()
    order_socket.close()
    context.term()