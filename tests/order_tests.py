import zmq
import trading_util.network.message_pb2 as msg
from time import sleep

context = zmq.Context()
order_socket = context.socket(zmq.DEALER)
order_socket.connect("tcp://127.0.0.1:5555") # Use your Master's IP


def InvalidOrderTest():
    order = msg.TradeOrder()
    order.ticker = "INVALID"
    order.action = msg.OrderSide.SELL
    order.qty = 10
    order.order_type = msg.BRKT
    order.stop_loss = 10

    binary_payload = order.SerializeToString()
    order_socket.send(binary_payload)

    reply = order_socket.recv()
    print(f"Master Client says: {reply}")

def OutsideOfMarketHours():
    '''
    Run this only outsidde of market hours
    '''
    order = msg.TradeOrder()
    order.ticker = "NVDA"
    order.action = msg.OrderSide.SELL
    order.qty = 10
    order.order_type = msg.BRKT
    order.stop_loss = 10

    binary_payload = order.SerializeToString()
    order_socket.send(binary_payload)

    reply = order_socket.recv()
    print(f"Master Client says: {reply}")

def TestRegularOrder():
    order = msg.TradeOrder()
    order.ticker = "NVDA"
    order.action = msg.OrderSide.SELL
    order.qty = 10
    order.order_type = msg.BRKT
    order.stop_loss = 10

    binary_payload = order.SerializeToString()
    order_socket.send(binary_payload)

    reply = order_socket.recv()
    print(f"Master Client says: {reply}")