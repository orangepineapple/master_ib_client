import zmq
from trading_util.network import message_pb2 as msg
from trading_util.network.conversion import PROTO_TO_IB_TYPE


context = zmq.Context()
order_socket = context.socket(zmq.REP)
order_socket.bind("tcp://*:5555")


while True:
    binary_data = order_socket.recv()
    binary_data2 = order_socket.recv()
    binary_data3 = order_socket.recv()
    order_msg = msg.TradeOrder()
    order_msg.ParseFromString(binary_data)
    print(order_msg.ticker)
    print(order_msg.action)

    if order_msg.action == msg.OrderSide.SELL:
        order_socket.send_string("XD")
    else:
        order_socket.send_string("POP")




    
