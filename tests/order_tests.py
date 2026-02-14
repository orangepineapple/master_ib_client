import zmq
import trading_util.network.message_pb2 as msg
from time import sleep

context = zmq.Context()
order_socket = context.socket(zmq.DEALER)
order_socket.connect("tcp://127.0.0.1:5555") # Use your Master's IP
