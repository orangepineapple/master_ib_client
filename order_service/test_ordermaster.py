from ib.order_client import OrderMaster
from config.constants import HOST, CLIENT_NUM
import zmq
from time import sleep


context = zmq.Context()
order_socket = context.socket(zmq.ROUTER)
order_socket.bind("tcp://*:5555")
order_socket.setsockopt(zmq.RCVTIMEO, 10000)
client = OrderMaster(HOST, 4002, CLIENT_NUM , order_socket)
client.launch()

sleep(5)

print(client.failed_to_connect)