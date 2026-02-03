# Handles mkt data coming from IB

from datetime import time, datetime
import zmq
from config.constants import HOST, CLIENT_NUM
from ib.master_client import MasterClient
import trading_util.network.message_pb2 as msg



def start_order_service():
    context = zmq.Context()
    mkt_data_socket = context.socket(zmq.REP)
    mkt_data_socket.bind("tcp://*:5556")

    # TODO figure out how to use Poller      
    poller = zmq.Poller()
    poller.register(mkt_data_socket, zmq.POLLIN)

    client = MasterClient(HOST, 4002, CLIENT_NUM , mkt_data_socket)

    in_progress_orders = {} # if no one is subscribed anymore cancel this

    while True:
        now = datetime.now().time()
        if now > time(16, 0): # 4:00 PM
            break
        
        



        try:
            binary_data = mkt_data_socket.recv()
            order_msg = msg.TradeOrder()
            order_msg.ParseFromString(binary_data)
        except zmq.Again:
            # This happens every 1 second if no message arrives
            # It just loops back up to check 'is_market_open' again
            continue


    # Shutdown- Save subscriber list to DB

    client.disconnect()
    mkt_data_socket.close()
    context.term()