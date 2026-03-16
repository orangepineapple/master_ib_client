# Handles mkt data coming from IB
import zmq
from config.constants import HOST, CLIENT_NUM
from ib.data_client import DataMaster
from time import sleep 
from datetime import time, datetime


def start_market_data_service():
    print("MARKET DATA SERVICE RUNNING")

    context = zmq.Context()
    new_subscription_socket = context.socket(zmq.ROUTER)
    new_subscription_socket.bind("tcp://*:5556")
    new_subscription_socket.setsockopt(zmq.RCVTIMEO, 10000)

    mkt_data_socket = context.socket(zmq.PUB)
    mkt_data_socket.bind("tcp://*:5557")
    
    # Continue to try to connect until end of day- so we don't have to restart the whole container if IB was not running
    while True:
        client = DataMaster(HOST, 4002, CLIENT_NUM , mkt_data_socket, new_subscription_socket)
        # Clients are in charge of re-subscribing the next day
        if not client.failed_to_connect:
            break
        else:
            now = datetime.now().time()
            if now > time(16, 0):
                mkt_data_socket.close()
                new_subscription_socket.close()
                context.term()
                return
            sleep(10)

    client.subscribe_to_market_data()
    client.disconnect()
    
    mkt_data_socket.close()
    new_subscription_socket.close()
    context.term()