# Handles mkt data coming from IB
import zmq
from config.constants import HOST, CLIENT_NUM
from ib.data_client import DataMaster

def start_market_data_service():
    context = zmq.Context()
    new_subscription_socket = context.socket(zmq.ROUTER) #TODO USE DEALER ROUTER
    new_subscription_socket.bind("tcp://*:5556")
    new_subscription_socket.setsockopt(zmq.RCVTIMEO, 10000)

    mkt_data_socket = context.socket(zmq.PUB)
    mkt_data_socket.bind("tcp://*:5557")
    
    client = DataMaster(HOST, 4002, CLIENT_NUM , mkt_data_socket, new_subscription_socket)

    client.subscribe_to_market_data()
    # Clients are in charge of re-subscribing the next day

    client.disconnect()
    mkt_data_socket.close()
    new_subscription_socket.close()
    context.term()