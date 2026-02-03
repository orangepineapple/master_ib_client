import zmq
import json
from threading import Lock
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

class MasterClient(EWrapper, EClient):
    def __init__(self, addr, port, client_id, mkt_data_socket):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        
        # Connect to TWS
        self.connect(addr, port, client_id)
        
        self.lock = Lock() 
        
        self.next_id = None
        
        # Mapping: {reqId: "SYMBOL"}
        self.tickers = {}
        
        # ZMQ sockets to respond on
        self.mkt_data_socket = mkt_data_socket
       

    def nextValidId(self, orderId: int):
        self.next_id = orderId
        print(f"Master ready. Next Order ID: {self.next_id}")

   