from threading import Lock
from threading import Thread
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from time import sleep
from decimal import Decimal

from trading_util.alert_util import send_notif 
from trading_util.order_util import OrderLog
from ibapi.order import Order
from datetime import datetime
from zmq import SyncSocket

import logging

logger = logging.getLogger(__name__)


class OrderMaster(EWrapper, EClient):
    def __init__(self, addr, port, client_id, order_socket : SyncSocket):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        
        # Connect to TWS
        self.connect(addr, port, client_id)
        
        self.lock = Lock() 
        
        self.next_id = None
        
        # Mapping: {reqId: "SYMBOL"}
        self.tickers = {}
        
        # ZMQ sockets to respond on
        self.order_socket = order_socket

        self.watchlist = []

        # Utility + Connectivity (Shared via Threads)
        self.failed_to_connect = False
        self.server_error = False
        self.ticker_unavailable = set()

        # Data Collection
        self.snapshot_data = {} # not shared
        self.data_arrived = []

        # Order Placement (Shared via Threads)
        self.order_ids = []
        self.current_order_id = None
        self.order_information : dict[int, OrderLog] = {}
        self.failed_orders = []
        self.filled_orders = set()

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        sleep(0.5) # give this thread some time to start

        # Get Next Valid ID on startup
        self.reqIds(1)


    def send_order_single_order(self, ticker : str, quantity : int, sent_by : int, action : str, orderType : str):
        '''
        Sends a single order- BUY/SELL, MARKET/LIMIT
        '''       

        while True: 
            with self.lock:
                if self.current_order_id is not None:
                    break
            sleep(0.5)

        ## CONCURRENT EXCECUTION on orderStatus
        
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"

        order = Order()
        order.action = action
        order.totalQuantity = Decimal(quantity)
        order.orderType = orderType
        order.transmit = True
        order.orderId = self.current_order_id
        # Increment after sending Orders
        self.current_order_id += 1


        # Concurrent Access of order information
        with self.lock:
            self.order_information[order.orderId] = OrderLog(
                quantity,
                ticker,
                "buy",
            )

        self.placeOrder(order.orderId, contract, order)
        print("Order sent", ticker, "buy", quantity)


    def send_braket_order(self):
        # Sends a bracket order
        pass

    def adjust_stoploss(self, order_id):
        # Adjusts the stoploss of provided order id
        pass
        

    

    ### BUILT IN CALLBACKS
    def nextValidId(self, orderId: int):
        '''
        Gets the orderIds and places them into a list, list is cleared after orders are send
        '''
        self.current_order_id = orderId
    
    def orderStatus(self, orderId: int, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        # when remaining is 0, then avgFillPrice contains the price we actaully finished the order add
        if remaining == 0 and status == "Filled": 
            with self.lock:
                #TODO remove from order table - trackings ticker, and who sent it
                pass
            
            #TODO order oject and send status
            self.order_socket.send("TEMP- SEND ORDER STATUS")


            print("Executed", self.order_information[orderId].ticker, "amount: ", self.order_information[orderId].quantity , "filled at :", avgFillPrice)
        
    ### ERROR HANDLING ###
    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        # client not connected error
        if errorCode == 504: 
            if not self.failed_to_connect:
                send_notif("@everyone gateway is not connected, please restart manually")
                logger.error("Client failed to connect")
            with self.lock:     
                self.failed_to_connect = True
        # Market Data might not be available to some of the sercurities
        elif errorCode == 10089: 
            with self.lock:           
                self.ticker_unavailable.add(self.watchlist[reqId])
                self.data_arrived[reqId] = 2
        # contract not found
        elif errorCode == 200: 
            print("contract not found")
            with self.lock:
                self.failed_orders.append(reqId)
        # Server Error
        elif errorCode == 321:
            if not self.server_error:
                send_notif("@everyone server error:"+ errorString) 
                logger.error("Client in read only mode")
            with self.lock:
                self.server_error = True
        else:
            print(errorCode, errorString)