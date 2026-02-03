from decimal import Decimal
from threading import Thread
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from time import sleep
from trading_util.alert_util import send_notif 
from trading_util.order_util import OrderLog

from ibapi.order import Order
from datetime import datetime
from threading import Lock

import logging

logger = logging.getLogger(__name__)

class BuyRebound(EWrapper, EClient):
    '''
    Used to request any historical stock data from IB
    '''
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        # Connect to TWS
        self.connect(addr, port, client_id)

        # Data shared between main and callback threads need to be locked
        self.lock = Lock() 

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
        self.initial_order_id = None
        self.order_information : dict[int, OrderLog] = {}
        self.failed_orders = []
        self.filled_orders = set()

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        sleep(0.5) # give this thread some time to start

    def wait_for_data(self):
        '''
        Returns True if Timeout, False if otherwise
        '''
        def data_not_arrived():
            with self.lock:
                for i in self.data_arrived:
                    if i < 2:
                        return True
                return False
    
        def error_check():
            with self.lock:
                return not (self.server_error or self.failed_to_connect)

        time_out_counter = 0
        while data_not_arrived() and error_check():
            time_out_counter += 1
            sleep(1)
            if time_out_counter >= 10:
                logger.error("Time out on waiting for data from IB")
                return
    
    def get_snapshot(self, watchlist) -> dict[str,dict[str, int]]:
        '''
        watchlist: list of string (ticker symbols)
        ''' 
        self.watchlist = watchlist
        self.data_arrived = [0 for x in range(len(watchlist))]

        
        for ticker in watchlist:
            self.snapshot_data[ticker] = {} 

        ## CONCURRENT EXECUTION AFTER HERE
        for i in range(len(watchlist)):
            stock = Contract()
            stock.symbol = watchlist[i]
            stock.secType = 'STK'
            stock.exchange = 'SMART'
            stock.currency = 'USD'   
    
            # Request snapshot for each ticker -> Not sure how many is max
            self.reqMktData(i, stock, '', True, False, [])

        # Wait for Data 
        self.wait_for_data()

        return self.snapshot_data       

    def send_market_orders(self, tickers : list[str], quantity : list[int]) -> list[OrderLog]:
        '''
        Sends market orders for each ticker inputted into the list
        '''
        logger.info("Start send market orders")
        # Get initial order id (only unique per client, so dont worry about clients polluting eachother)
        ## Concurrent execution of nextValidId
        self.reqIds(1)

        while True: 
            with self.lock:
                if self.initial_order_id is not None:
                    break
            sleep(0.5)

        ## END Concurrent execution of nextValidId
        for i in range(len(tickers)):
            self.order_ids.append(self.initial_order_id)
            self.initial_order_id += 1

        ## CONCURRENT EXCECUTION on orderStatus
        for i , ticker in enumerate(tickers):
            contract = Contract()
            contract.symbol = ticker
            contract.secType = "STK"
            contract.currency = "USD"
            contract.exchange = "SMART"

            order = Order()
            order.action = 'BUY'
            order.totalQuantity = Decimal(quantity[i])
            order.orderType = 'MKT'
            order.transmit = True
            order.orderId = self.order_ids[i]


            # Concurrent Access of order information
            with self.lock:
                self.order_information[order.orderId] = OrderLog(
                    quantity[i],
                    ticker,
                    "buy",
                )

            self.placeOrder(order.orderId, contract, order)
            print("Order sent", ticker, "buy", quantity[i])
        
        # Lock the read here
        while True:
            with self.lock:
                if len(self.filled_orders) >= len(self.order_ids):
                    break
            sleep(0.5)
        
        # Reset Counters
        with self.lock:
            self.order_ids = []
            self.filled_order_count = 0

        return list(self.order_information.values())
    
    ### BUILT IN CALLBACKS
    def nextValidId(self, orderId: int):
        '''
        Gets the orderIds and places them into a list, list is cleared after orders are send
        '''
        self.initial_order_id = orderId
    
    def orderStatus(self, orderId: int, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        # when remaining is 0, then avgFillPrice contains the price we actaully finished the order add
        if remaining == 0 and status == "Filled": 
            with self.lock:
                self.order_information[orderId].filled_at(avgFillPrice)
                self.order_information[orderId].executed_at(datetime.now())
                self.filled_orders.add(orderId) # the same order calls filled status more than once, collect only unique fills                
            
            print("Executed", self.order_information[orderId].ticker, "amount: ", self.order_information[orderId].quantity , "filled at :", avgFillPrice)

    def tickPrice(self, reqId, tickType, price, attrib):
        # only check for last price
        if tickType == 4:
            with self.lock:
                self.data_arrived[reqId] += 1
                self.snapshot_data[self.watchlist[reqId]]["price"] = price

    def tickSize(self, reqId: int, tickType: int, size: Decimal):
        if tickType == 8:
            with self.lock:
                self.data_arrived[reqId] += 1
                self.snapshot_data[self.watchlist[reqId]]["vol"] = size

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