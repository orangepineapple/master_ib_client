from threading import Lock
from threading import Thread
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from time import sleep
from decimal import Decimal

from trading_util.alert_util import send_notif 
from ibapi.order import Order
from datetime import datetime
from zmq import SyncSocket

import trading_util.network.message_pb2 as msg
from trading_util.network import PROTO_TO_IB_ACTION, PROTO_TO_IB_ORDERTYPE

import logging

logger = logging.getLogger(__name__)

from dataclasses import dataclass, field
from typing import Optional

@dataclass
class OrderInfo:
    # Basic fields with type hints
    sender : bytes 
    ticker: str
    orderType : msg.OrderType.ValueType
    orderSide : msg.OrderSide.ValueType

class OrderMaster(EWrapper, EClient):
    def __init__(self, addr, port, client_id, order_socket : SyncSocket):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        
        # Connect to TWS
        self.connect(addr, port, client_id)
        
        self.lock = Lock() 
        
        self.current_order_id = None
        
        # ZMQ sockets to respond on
        self.order_socket = order_socket

        # Utility + Connectivity (Shared via Threads)
        self.failed_to_connect = False
        self.server_error = False
        self.ticker_unavailable = set()

        # Data Collection
        self.snapshot_data = {} # not shared
        self.data_arrived = []

        # Order Placement (Shared via Threads)
        self.order_information : dict[int, OrderInfo] = {}

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        sleep(0.5) # give this thread some time to start

        # Get Next Valid ID on startup
        self.reqIds(1)


    def send_order_single_order(self, order_msg : msg.TradeOrder, sender : bytes):
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
        contract.symbol = order_msg.ticker
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"

        order = Order()
        order.action = PROTO_TO_IB_ACTION[order_msg.action]
        order.totalQuantity = Decimal(order_msg.qty)
        order.orderType = PROTO_TO_IB_ORDERTYPE[order_msg.order_type]
        order.transmit = True
        order.orderId = self.current_order_id
        # Increment after sending Orders
        self.current_order_id += 1
        print("symnbol:", contract.symbol, order.action, order.orderType)

        # Save Sender to respond to
        # Concurrent Access of order information
        with self.lock:
            self.order_information[order.orderId] = OrderInfo(
                sender,
                order_msg.ticker,
                order_msg.order_type,
                order_msg.action,  
            )

        self.placeOrder(order.orderId, contract, order)
        print("Order sent", order_msg.ticker, "buy", order_msg.qty)


    def send_bracket_order(self, order_msg : msg.TradeOrder, sender : bytes):
        # Sends a bracket order
        pass

    def adjust_stoploss(self, order_id):
        # Adjusts the stoploss of provided order id
        pass


    ### BUILT IN CALLBACKS
    def orderStatus(self, orderId: int, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        # when remaining is 0, then avgFillPrice contains the price we actaully finished the order add
        print(status)
        if status == "Open":
            pass

        if remaining == 0 and status == "Filled":  # gets called with "Filled" status multiple times
            if orderId in self.order_information:
                print("Executed", self.order_information[orderId].ticker, "amount: ", filled, "filled at :", avgFillPrice)

                with self.lock:
                    order_info = self.order_information.pop(orderId)
                    
                resp = msg.TradeUpdate(
                    order_id=orderId,
                    ticker=order_info.ticker,
                    status=msg.OrderStatus.FILLED,
                    order_type=order_info.orderType,
                    action=order_info.orderSide,
                    total_qty=int(filled),
                    filled_qty=int(filled),
                    remaining_qty=int(remaining),
                    avg_fill_price=avgFillPrice,
                    last_fill_price=lastFillPrice,
                    timestamp=datetime.now().isoformat()
                )

                self.order_socket.send_multipart([order_info.sender, b"", resp.SerializeToString()])

    
    def nextValidId(self, orderId: int):
        '''
        Gets the orderIds and places them into a list, list is cleared after orders are send
        '''
        self.current_order_id = orderId  
    
    ### ERROR HANDLING ###
    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        # client not connected error
        if errorCode == 504: 
            if not self.failed_to_connect:

                # UNEXPECTED RUNNING ERRORS ALSO HAPPEN HERE
                send_notif("@everyone ORDER MASTER ERROR: " + errorString)
            with self.lock:     
                self.failed_to_connect = True
        # contract not found
        elif errorCode == 201: 
            # Order Rejected
            with self.lock:
                order_info = self.order_information.pop(reqId)

            resp = msg.TradeUpdate(
                order_id=reqId,
                ticker=order_info.ticker,
                status=msg.OrderStatus.ERROR,
                order_type=order_info.orderType,
                action=order_info.orderSide,
                total_qty=0,
                filled_qty=0,
                remaining_qty=0,
                avg_fill_price=0,
                last_fill_price=0,
                timestamp=datetime.now().isoformat(),
                error_message=errorString
            )

            self.order_socket.send_multipart([order_info.sender, b"", resp.SerializeToString()])

        # Server Error
        elif errorCode == 321:
            if not self.server_error:
                send_notif("@everyone server error:"+ errorString) 
            with self.lock:
                self.server_error = True
        else:
            print(errorCode, errorString)