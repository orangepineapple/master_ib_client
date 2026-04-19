from decimal import Decimal

from ibapi.client import EClient
from ibapi.common import TickAttribBidAsk, TickerId
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from threading import Lock, Thread
from trading_util.alert_util import PushNotification
from time import sleep
import time as python_time
import zmq
from trading_util import message_pb2 as msg
from ibapi.client import TickAttribLast
from dataclasses import dataclass
from typing import Optional
from collections import deque
# Collect all the data we need, and build order flow
# Will give a good solid foundational understanding of how it works


# ─────────────────────────────────────────────
# Trade Classification
# ─────────────────────────────────────────────
@dataclass
class QuoteState:
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_size: Optional[Decimal] = None
    ask_size: Optional[Decimal] = None
    ts: Optional[int] = None

class Orderflow(EWrapper, EClient):
    '''
    Used to request any historical stock data from IB
    '''
    def __init__(self, addr, port, client_id, input_socket : zmq.SyncSocket, trade_socket : zmq.SyncSocket):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        # Connect to TWS
        self.connect(addr, port, client_id)

        self.lock = Lock()

        self.failed_to_connect = False

        self.pn = PushNotification("HISTORICAL DATA")

        self.input_socket = input_socket
        self.trade_socket = trade_socket

        self.allLast_reqId_to_ticker = {}
        self.quote_reqId_to_ticker : dict[int, QuoteState] = {}

        # Maybe Buffer in future if slow
        # self.buffer : deque[msg.TradeTick] = deque()
        # self.batch_size = 50

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        sleep(0.5) # give this thread some time to start

    def start_data(self):
        contract = Contract()
        contract.symbol = "MNQ"
        contract.secType = "FUT"
        contract.exchange = "CME"
        contract.currency = "USD"

        self.reqTickByTickData(1, contract, "AllLast", 0, True)
        self.reqTickByTickData(2, contract, "BidAsk", 0 , True)
        #self.reqMktDepth(2, contract, 10, True, [])

    def tickByTickAllLast(self, reqId, tickType, time, price, size, tickAttribLast : TickAttribLast, exchange, specialConditions):

        with self.lock:
            quote = self.quote_reqId_to_ticker[reqId]
            side  = classify_side(price, quote)

        self.trade_socket.send_multipart([])
        tick = msg.TradeTick(
            symbol            = self.allLast_reqId_to_ticker[reqId],
            ts                = time,
            ts_received       = python_time.time(),
            price             = price,
            size              = int(size),
            bid_price         = quote.bid,
            ask_price         = quote.ask,
            exchange          = exchange,
            pastLimit         = bool(tickAttribLast.pastLimit),
            side              = side,
            unreported        = bool(tickAttribLast.unreported),
            special_conditions = specialConditions,
        )

        print(tick)

        self.trade_socket.send_multipart([tick.symbol.encode('utf-8'), tick.SerializeToString()])
        print("TICK BY TICK ALL LAST")

    def tickByTickBidAsk(self, reqId: TickerId, time: TickerId, bidPrice: float, askPrice: float, bidSize: Decimal, askSize: Decimal, tickAttribBidAsk: TickAttribBidAsk):

        with self.lock:
            quote = self.quote_reqId_to_ticker[reqId]
            quote.bid      = bidPrice
            quote.ask      = askPrice
            quote.bid_size = bidSize
            quote.ask_size = askSize
            quote.ts       = time

    def error(self, reqId: TickerId, errorTime: TickerId, errorCode: TickerId, errorString: str, advancedOrderRejectJson=""):
        print(errorCode, errorString)
        # client not connected error
        if errorCode == 504 or errorCode == 502: 
            if not self.failed_to_connect:
                # UNEXPECTED RUNNING ERRORS ALSO HAPPEN HERE
                # self.pn.send_notif("@everyone ORDER MASTER ERROR: " + errorString)
                with self.lock:     
                    self.failed_to_connect = True
        # contract not found
        elif errorCode == 201: 
            # Order Rejected
            pass
        # Server Error
        elif errorCode == 321:
            if not self.server_error:
                self.pn.send_notif("@everyone server error:"+ errorString) 
            with self.lock:
                self.server_error = True
        elif errorCode in (2104, 2107, 2158):
            print(errorCode, errorString)
        else:
            self.pn.send_notif(str(errorCode) + " " + errorString)


# ─────────────────────────────────────────────
# Trade Classification
# ─────────────────────────────────────────────

def classify_side(price: float, quote: QuoteState) -> msg.OrderSide:
    if quote.bid is None or quote.ask is None:
        return msg.UNKNOWN

    if price >= quote.ask:
        return msg.OrderSide.BUY
    elif price <= quote.bid:
        return msg.OrderSide.SELL
    elif price > (quote.bid + quote.ask) / 2:
        return msg.OrderSide.BUY
    elif price < (quote.bid + quote.ask) / 2:
        return msg.OrderSide.SELL
    else:
        return msg.UNKNOWN