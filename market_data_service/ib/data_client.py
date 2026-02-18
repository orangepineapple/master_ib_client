import zmq
from threading import Lock
from threading import Thread
from ibapi.client import EClient
from time import sleep
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from trading_util.network import message_pb2 as msg
from datetime import time, datetime
from trading_util.alert_util import send_notif 

class DataMaster(EWrapper, EClient):
    def __init__(self, addr, port, client_id, mkt_data_socket : zmq.SyncSocket, subscription_socket : zmq.SyncSocket):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        
        # Connect to TWS
        self.connect(addr, port, client_id)
        
        self.lock = Lock() 
        
        self.next_id = None
        
        self.REQID_TO_TICKER = {}
        self.TICKER_TO_REQ_ID = {}
        self.REQID_TO_SENDER = {}
        self.subscription_count : dict[int, set[bytes]] = {}  #TODO, do not add duplicate counts for same sender
        self.requests_to_ack = set()

        self.req_to_ack = set()

        # ZMQ Socket for broadcasting Market Data
        self.mkt_data_socket = mkt_data_socket
        self.subscription_socket = subscription_socket

        self.market_req_id = 1

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        sleep(0.5) # give this thread some time to start
    

    def subscribe_to_market_data(self):
        while True:
            now = datetime.now().time()
            if now > time(16, 0): # 4:00 PM
                return
            try:
                sender, empty, market_sub_binary = self.subscription_socket.recv_multipart()
                mkt_data_sub = msg.SubscriptionReq()
                mkt_data_sub.ParseFromString(market_sub_binary)

                if mkt_data_sub.action == msg.SubscriptionAction.SUBSCRIBE:
                    print("SUBSCRIPTION RECV")
                    if mkt_data_sub.ticker in self.TICKER_TO_REQ_ID:
                        print("SUBSCRIPTION exists")
                        self.subscription_count[self.TICKER_TO_REQ_ID[mkt_data_sub.ticker]].add(sender)
                        continue

                    stock = Contract()
                    stock.symbol = mkt_data_sub.ticker
                    stock.secType = 'STK'
                    stock.exchange = 'SMART'
                    stock.currency = 'USD'   
                    
                    self.reqMktData(self.market_req_id, stock, '', False, False, []) 
                    print("MKR DATA REQUESTED")

                    with self.lock:
                        self.requests_to_ack.add(self.market_req_id)
                        self.REQID_TO_SENDER[self.market_req_id] = sender
                        self.REQID_TO_TICKER[self.market_req_id] = mkt_data_sub.ticker
                        self.TICKER_TO_REQ_ID[mkt_data_sub.ticker] = self.market_req_id
                        self.subscription_count[self.market_req_id] = {sender}
                        self.market_req_id += 1

                else: # Canceling a sub
                    reqId = self.TICKER_TO_REQ_ID[mkt_data_sub.ticker]
                    self.subscription_count[reqId].discard(sender)
                    if len(self.subscription_count[reqId]) == 0:
                        self.cancelMktData(reqId)

            except zmq.Again:
                # This happens every 10 seconds if no message arrives
                # It just loops back up to check 'is_market_open' again
                continue
       

    def tickPrice(self, reqId, tickType, price, attrib):
        '''
        Callback for market data subscription, only used for ingestion, we process based on ticks recevied
        ''' 
        if tickType == 4:   # only check for last price      
            tick = msg.Tick(ticker=self.REQID_TO_TICKER[reqId], price=price)
            # SEND DATA OFF
            print(tick.price)
            # print(tick.ticker)
            # print(self.REQID_TO_TICKER[reqId])
            self.mkt_data_socket.send_multipart([tick.ticker.encode('utf-8'), tick.SerializeToString()])
            #print("SENT")

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        # client not connected error
        if errorCode == 504: 
            if not self.failed_to_connect:
                send_notif("@everyone gateway is not connected, please restart manually")
            with self.lock:     
                self.failed_to_connect = True
        # Market Data might not be available to some of the sercurities
        elif errorCode == 10089 or errorCode == 200: 
            # Roll back the data storing subscriptions
            self.subscription_socket.send_multipart([self.REQID_TO_SENDER[reqId], b"", msg.SubscriptionAck(result=msg.FAILED).SerializeToString()])

            with self.lock:
                del self.subscription_count[reqId] 
                del self.TICKER_TO_REQ_ID[self.REQID_TO_TICKER[reqId]]
                del self.REQID_TO_TICKER[reqId]
                del self.REQID_TO_SENDER[reqId]
                self.requests_to_ack.discard(reqId) # Possible this is very delayed more than 1 s, in which case an ACK will be followed by a NACK
                # no need to rollback the id count - since we don't know how many ids after the failed one have been created
        # Server Error
        elif errorCode == 321:
            if not self.server_error:
                send_notif("@everyone server error:"+ errorString) 
            with self.lock:
                self.server_error = True
        else:
            print(errorCode, errorString)      