from threading import Thread
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract , ContractDetails
import time
from ibapi.scanner import ScannerSubscription

from config.constants import HOST, CLIENT_NUM

class MarketScreener(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        # Connect to TWS
        self.connect(addr, port, client_id)

        self.data = []
        self.done = False
        self.no_results = True

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        time.sleep(0.5)#give this thread some time to start
    
    def scan_snapshot(self, marketCapMax, marketCapMin, scanCode = "MOST_ACTIVE"):
        '''
        Scans US securities, optional scanCode
        '''
        self.done = False
        self.no_results = False
        
        #Create Scanner Subscription
        my_filter = ScannerSubscription()
        my_filter.numberOfRows = 100
        my_filter.locationCode = "STK.US.MAJOR"
        my_filter.instrument = "STK"
        my_filter.marketCapAbove = marketCapMax #this is in millions
        my_filter.marketCapBelow = marketCapMin 
        my_filter.scanCode = scanCode

        self.reqScannerSubscription(1 , my_filter, [], [])

        while(self.done is False):
            time.sleep(1)
        
        self.cancelScannerSubscription(1)

        return self.data

    def scanner_subscription(self, marketCapMax, marketCapMin, scanCode = "TOP_PERC_GAIN"):
        pass

    ### CALLBACKS BELOW ###
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        contract = contractDetails.contract
        self.data.append(contract.symbol)
        
        print("ticker",contract.symbol,"rank", rank , "distance", distance, "benchmark", benchmark, "projection", projection, "legsStr", legsStr)

    def scannerDataEnd(self, reqId: int):
        self.done = True

    def error(self, reqId: TickerId, errorTime: TickerId, errorCode: TickerId, errorString: str, advancedOrderRejectJson=""):
        if errorCode == 165:
            print("no data")
            self.no_results = True
            self.cancelScannerSubscription(1)

        if advancedOrderRejectJson:
            print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString, "AdvancedOrderRejectJson:", advancedOrderRejectJson)
        else:
            print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)