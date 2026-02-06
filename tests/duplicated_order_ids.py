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
from order_service.ib.order_client import OrderMaster

client = OrderMaster("192.168.2.60", 4002, 1 , None)
client2 = OrderMaster("192.168.2.60", 4002, 2 , None)

contract = Contract()
contract.symbol = "TSLA"
contract.secType = "STK"
contract.currency = "USD"
contract.exchange = "SMART"

order = Order()
order.action = 'BUY'
order.totalQuantity = Decimal(1)
order.orderType = 'MKT'
order.transmit = True
order.orderId = 1



print("TWO CLIENT DIFF ORDER IDS")
client.placeOrder(order.orderId, contract, order)
client2.placeOrder(order.orderId, contract, order)

# WILL GIV
# E103 Duplicate order id



sleep(10)

client.disconnect()
client2.disconnect()