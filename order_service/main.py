from datetime import time, datetime
import zmq
from ib.order_client import OrderMaster
from trading_util.alert_util import PushNotification  
from trading_util.network import message_pb2 as msg
from config.constants import HOST, CLIENT_NUM
from time import sleep

def start_order_service():
    pn = PushNotification("ORDER SERVICE")
    print("Order Service Started")
    #pn.send_notif("Order Service Started")
    context = zmq.Context()
    order_socket = context.socket(zmq.ROUTER)
    order_socket.bind("tcp://*:5555")
    order_socket.setsockopt(zmq.RCVTIMEO, 10000)
    alerted = False
    client = OrderMaster(HOST, 4002, CLIENT_NUM , order_socket)

    # Try to connect to gateway
    while True:
        if not client.failed_to_connect:
            break
        else:
            if not alerted:
                pn.send_notif(f"@everyone Not Connected, attemping Reconnecting")
                alerted = True
            now = datetime.now().time()
            if now > time(16, 0):
                order_socket.close()
                context.term()
                return
            
            sleep(5)
            client.retry_connection()

    if alerted:
        pn.send_notif(f"@everyone Reconnected")
    
    # Start the Service after connection is confirmed
    client.launch()

    while True:
        now = datetime.now().time()
        if now > time(16, 0): # 4:00 PM
            break

        try:
            sender, empty, order_binary = order_socket.recv_multipart()
            order_msg = msg.TradeOrder()
            order_msg.ParseFromString(order_binary)

            if order_msg.order_type in (msg.OrderType.MKT, msg.OrderType.LMT):
                client.send_order_single_order(
                    order_msg,
                    sender
                )
            elif order_msg.order_type == msg.OrderType.BRKT:
                client.send_bracket_order(
                    order_msg,
                    sender
                )

            elif order_msg.order_type == msg.OrderType.STP:
                client.adjust_stoploss(order_msg.order_id)
            
            elif order_msg.order_type == msg.OrderType.SPCL:
                start_id, end_id = client.get_order_id_slice(int(order_msg.qty))
                print("MAIN",start_id, end_id)
                resp = msg.Ticket(
                    order_id_start=start_id,
                    order_id_end = end_id
                )
                order_socket.send_multipart([sender, b"", resp.SerializeToString()])

        except zmq.Again:
            # It just loops back up to check 'is_market_open' again
            continue

    client.disconnect()
    order_socket.close()
    context.term()

start_order_service()