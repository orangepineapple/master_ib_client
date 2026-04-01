import zmq
import trading_util.network.message_pb2 as msg
from time import sleep

context = zmq.Context()
order_socket = context.socket(zmq.DEALER)
order_socket.connect("tcp://127.0.0.1:5555") # Use your Master's IP


def getTicket():
    print("TICKET TEST")
    order = msg.TradeOrder()
    order.ticker = "INVALID"
    order.action = msg.OrderSide.SELL
    order.qty = 1
    order.order_type = msg.SPCL
    order.stop_loss = 10
    print("SENDING")
    binary_payload = order.SerializeToString()
    order_socket.send_multipart([b"",binary_payload])

    empty, reply = order_socket.recv_multipart()

    ticket = msg.Ticket()
    ticket.ParseFromString(reply)

    print(f"We can use seq ID's including: [{ticket.order_id_start}, {ticket.order_id_end}]")


getTicket()
order_socket.close()
context.term()
