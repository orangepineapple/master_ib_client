#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import zmq
import trading_util.network.message_pb2 as msg

# 1. ZMQ Setup
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://192.168.x.xx:5555") # Use your Master's IP

# 2. Create the Message Object
order = msg.TradeOrder()
order.symbol = "NVDA"
order.action = msg.OrderSide.BUY
order.qty = 10
order.order_type = msg.DYN_BRKT
order.stop_loss_perc = 10

# 3. Serialize and Send
# .SerializeToString() turns the object into compact binary
binary_payload = order.SerializeToString()
socket.send(binary_payload)

# 4. Wait for confirmation
reply = socket.recv_string()
print(f"Master Client says: {reply}")