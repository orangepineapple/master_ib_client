import zmq
import trading_util.network.message_pb2 as msg

# 1. ZMQ Setup
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:5555") # Use your Master's IP

print("CLIENT 1 SETUP")

# 2. Create the Message Object

counter = 20

while True:
    if counter == 0:
        break
    order = msg.TradeOrder()
    order.ticker = "NVDA"
    order.action = msg.OrderSide.BUY
    order.qty = 10
    order.order_type = msg.BRKT
    order.stop_loss = 10

    # 3. Serialize and Send
    # .SerializeToString() turns the object into compact binary
    binary_payload = order.SerializeToString()
    print(binary_payload)
    socket.send(binary_payload)

    # 4. Wait for confirmation
    reply = socket.recv_string()
    print(f"Master Client says: {reply}")

    counter -=1
