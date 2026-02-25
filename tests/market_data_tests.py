import zmq
import trading_util.network.message_pb2 as msg
from time import sleep

context = zmq.Context()
market_data_sub = context.socket(zmq.DEALER)
market_data_sub.connect("tcp://127.0.0.1:5556") # Use your Master's IP

market_data_recv = context.socket(zmq.SUB)
market_data_recv.connect("tcp://127.0.0.1:5557")
market_data_recv.setsockopt(zmq.SUBSCRIBE, b"AAPL")

def TestRegularRequest():
   
    subscription = msg.SubscriptionReq(
        ticker="AAPL",
        action=msg.SUBSCRIBE
    )

    print("STARTED")
    # 3. Serialize and Send
    # .SerializeToString() turns the object into compact binary
    binary_payload = subscription.SerializeToString()
    market_data_sub.send_multipart([b"",binary_payload])
    print("SEND SUB")

    # # 4. Wait for confirmation
    # print("WAITING FOR REPLY")
    # empty , reply = market_data_sub.recv_multipart()
    # ack = msg.SubscriptionAck()
    # ack.ParseFromString(reply)
    # print(f"Master Client says: {ack}")

    counter = 0
    while True:
        # Receive messages
        print("RECEIVING MARKET DATA")
        ticker, payload = market_data_recv.recv_multipart()
        tick = msg.Tick()
        tick.ParseFromString(payload)
        
        print(f"Received message: {tick.price} ticker : {tick.ticker}")
        counter += 1
        if counter > 20:
            break
    
    # End the subscription
    subscription = msg.SubscriptionReq(
        ticker="AAPL",
        action=msg.CANCEL
    )

    binary_payload = subscription.SerializeToString()
    market_data_sub.send_multipart([b"",binary_payload])
    print("CANCEL AAPL")

    sleep(5)

    try:
        ticker, payload = market_data_recv.recv_multipart()
        print(payload)
    except zmq.Again:
        # This happens every 10 seconds if no message arrives
        # It just loops back up to check 'is_market_open' again    
        print("DONE")
        
TestRegularRequest()

def TestInvalidTIcker():
    subscription = msg.SubscriptionReq(
        ticker="DEEZ_NUTS_CO",
        action=msg.SUBSCRIBE
    )

    print("STARTED")
    
    binary_payload = subscription.SerializeToString()
    market_data_sub.send_multipart([b"",binary_payload])
    print("SEND SUB")

    print("WAITING FOR REPLY")
    empty , reply = market_data_sub.recv_multipart()
    ack = msg.SubscriptionAck()
    ack.ParseFromString(reply)
    print(f"Master Client says: {ack}")
