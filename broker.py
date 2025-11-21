import zmq
import json

#  PUB port: sends messages to all agents
#  SUB port: receives messages from all agents
   
def broker(pub_port=6666, sub_port=7777):
    context = zmq.Context()

    # PUB socket: agents subscribe here
    pub = context.socket(zmq.PUB)
    pub.bind(f"tcp://*:{pub_port}")

    # SUB socket: agents send messages here
    sub = context.socket(zmq.SUB)
    sub.bind(f"tcp://*:{sub_port}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all messages

    print(f"Broker started: PUB -> {pub_port}, SUB <- {sub_port}")

    while True:
        msg = sub.recv_string()   # blocking recv
        # forward to all agents
        pub.send_string(msg)

if __name__ == "__main__":
    broker()
