import zmq
import json
import time 

# Every Agent will have an instance of this Network class to handle communication
class Network:
    def __init__(self, broker_pub_port=6666 , broker_sub_port=7777):
        # Initialzing the Context 
        context = zmq.Context()
        
        # Setting up the Publisher Socket which will send message to the broker sub port
        self.pub = context.socket(zmq.PUB)
        self.pub.connect(f"tcp://localhost:{broker_sub_port}")

        # Agents will recieve messages from the broker pub port using Subscriber Socket
        self.sub =  context.socket(zmq.SUB)
        self.sub.connect(f"tcp://localhost:{broker_pub_port}")
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
        time.sleep(0.5)  # Allow some time for connections to establish

    def send_message(self, msg_dictionary):
        # Converting the dictionary to JSON string before sending
        msg = json.dumps(msg_dictionary)
        # Send it to other agents from the Publisher socket
        self.pub.send_string(msg)

    def receive_message(self):
        # Receive the message from the Subscriber socket (no block means non-blocking)
        try:
            return json.loads(self.sub.recv_string(flags=zmq.NOBLOCK))
        except zmq.Again as e:
            return None