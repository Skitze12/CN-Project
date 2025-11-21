import time
import threading
from logger import log
from coordinator import Coordinator
from network import Network

class Agent:
    def __init__(self, id, total_agents=5):
        self.id = id
        self.state = "alive"
        self.network = Network(broker_pub_port=6666, broker_sub_port=7777)
        self.leader = None
        self.coord = Coordinator(self)
        self.total_agents = total_agents
        
    # I was having the problem where even before all the other agents started, one would be calling out start election
    # So i decided to implement a simple handshake and a timeout functionality 

        # Tracking handshakes
        # Agents that have sent ready
        self.ready_agents = set()
        self.ready_agents.add(self.id)  # Mark self as ready
        self.acks_received = {self.id: set()} # Tracking who acknowledged my ready

        # Timeout Configuration
        self.ack_timeout = 2  # seconds
        self.last_ready_sent = 0


    #------Handshaking methods------#
    def send_ready(self):
        # Send READY message to all agents
        self.network.send_message({
            "type": "READY",
            "from": self.id
        })
        self.last_ready_sent = time.time()

        # Ensure we track our own READ

        if(self.id not in self.ready_agents):
            self.ready_agents[self.id] = set() 
            log(self.id, f"Marked self as READY")

    def wait_for_all_ready(self):
         log(self.id, "Sending initial READY signal")
         self.send_ready()

         while True:
            # We will check if everyone is ready and has acknowledged my ready signal
            all_ready = len(self.ready_agents) == self.total_agents
            all_acked = len(self.acks_received[self.id]) == self.total_agents
            if( all_ready and all_acked):
                break

            # If not all ready, resend READY if timeout exceeded
            current_time = time.time()
            if current_time - self.last_ready_sent > self.ack_timeout:
                log(self.id, "Resending READY signal due to timeout")
                self.send_ready()
            time.sleep(0.2)  # avoid busy-waiting

         log(self.id, "All agents ready — starting election")


    #------Message Handling methods------#
    def handle_message(self, msg):
        # Because msg will be in dictionary format
        sender = msg["from"]
        mtype = msg["type"]

        if mtype == "ELECTION":
            self.coord.handle_election(msg["from"])

        elif mtype == "OK":
            log(self.id, "Received OK — waiting for leader announcement.")

        elif mtype == "LEADER":
            self.leader = msg["from"]
            log(self.id, f"New leader elected: Agent {self.leader}")
        
        elif mtype == "READY":
            if sender not in self.ready_agents:
                self.ready_agents.add(sender)
                log(self.id, f"Received READY from Agent {sender}")
            # Send ACK_Ready back to sender

            self.network.send_message({
                "type": "ACK_READY",
                "from": self.id,
                "to": sender
            })

        elif mtype == "ACK_READY":
            # Only process ACK_READY meant for me
            if msg.get("to") == self.id:
                if sender not in self.acks_received[self.id]:
                    self.acks_received[self.id].add(sender)
                    log(self.id, f"Received ACK_READY from Agent {sender}")



    #------Heartbeat methods------#
    def heartbeat(self):

        # If I am the leader, send heartbeat messages periodically to all agents to say that I am alive
        while True:
            time.sleep(2)
            if self.leader == self.id:
                self.network.send_message({
                    "type": "HEARTBEAT",
                    "from": self.id
                })


    #------Main Run Loop------#
    def listener(self):
        # Continuously listen for incoming messages
        while True:
            msg = self.network.receive_message()
            if msg:
                self.handle_message(msg)


    def run(self):
        log(self.id, "Agent started.")

        # Start listener thread
        threading.Thread(target=self.listener, daemon=True).start()

        # Wait for all agents to be ready
        self.wait_for_all_ready()

        # Begin election if no leader known
        if self.id == self.total_agents - 1:
            log(self.id, "I am the highest ID — starting election")
            self.coord.start_election()
        else:
            log(self.id, "Waiting for leader announcement...")


        # Heartbeat thread
        threading.Thread(target=self.heartbeat, daemon=True).start()

        # Keep alive
        while True:
            time.sleep(1)
