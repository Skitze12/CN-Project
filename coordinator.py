from logger import log
import threading
import time

# Coordinator handles election logic for the Agent
# Bully algorithm: agent with highest ID becomes leader
# Communication via the network module
class Coordinator:
    # Network-wide flag to ensure only one election starts
    network_election_started = False  

    def __init__(self, agent):
        self.agent = agent
        self.election_in_progress = False
        self.election_lock = threading.Lock()
        self.recieved_ok = False
        self.leader_declared = False

    # Start election if none in progress in the network
    def start_election(self):
        with self.election_lock:
            if self.election_in_progress or Coordinator.network_election_started:
                return  # Election already in progress anywhere

            self.election_in_progress = True
            Coordinator.network_election_started = True  # set network-wide flag
            self.recieved_ok = False
            self.leader_declared = False

        log(self.agent.id, "Starting election...")

        # Send ELECTION messages to all agents with higher IDs
        for other_agent_id in range(self.agent.id + 1, self.agent.total_agents):
            self.agent.network.send_message({
                "type": "ELECTION",
                "from": self.agent.id,
                "to": other_agent_id
            })

        # Wait for OK responses
        def election_timeout():
            time.sleep(3)  # Wait for OKs
            with self.election_lock:
                if not self.recieved_ok:
                    # No OK received: I am the leader
                    self.declare_leader()
                self.election_in_progress = False

        threading.Thread(target=election_timeout, daemon=True).start()

    # Handle incoming ELECTION message
    def handle_election(self, other_agent_id):
        if other_agent_id < self.agent.id:
            # I am stronger — respond OK
            self.agent.network.send_message({
                "type": "OK",
                "from": self.agent.id,
                "to": other_agent_id
            })
            # Start my own election if none in progress
            self.start_election()

    # Handle incoming OK message
    def handle_ok(self):
        with self.election_lock:
            self.recieved_ok = True
            log(self.agent.id, "Received OK — waiting for leader announcement.")

    # Declare self as leader and broadcast to network
    def declare_leader(self):
        if self.leader_declared:
            return  # Already declared
        if self.agent.id == max(range(self.agent.total_agents)):
            log(self.agent.id, "I am the leader now!")
            self.agent.leader = self.agent.id
            self.leader_declared = True
            Coordinator.network_election_started = False  # Reset network flag
            self.agent.network.send_message({
                "type": "LEADER",
                "from": self.agent.id
            })

    # Handle incoming LEADER announcement
    def handle_leader(self, leader_id):
        self.agent.leader = leader_id
        log(self.agent.id, f"New leader elected: Agent {leader_id}")
        with self.election_lock:
            self.election_in_progress = False
            self.leader_declared = (self.agent.id == leader_id)
            Coordinator.network_election_started = False
