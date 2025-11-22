import zmq
import threading
import time
import json
from logger import log

class Agent:
    ELECTION_COOLDOWN = 5     # seconds
    LEADER_TIMEOUT = 8        # seconds

    def __init__(self, agent_id, port, log_queue, broker_host="localhost", check_interval=3):
        self.id = agent_id
        self.port = port
        self.host = "localhost"
        self.check_interval = check_interval
        self.log_queue = log_queue
        self.context = zmq.Context()

        # ROUTER server for peers and clients (system_main)
        self.server = self.context.socket(zmq.ROUTER)
        # Set a short linger period for cleaner shutdown
        self.server.setsockopt(zmq.LINGER, 0) 
        self.server.bind(f"tcp://*:{self.port}")
        self.poller = zmq.Poller()
        self.poller.register(self.server, zmq.POLLIN)

        self.peers = {} # pid -> DEALER socket
        self.leader = None
        self.running = True
        self.in_election = False
        self.broker_host = broker_host

        self.last_election_time = 0
        self.last_leader_pong = time.time()

        # Task control
        self.task_accumulated = 0
        self.task_results_received = 0
        self.expected_results = 0
        # Event to pause election/monitor during a large task distribution
        self.task_running_event = threading.Event() 

        self.ready_for_tasks = False
        self.expected_peer_count = 0

    def log(self, msg):
        log(self.log_queue, f"AGENT {self.id}", msg)

    # ------------------ Broker Interaction ------------------ #
    def register_with_broker(self):
        """Initial registration with the broker."""
        req = self.context.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(f"tcp://{self.broker_host}:5000")
        self.log("Registering with Broker...")
        
        try:
            req.send_json({"action": "REGISTER", "id": self.id, "host": self.host, "port": self.port})
            if req.poll(2000): # Wait up to 2s for registration response
                resp = req.recv_json()
                req.close()
                peers = resp.get("peers", {})
                self.expected_peer_count = len(peers)
                self.update_peers(peers)
                self.log(f"Registration successful. Found {len(peers)} peers.")
            else:
                self.log("Registration failed: Broker unresponsive.")
                req.close()
        except zmq.error.ZMQError as e:
            self.log(f"Registration failed due to ZMQ error: {e}")
            try: req.close()
            except: pass

    def broker_heartbeat(self, req):
            """Sends a heartbeat and synchronously updates peers from the Broker."""
            try:
                # 1. Send Heartbeat to Broker
                heartbeat_msg = {
                    "action": "HEARTBEAT",
                    "id": self.id,
                    "port": self.port,
                    "leader": self.leader,
                    "in_election": self.in_election
                }
                req.send_json(heartbeat_msg)
                
                # 2. Wait for response (contains updated peer list)
                if req.poll(500): # 500ms timeout
                    resp = req.recv_json()
                    if resp.get("status") == "OK" and "peers" in resp:
                        # Update the local peer map based on the Broker's registry
                        self.update_peers(resp["peers"])
                        return True
                    else:
                        self.log(f"Broker heartbeat response error: {resp.get('message')}")
                        return False
                else:
                    self.log("Broker heartbeat timeout (500ms).")
                    return False

            except zmq.error.ZMQError as e:
                self.log(f"ZMQ Error during Broker heartbeat: {e}. Broker may be down.")
                return False
            except Exception as e:
                self.log(f"Unexpected error during Broker heartbeat: {e}")
                return False
            

    def broker_heartbeat_loop(self):
        """Sends periodic heartbeats and updates peer list."""
        req = self.context.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        
        while self.running:
            try:
                # Reconnect if connection is down (e.g., broker restarted)
                try:
                    req.connect(f"tcp://{self.broker_host}:5000") # <-- This should not be inside the loop unless you're always reconnecting
                except zmq.error.ZMQError:
                    time.sleep(1)
                    continue

                # Instead of the complicated send/receive/close/reopen block...
                # ... use the new helper function you already wrote!
                
                # --- CLEANUP: Use the helper for a single iteration ---
                self.broker_heartbeat(req)
                # -----------------------------------------------------

            except zmq.error.ContextTerminated:
                break
            except Exception as e:
                self.log(f"Error in Heartbeat loop: {e}")
            time.sleep(3) # Wait the interval

        req.close()
    # ------------------ Peer Management ------------------ #
    def connect_to(self, pid, host, port):
        """Creates a ZMQ DEALER socket and connects to a peer."""
        if pid in self.peers: return
        sock = self.context.socket(zmq.DEALER)
        sock.setsockopt_string(zmq.IDENTITY, str(self.id))
        sock.setsockopt(zmq.LINGER, 0)
        
        try:
            sock.connect(f"tcp://{host}:{port}")
            self.peers[pid] = sock
            self.send(pid, {"type": "HELLO", "from": self.id, "host": self.host, "port": self.port})
            self.log(f"Connected to peer {pid} on port {port}.")
        except zmq.error.ZMQError as e:
            self.log(f"Failed to connect to peer {pid} at {host}:{port}: {e}")
            sock.close()
            
    def update_peers(self, peers_map):
            """Connects to new peers listed by the broker and removes peers the broker no longer tracks."""
            
            # 1. Collect PIDs that the Broker says are active
            active_pids_from_broker = set()
            for pid_str in peers_map.keys():
                try:
                    active_pids_from_broker.add(int(pid_str))
                except ValueError:
                    self.log(f"Ignoring non-integer peer ID from broker: {pid_str}")
                    
            # 2. Identify and remove agents that are locally known but missing from the Broker's list
            # We iterate over a copy of keys to safely modify the dictionary
            peers_to_remove = [pid for pid in list(self.peers.keys()) if pid not in active_pids_from_broker]
            for pid in peers_to_remove:
                self.log(f"Broker no longer tracking Agent {pid}. Closing local connection and removing.")
                try: 
                    self.peers[pid].close()
                except: 
                    pass
                self.peers.pop(pid, None)
            
            # 3. Add/connect to new peers (using the original logic)
            for pid_str, info in peers_map.items():
                try:
                    pid = int(pid_str)
                    if pid != self.id:
                        self.connect_to(pid, info["host"], info["port"])
                except ValueError:
                    pass # Already logged in step 1 or handled by connect_to

    def send(self, pid, msg):
        """Sends a JSON message to a specific peer."""
        if pid not in self.peers:
            return False

        try:
            self.peers[pid].send_json(msg)
            return True
        except zmq.error.ZMQError as e:
            # Assume peer is dead if send fails
            self.log(f"Error sending to {pid}: {e}. Removing peer.")
            try: self.peers[pid].close()
            except: pass
            self.peers.pop(pid, None)
            return False
        except Exception as e:
            self.log(f"Unexpected send error to {pid}: {e}")
            return False

    # ------------------ Message Handling ------------------ #
    def handle_msg(self, frames):
        """Processes incoming messages from the ROUTER socket."""
        if len(frames) < 2:
            self.log(f"Received malformed message (less than 2 frames).")
            return

        sender_id_bytes = frames[0]
        payload_bytes = frames[1]
        
        try:
            sender_id = sender_id_bytes.decode('utf-8')
            data = json.loads(payload_bytes.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError, Exception) as e:
            self.log(f"Error decoding message from {sender_id_bytes}: {e}")
            return
        
        msg = data
        mtype = msg.get("type")        
        sender = msg.get("from")
        
        # Ensure sender is an integer if it came from another Agent
        if isinstance(sender, str) and sender.isdigit():
            sender = int(sender)

        # Connect to a peer if HELLO is received and we aren't connected
        if sender and sender != "CLIENT" and sender not in self.peers and "host" in msg and "port" in msg:
            self.connect_to(sender, msg["host"], msg["port"])

        # --- TASK LOGIC ---
        if mtype == "START_TASK":
            limit = msg.get("limit", 100)
            if self.leader == self.id:
                # This task came from the external CLIENT (system_main)
                self.distribute_work(limit)
            else:
                self.log(f"Ignoring START_TASK. I am not leader (Leader is {self.leader})")

        elif mtype == "ASSIGN_WORK":
            start_n = msg["start"]
            end_n = msg["end"]
            result = sum(range(start_n, end_n + 1))
            self.log(f"🚧 Worker Task: Sum({start_n}-{end_n}) = {result}. Sending to Leader.")
            # Note: We send to self.leader, which should be the agent that sent ASSIGN_WORK
            self.send(self.leader, {"type": "WORK_RESULT", "from": self.id, "result": result})

        elif mtype == "WORK_RESULT":
            if self.leader != self.id:
                self.log("Received WORK_RESULT but I am not the leader. Ignoring.")
                return

            res = msg["result"]
            self.task_accumulated += res
            self.task_results_received += 1
            self.log(f"📥 Received result {res} from Agent {sender}. Progress: {self.task_results_received}/{self.expected_results}")

            if self.task_results_received >= self.expected_results:
                final_sum = self.task_accumulated
                self.log(f"✅ FINAL SUMMATION RESULT: {final_sum}")
                # Optional: The leader could send this result back to the original client if its ID was known.
                time.sleep(1)
                self.last_leader_pong = time.time()
                self.task_running_event.clear() # Task complete, resume monitoring/election

        # --- ELECTION / P2P LOGIC ---
        elif mtype == "HELLO":
            self.send(sender, {"type": "ACK", "ack_for": "HELLO", "from": self.id})
            if self.leader:
                self.send(sender, {"type": "COORDINATOR", "leader": self.leader, "from": self.id})

        elif mtype == "PING":
            if self.leader == self.id:
                if sender in self.peers:
                    self.send(sender, {"type": "PONG", "from": self.id})
                    self.log(f"➡️ PONG sent to {sender}")

        elif mtype == "PONG":
            self.last_leader_pong = time.time()
            self.log(f"✅ PONG received from Leader {sender}")

        elif mtype == "ELECTION":
            # Respond with ALIVE if higher ID
            if self.id > sender:
                self.send(sender, {"type": "ALIVE", "from": self.id})
                # If not already in election, start one
                if not self.in_election and not self.task_running_event.is_set():
                    self.start_election()
            # If lower ID, ignore ELECTION message

        elif mtype == "ALIVE":
            # Stop current election since a higher ID agent is running its own
            self.in_election = False
            self.log(f"ALIVE received from Agent {sender}. Stopping election.")

        elif mtype == "COORDINATOR":
            # A new leader has been declared
            self.leader = msg["leader"]
            self.in_election = False
            self.last_leader_pong = time.time()
            self.log(f"NEW LEADER: {self.leader}")

            # Notify broker that this agent is ready to receive tasks
            self.ready_for_tasks = True
            req = self.context.socket(zmq.REQ)
            req.setsockopt(zmq.LINGER, 0)
            req.connect(f"tcp://{self.broker_host}:5000")
            req.send_json({"action": "READY", "id": self.id})
            try:
                if req.poll(1000): _ = req.recv_json()
            except: pass
            finally: req.close()

    # ------------------ Task Distribution ------------------ #
    def distribute_work(self, total_limit):
        """Distributes the summation task among itself and active peers."""
        self.task_running_event.set() # Block election/monitoring during task
        self.log(f"STARTING DISTRIBUTED SUMMATION (0 to {total_limit})")
        self.task_accumulated = 0
        self.task_results_received = 0
        
        req = self.context.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(f"tcp://{self.broker_host}:5000")

        self.broker_heartbeat(req) # Use the function you defined

        req.close()

        # Perform a synchronous ping to filter dead peers before chunking
        active_peers = []
        for pid in list(self.peers.keys()): # Iterate over copy
            if self.send(pid, {"type": "PING", "from": self.id}):
                 active_peers.append(pid)
            else:
                 self.log(f"Agent {pid} failed PING check. Excluded from work distribution.")
        
        count = len(active_peers) + 1  # Total participants (Active Peers + Leader)

        chunk_size = total_limit // count
        remainder = total_limit % count

        start = 0
        self.expected_results = 0 

        # 1. Dispatch work to ACTIVE PEERS
        for pid in active_peers:
            current_chunk_size = chunk_size + (1 if remainder > 0 else 0)
            end = start + current_chunk_size - 1
            if remainder > 0: remainder -= 1
            
            work_msg = {
                "type": "ASSIGN_WORK", 
                "from": self.id, 
                "start": start, 
                "end": end
            }
            
            if self.send(pid, work_msg):
                self.log(f"Leader dispatch: Agent {pid} assigned chunk {start}-{end}")
                self.expected_results += 1 # Only count successfully dispatched tasks
            else:
                self.log(f"⚠️ Leader dispatch failed to Agent {pid}. Work not assigned.")
                
            start = end + 1 # Update start for the next chunk

        # 2. Leader performs own chunk (The remaining part)
        if start <= total_limit:
            end = total_limit
            my_result = sum(range(start, end + 1))
            self.task_accumulated += my_result
            self.log(f"Leader Task: Sum({start}-{end}) = {my_result}. Waiting for {self.expected_results} peers...")
            
        else:
            self.log(f"Leader Task: Sum(0-0) = 0. Waiting for {self.expected_results} peers...")
            
        # If expected_results is 0 (i.e., only leader is active), clear the event immediately
        if self.expected_results == 0:
            self.log("Task complete (Only leader was active).")
            self.task_running_event.clear()


    # ------------------ Election ------------------ #
    def start_election(self):
        """Initiates the Bully election algorithm."""
        now = time.time()
        # Cooldown prevents rapid, unstable elections
        if self.in_election or (now - self.last_election_time < self.ELECTION_COOLDOWN) or self.task_running_event.is_set():
            return

        self.last_election_time = now
        self.in_election = True
        self.leader = None
        self.log("⚡ STARTING ELECTION")

        # Only consider currently connected peers with higher ID
        higher_peers = [p for p in self.peers if p > self.id]
        if not higher_peers:
            self.declare_victory()
            return

        # Send ELECTION to higher peers
        for p in higher_peers:
            self.send(p, {"type": "ELECTION", "from": self.id})

        # Wait up to 0.5s for any ALIVE response
        start_wait = time.time()
        while time.time() - start_wait < 0.5:
            # Check for incoming messages on the server socket
            # This is a mini-polling loop within the election to quickly process ALIVE messages
            socks = dict(self.poller.poll(10)) # Poll for 10ms (non-blocking wait)
            if self.server in socks:
                frames = self.server.recv_multipart()
                # Process the message immediately. If it's an ALIVE, 
                # handle_msg will set self.in_election = False
                self.handle_msg(frames) 
            
            if not self.in_election:
                return  # ALIVE received, election cancelled

        # If no ALIVE received and we are still in election mode, declare self leader
        if self.in_election:
            self.declare_victory()

    def declare_victory(self):
        """Declares self as the leader and notifies all peers."""
        self.leader = self.id
        self.in_election = False
        self.last_leader_pong = time.time()
        self.log(f"🏆 DECLARED LEADER {self.id}")
        # Send COORDINATOR message to ALL peers (higher and lower)
        for p in self.peers:
            self.send(p, {"type": "COORDINATOR", "from": self.id, "leader": self.id})
        
        # Self-declare ready status to the broker
        self.ready_for_tasks = True
        req = self.context.socket(zmq.REQ)
        req.setsockopt(zmq.LINGER, 0)
        req.connect(f"tcp://{self.broker_host}:5000")
        req.send_json({"action": "READY", "id": self.id})
        try:
            if req.poll(1000): _ = req.recv_json()
        except: pass
        finally: req.close()


    # ------------------ Leader Monitor ------------------ #
    def leader_monitor_loop(self):
        """Monitors the current leader's health."""
        while self.running:
            # Wait longer if a task is running to avoid unnecessary interruptions
            if self.task_running_event.is_set():
                time.sleep(1)
                continue

            time.sleep(0.5)

            if self.leader and self.leader != self.id:
                leader_id = self.leader

                # 1. Check if the leader is still in our peers list
                if leader_id not in self.peers:
                    self.log(f"Leader {leader_id} connection lost. ⚠️ Starting election immediately.")
                    self.leader = None
                    self.last_leader_pong = 0
                    self.start_election()
                    continue

                # 2. Send PING to the leader
                self.send(leader_id, {"type": "PING", "from": self.id})

                # 3. Check for unresponsiveness
                if time.time() - self.last_leader_pong > 2:  # 2s ping-pong timeout
                    self.log(f"🚨 Leader {leader_id} unresponsive. Starting election.")
                    self.leader = None
                    self.start_election()

            elif not self.leader and not self.in_election:
                # No leader, no election running, must start one
                self.log("No leader detected. Starting election.")
                self.start_election()
        
        self.log("Leader monitor terminated.")


    # ------------------ Run ------------------ #
    def start(self):
        """Initializes the agent, sets up threads, and starts the main loop."""
        self.register_with_broker()

        if not self.peers:
            # No peers detected, immediately declare self as leader
            self.leader = self.id
            self.declare_victory()
        else:
            self.log("Peers detected on startup. Initiating election for coordinated leader selection...")
            self.start_election()

        # Start worker threads
        threading.Thread(target=self.broker_heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.leader_monitor_loop, daemon=True).start()
        
        # Main message processing loop (blocking)
        self.run()

    def run(self):
        """The main message processing loop for the Agent's ZMQ ROUTER socket."""
        self.log("Agent main loop started.")
        while self.running:
            try:
                # Use poller to wait for messages on the server socket
                socks = dict(self.poller.poll(500)) # Poll for 500ms
                
                if self.server in socks:
                    frames = self.server.recv_multipart()
                    self.handle_msg(frames)
                
            except zmq.error.ContextTerminated:
                # Expected during graceful shutdown
                break
            except KeyboardInterrupt:
                self.running = False
                break
            except Exception as e:
                self.log(f"Unexpected error in main run loop: {e}")
                time.sleep(1)

        # Cleanup
        self.server.close()
        self.context.term()
        self.log("Agent shutdown complete.")