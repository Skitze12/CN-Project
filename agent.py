import zmq
import threading
import time
import json
from logger import log

class Agent:
    ELECTION_COOLDOWN = 5   # seconds
    LEADER_TIMEOUT = 8      # seconds

    def __init__(self, agent_id, port, log_queue, broker_host="localhost", check_interval=3):
        self.id = agent_id
        self.port = port
        self.host = "localhost"
        self.check_interval = check_interval
        self.log_queue = log_queue
        self.context = zmq.Context()

        # ROUTER server for peers
        self.server = self.context.socket(zmq.ROUTER)
        self.server.bind(f"tcp://*:{self.port}")
        self.poller = zmq.Poller()
        self.poller.register(self.server, zmq.POLLIN)

        self.peers = {}
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
        self.task_running_event = threading.Event()  # Pause election/monitor during task

        self.ready_for_tasks = False
        self.expected_peer_count = 0

    def log(self, msg):
        log(self.log_queue, f"AGENT {self.id}", msg)

    # ------------------ Broker Interaction ------------------ #
    def register_with_broker(self):
        req = self.context.socket(zmq.REQ)
        req.connect(f"tcp://{self.broker_host}:5000")
        req.send_json({"action": "REGISTER", "id": self.id, "host": self.host, "port": self.port})
        resp = req.recv_json()
        req.close()
        peers = resp.get("peers", {})
        self.expected_peer_count = len(peers)
        self.update_peers(peers)

    def broker_heartbeat_loop(self):
        req = self.context.socket(zmq.REQ)
        req.connect(f"tcp://{self.broker_host}:5000")
        while self.running:
            try:
                req.send_json({"action": "HEARTBEAT", "id": self.id})
                if req.poll(2000):
                    resp = req.recv_json()
                    if "peers" in resp:
                        self.update_peers(resp["peers"])
                else:
                    req.close()
                    req = self.context.socket(zmq.REQ)
                    req.connect(f"tcp://{self.broker_host}:5000")
            except:
                pass
            time.sleep(3)

    # ------------------ Peer Management ------------------ #
    def connect_to(self, pid, host, port):
        if pid in self.peers: return
        sock = self.context.socket(zmq.DEALER)
        sock.setsockopt_string(zmq.IDENTITY, str(self.id))
        sock.connect(f"tcp://{host}:{port}")
        self.peers[pid] = sock
        self.send(pid, {"type": "HELLO", "from": self.id, "host": self.host, "port": self.port})

    def update_peers(self, peers_map):
        for pid_str, info in peers_map.items():
            pid = int(pid_str)
            if pid != self.id:
                self.connect_to(pid, info["host"], info["port"])

    def send(self, pid, msg):
        if pid not in self.peers:
            return False

        try:
            self.peers[pid].send_json(msg)
            return True
        except zmq.error.ZMQError as e:
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
        if len(frames) < 2:
            self.log(f"Received malformed message (less than 2 frames). Frames: {frames}")
            return

        sender_id = frames[0].decode('utf-8')
        
        try:
            # Decode the actual JSON payload from the second frame
            data = json.loads(frames[1].decode('utf-8'))
            
        except json.JSONDecodeError as e:
            self.log(f"JSON Decode Error from Agent {sender_id}: {e}. Raw data: {frames[1]}")
            return
        except Exception as e:
            self.log(f"Unexpected Error during message decode: {e}")
            return

        # --- Start of the actual message processing ---
        
        msg = data
        mtype = msg.get("type")        
        sender = msg.get("from")
        
        if isinstance(sender, str) and sender.isdigit():
            sender = int(sender)

        if sender and sender != "CLIENT" and sender not in self.peers and "host" in msg and "port" in msg:
            self.connect_to(sender, msg["host"], msg["port"])

        # --- TASK LOGIC ---
        if mtype == "START_TASK":
            limit = msg.get("limit", 100)
            if self.leader == self.id:
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
            res = msg["result"]
            self.task_accumulated += res
            self.task_results_received += 1
            self.log(f"📥 Received result {res} from Agent {sender}. Progress: {self.task_results_received}/{self.expected_results}")

            if self.task_results_received >= self.expected_results:
                self.log(f"✅ FINAL SUMMATION RESULT: {self.task_accumulated}")
                time.sleep(1)
                self.last_leader_pong = time.time()
                self.task_running_event.clear()

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
            if self.id > sender:
                self.send(sender, {"type": "ALIVE", "from": self.id})
                if not self.in_election and not self.task_running_event.is_set():
                    self.start_election()

        elif mtype == "ALIVE":
            self.in_election = False

        elif mtype == "COORDINATOR":
            self.leader = msg["leader"]
            self.in_election = False
            self.last_leader_pong = time.time()
            self.log(f"NEW LEADER: {self.leader}")

            self.ready_for_tasks = True
            req = self.context.socket(zmq.REQ)
            req.connect(f"tcp://{self.broker_host}:5000")
            req.send_json({"action": "READY", "id": self.id})
            try:
                if req.poll(1000): _ = req.recv_json()
            except: pass
            finally: req.close()

    # ------------------ Task Distribution ------------------ #
    def distribute_work(self, total_limit):
        self.task_running_event.set()
        self.log(f"STARTING DISTRIBUTED SUMMATION (0 to {total_limit})")
        self.task_accumulated = 0
        self.task_results_received = 0

        peers_list = list(self.peers.keys())
        
        # 1. First pass: try to ping all peers to clean out the dead ones
        # This is a synchronous check that helps clear out dead peers before chunking
        active_peers = []
        for pid in peers_list:
            if self.send(pid, {"type": "PING", "from": self.id}):
                 active_peers.append(pid)
            else:
                 # If send fails, the peer is removed from self.peers by the send() function
                 self.log(f"Agent {pid} failed PING check. Excluded from work distribution.")
        
        # Use the newly clean list for calculation
        count = len(active_peers) + 1  # Total participants (Active Peers + Leader)

        chunk_size = total_limit // count
        remainder = total_limit % count

        start = 0
        self.expected_results = 0 # Start at 0 and increment for successful dispatches

        # 2. Dispatch work to ACTIVE PEERS
        for pid in active_peers:
            # Distribute remainder across the first 'remainder' agents
            current_chunk_size = chunk_size + (1 if remainder > 0 else 0)
            end = start + current_chunk_size - 1
            if remainder > 0: remainder -= 1
            
            # Send the ASSIGN_WORK message and CHECK the return value.
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
                # If self.send fails, the peer was removed within self.send(), 
                # and we don't count it towards expected results.
                self.log(f"⚠️ Leader dispatch failed to Agent {pid}. Work not assigned.")
                
            start = end + 1 # Update start for the next agent (or the leader)

        # 3. Leader performs own chunk (The remaining part)
        # The leader's chunk starts from the last calculated 'start' up to 'total_limit'
        if start <= total_limit:
            end = total_limit
            my_result = sum(range(start, end + 1))
            
            # The leader's result is added immediately to the accumulated total
            self.task_accumulated += my_result
            self.log(f"Leader Task: Sum({start}-{end}) = {my_result}. Waiting for {self.expected_results} peers...")
            
        else:
            # This happens if total_limit was 0, or if all work was distributed to peers (unlikely).
            self.log(f"Leader Task: Sum(0-0) = 0. Waiting for {self.expected_results} peers...")
            
    # ------------------ Election ------------------ #
    def start_election(self):
        now = time.time()
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
            socks = dict(self.poller.poll(10)) # Poll for 10ms (non-blocking wait)
            if self.server in socks:
                frames = self.server.recv_multipart()
                # Process the message immediately. If it's an ALIVE, 
                # the handle_msg function will set self.in_election = False
                self.handle_msg(frames) 
                
            if not self.in_election:
                return  # ALIVE received and processed by higher agent

        # If no ALIVE received and we are still in election mode, declare self leader
        if self.in_election:
            self.declare_victory()

    def declare_victory(self):
        self.leader = self.id
        self.in_election = False
        self.last_leader_pong = time.time()
        self.log(f"🏆 DECLARED LEADER {self.id}")
        for p in self.peers:
            self.send(p, {"type": "COORDINATOR", "from": self.id, "leader": self.id})


    # ------------------ Leader Monitor ------------------ #
    def leader_monitor_loop(self):
        while self.running:
            if self.task_running_event.is_set():
                time.sleep(0.2)
                continue

            # Check every 0.5s for faster leader detection
            time.sleep(0.5)

            if self.leader and self.leader != self.id:
                leader_id = self.leader
                if leader_id not in self.peers:
                    self.log(f"Leader {leader_id} gone. ⚠️ Starting election immediately.")
                    self.leader = None
                    self.last_leader_pong = 0
                    self.start_election()
                    continue

                self.send(leader_id, {"type": "PING", "from": self.id})

                # If leader unresponsive, trigger election
                if time.time() - self.last_leader_pong > 2:  # faster timeout
                    self.log(f"🚨 Leader {leader_id} unresponsive. Starting election.")
                    self.leader = None
                    self.start_election()

            elif not self.leader and not self.in_election and len(self.peers) > 0:
                self.log("No leader detected. Starting election.")
                self.start_election()

    # ------------------ Run ------------------ #
    def start(self):
        self.register_with_broker()

        if not self.peers:
            self.leader = self.id
            self.declare_victory()
        else:
            self.log("Peers detected on startup. Initiating election for coordinated leader selection.")
            self.start_election()

        threading.Thread(target=self.broker_heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.leader_monitor_loop, daemon=True).start()

        while self.running:
            try:
                socks = dict(self.poller.poll(100))
                if self.server in socks:
                    frames = self.server.recv_multipart()
                    self.handle_msg(frames)
            except zmq.error.ZMQError:
                pass
            except Exception:
                pass