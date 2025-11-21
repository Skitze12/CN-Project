# broker.py
import zmq
import threading
import time
from logger import log

class Broker:
    def __init__(self, log_queue, port=5000):
        self.port = port
        self.log_queue = log_queue
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{self.port}")
        self.registry = {}  # agent_id -> {host, port, last_seen}
        self.lock = threading.Lock()
        self.running = True

    def log(self, msg):
        log(self.log_queue, "BROKER", msg)

    def janitor(self):
        """Removes agents that haven't sent a heartbeat recently."""
        while self.running:
            time.sleep(2)
            with self.lock:
                now = time.time()
                dead = [aid for aid, info in self.registry.items() 
                        if now - info['last_seen'] > 15]
                for aid in dead:
                    self.log(f"Timeout: Removing Agent {aid}")
                    del self.registry[aid]

    def run(self):
        self.log(f"Registry online at port {self.port}")
        threading.Thread(target=self.janitor, daemon=True).start()

        while self.running:
            try:
                if self.socket.poll(100):
                    msg = self.socket.recv_json()
                    action = msg.get('action')
                    aid = msg.get('id')

                    with self.lock:
                        if action == "REGISTER":
                            self.log(f"Registering Agent {aid}...")
                            self.registry[aid] = {
                                'host': msg['host'],
                                'port': msg['port'],
                                'last_seen': time.time()
                            }
                            peers = {k: v for k, v in self.registry.items() if k != aid}
                            self.socket.send_json({"status": "OK", "peers": peers})

                        elif action == "HEARTBEAT":
                            if aid in self.registry:
                                self.registry[aid]['last_seen'] = time.time()
                                peers = {k: v for k, v in self.registry.items() if k != aid}
                                self.socket.send_json({"status": "OK", "peers": peers})
                            else:
                                self.socket.send_json({"status": "RE_REGISTER"})
                                
                        elif action == "GET_REGISTRY":
                            self.socket.send_json({"status": "OK", "registry": self.registry})

                        elif action == "READY":
                            if aid in self.registry:
                                self.registry[aid]['ready_for_tasks'] = True
                            self.socket.send_json({"status": "OK"})

                        else:
                            self.log(f"Unknown action: {action}")
                            self.socket.send_json({"status": "ERROR", "message": "Unknown action"})
            except Exception as e:
                self.log(f"Error in Broker loop: {e}")
