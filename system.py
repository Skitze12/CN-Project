import multiprocessing
import zmq
import time
import threading
from agent import Agent
from broker import Broker
from logger import log_listener, log
import signal
import sys
import os
import json

# --- Shared State for Agent Management ---
active_agents = {} # agent_id -> Process object
agent_lock = threading.Lock() 
NEXT_PORT = 6006

# --- Helper Functions (Same as before) ---
def run_broker(queue):
    """Wrapper to run the Broker process."""
    try:
        Broker(queue).run()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log(queue, "BROKER_PROC", f"Fatal Error: {e}")

def run_agent(aid, port, queue):
    """Wrapper to run the Agent process."""
    try:
        Agent(aid, port, queue).start()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log(queue, f"AGENT {aid}", f"Fatal Error: {e}")

def trigger_task_to_port(leader_port, total_sum_limit, log_queue):
    """Sends the START_TASK message to a specific port."""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt_string(zmq.IDENTITY, "CLIENT")
    sock.setsockopt(zmq.RCVTIMEO, 1000) 
    
    log(log_queue, "CLIENT", f"Attempting to send task to port {leader_port}...")

    try:
        sock.connect(f"tcp://localhost:{leader_port}")
        
        log(log_queue, "CLIENT", f"Sending START_TASK (0-{total_sum_limit}) to Port {leader_port}...")
        sock.send_json({
            "type": "START_TASK", 
            "from": "CLIENT", 
            "limit": total_sum_limit
        })
        log(log_queue, "CLIENT", "Message sent.")
        
    except zmq.error.ZMQError as e:
        log(log_queue, "CLIENT", f"!!! Failed to send task to port {leader_port}: {e}")
    finally:
        sock.close()
        ctx.term()

def get_current_leader_port(log_queue, broker_host="localhost", broker_port=5000):
    """Asks the Broker for the registry and finds the agent with the highest ID that is ready."""
    ctx = zmq.Context()
    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.RCVTIMEO, 1000)
    req.connect(f"tcp://{broker_host}:{broker_port}")
    
    leader_id = -1
    leader_port = None
    
    try:
        req.send_json({"action": "GET_REGISTRY"})
        if req.poll(1000):
            resp = req.recv_json()
            if resp.get("status") == "OK":
                registry = resp.get("registry", {})
                
                # --- NEW CODE: Use active_agents and agent_lock ---
                with agent_lock:
                    for aid_str, info in registry.items():
                        try:
                            aid = int(aid_str)
                            # 1. Check if agent is ready (from broker)
                            if info.get('ready_for_tasks', False): 
                                # 2. CRITICAL CHECK: Ensure the process is actually alive in our system map
                                if aid in active_agents and active_agents[aid].is_alive():
                                    if aid > leader_id:
                                        leader_id = aid
                                        leader_port = info['port']
                                # Optional: Log if broker has a dead agent
                                elif aid in active_agents and not active_agents[aid].is_alive():
                                    log(log_queue, "SYSTEM", f"Broker registry lists Agent {aid} as ready, but its process is dead locally.")
                                elif aid not in active_agents:
                                    log(log_queue, "SYSTEM", f"Broker registry lists Agent {aid}, but it's not in active_agents map.")
                                    
                        except ValueError:
                            continue # Skip non-integer IDs
                # --- END NEW CODE ---
                #                
                if leader_id != -1:
                    log(log_queue, "SYSTEM", f"Leader found: Agent {leader_id} on Port {leader_port}")
                    return leader_port
                else:
                    log(log_queue, "SYSTEM", "No ready agents found in registry to be leader.")
                    return None
            else:
                log(log_queue, "SYSTEM", f"Broker error during leader lookup: {resp.get('message')}")
                return None
        else:
            log(log_queue, "SYSTEM", "Broker did not respond to GET_REGISTRY.")
            return None
    except zmq.error.ZMQError:
        log(log_queue, "SYSTEM", "Could not connect to broker.")
    finally:
        req.close()
        ctx.term()
    return None

# --- New: Controller Handler Thread ---
class ControllerHandler(threading.Thread):
    def __init__(self, log_queue, control_port=9000):
        super().__init__(daemon=True)
        self.log_queue = log_queue
        self.running = True
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind(f"tcp://*:{control_port}")
        self.socket.setsockopt(zmq.RCVTIMEO, 100) # 100ms timeout
        self.log("Control Handler online on Port 9000.")

    def log(self, msg):
        log(self.log_queue, "CTRL_SVR", msg)

    def process_command(self, sender_id, msg):
        global NEXT_PORT
        command = msg.get("command", "").lower()
        args = msg.get("args", [])
        response = {"status": "ERROR", "message": "Unknown command"}
        
        try:
            if command == 'add':
                if not args or not args[0].isdigit():
                    response = {"status": "ERROR", "message": "Missing or invalid Agent ID."}
                else:
                    aid = int(args[0])
                    with agent_lock:
                        if aid in active_agents:
                            response = {"status": "ERROR", "message": f"Agent {aid} is already running."}
                        else:
                            port = NEXT_PORT
                            NEXT_PORT += 1
                            
                            self.log(f"Launching new Agent {aid} on Port {port}...")
                            # Important: Pass the log queue to the agent process
                            p = multiprocessing.Process(target=run_agent, args=(aid, port, self.log_queue))
                            p.start()
                            active_agents[aid] = p
                            response = {"status": "OK", "message": f"Agent {aid} launched on Port {port}."}

            elif command == 'remove':
                if not args or not args[0].isdigit():
                    response = {"status": "ERROR", "message": "Missing or invalid Agent ID."}
                else:
                    aid = int(args[0])
                    with agent_lock:
                        if aid not in active_agents:
                            response = {"status": "ERROR", "message": f"Agent {aid} not found."}
                        else:
                            p = active_agents[aid]
                            if p.is_alive():
                                self.log(f"Terminating Agent {aid}...")
                                p.terminate()
                                p.join()
                                del active_agents[aid]
                                new_leader = max(active_agents.keys()) if active_agents else "None"
                                response = {"status": "OK", "message": f"Agent {aid} terminated. New election triggered."}
                            else:
                                del active_agents[aid]
                                response = {"status": "OK", "message": f"Agent {aid} was already dead. Removed from list."}

            elif command == 'sum':
                if not args or not args[0].isdigit():
                    response = {"status": "ERROR", "message": "Missing or invalid summation LIMIT."}
                else:
                    limit = int(args[0])
                    leader_port = get_current_leader_port(self.log_queue)
                    
                    if leader_port:
                        trigger_task_to_port(leader_port, limit, self.log_queue)
                        response = {"status": "OK", "message": f"Task sent to current leader on Port {leader_port}."}
                    else:
                        response = {"status": "ERROR", "message": "No active leader found to start the task."}

            elif command == 'list':
                with agent_lock:
                    agent_list = []
                    for aid, p in active_agents.items():
                        # We must rely on the initialization data (p.args[1]) for the port
                        # as the Agent Process object itself doesn't expose the port easily.
                        port = p.args[1] if len(p.args) > 1 else "Unknown"
                        status = "Alive" if p.is_alive() else "Dead"
                        agent_list.append({"id": aid, "port": port, "status": status})

                    response = {"status": "OK", "agents": agent_list}
            
            elif command == 'quit':
                self.running = False
                response = {"status": "OK", "message": "System shutdown initiated."}
                # System shutdown will be handled by the main thread

            else:
                response = {"status": "ERROR", "message": f"Unknown command: {command}"}

        except Exception as e:
            self.log(f"Error processing command {command}: {e}")
            response = {"status": "ERROR", "message": f"Internal server error: {e}"}

        # Send response back to the controller client
        self.socket.send_multipart([sender_id, json.dumps(response).encode('utf-8')])

    def run(self):
        while self.running:
            try:
                # ROUTER socket polling/recv for client commands
                # We expect a multipart message: [identity, payload]
                if self.socket.poll(100):
                    frames = self.socket.recv_multipart()
                    if len(frames) == 2:
                        sender_id = frames[0]
                        msg_data = frames[1].decode('utf-8')
                        msg = json.loads(msg_data)
                        self.process_command(sender_id, msg)
                
            except zmq.error.ZMQError:
                pass # Timeout, continue loop
            except Exception as e:
                self.log(f"Error in command loop: {e}")
        
        # Cleanup
        self.socket.close()
        self.context.term()
        self.log("Control Handler terminated.")

# --- Main Orchestration ---
def shutdown_system(b_proc, log_queue, controller_handler):
    """Gracefully shuts down all processes."""
    
    # Stop the Controller Handler thread
    if controller_handler.is_alive():
        controller_handler.running = False
        controller_handler.join(timeout=1)
    
    print("\n>>> Shutting down active agents...")
    with agent_lock:
        for aid, p in list(active_agents.items()): # Iterate over a copy
            if p.is_alive():
                log(log_queue, "SYSTEM", f"Terminating Agent {aid}...")
                p.terminate()
                p.join(timeout=1)
            del active_agents[aid]
    
    print(">>> Shutting down Broker...")
    if b_proc.is_alive():
        b_proc.terminate()
        b_proc.join(timeout=1)

    # Send None to logging queue to stop the listener thread (must be last)
    try:
        log_queue.put_nowait(None)
    except:
        pass

    print(">>> All components terminated. Exiting.")
    # Force exit to ensure all child processes and ZMQ contexts are cleared
    os._exit(0) 

if __name__ == "__main__":
    # 1. Setup Logging and Signal Handling
    log_queue = multiprocessing.Queue()
    threading.Thread(target=log_listener, args=(log_queue, "Distributed Summation Log"), daemon=True).start()

    print(f"\n=== DISTRIBUTED SUMMATION SYSTEM (PID: {os.getpid()}) ===")
    print("      *** Running in background. Use controller_cli.py ***")
    
    # 2. Start Broker
    b_proc = multiprocessing.Process(target=run_broker, args=(log_queue,))
    b_proc.start()
    time.sleep(1) # Give the broker time to bind its socket

    # 3. Start Initial Agents
    initial_agents_ports = [(1, 6001), (2, 6002), (3, 6003), (4, 6004), (5, 6005)]
    
    for aid, port in initial_agents_ports:
        p = multiprocessing.Process(target=run_agent, args=(aid, port, log_queue))
        p.start()
        with agent_lock:
            active_agents[aid] = p
        time.sleep(0.5) # Give agents time to start and register
    
    # Set the starting port for any dynamically added agents
    NEXT_PORT = initial_agents_ports[-1][1] + 1

    # 4. Start the Controller Handler Thread (ZMQ Server)
    controller_handler = ControllerHandler(log_queue)
    controller_handler.start()

    # 5. Main loop runs until the controller thread or a keyboard interrupt shuts it down
    try:
        # Wait for the controller handler to receive a 'quit' command
        while controller_handler.is_alive() and controller_handler.running:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        shutdown_system(b_proc, log_queue, controller_handler)