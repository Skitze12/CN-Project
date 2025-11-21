import multiprocessing
import zmq
import time
import threading
from agent import Agent
from broker import Broker
from logger import log_listener
import signal
import sys

# ----------------- Helpers ----------------- #
def run_broker(queue):
    Broker(queue).run()

def run_agent(aid, port, queue):
    try:
        Agent(aid, port, queue).start()
    except KeyboardInterrupt:
        pass

def trigger_leader_task(leader_port, total_sum_limit):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt_string(zmq.IDENTITY, "CLIENT")
    sock.connect(f"tcp://localhost:{leader_port}")
    
    print(f"\n>>> CLIENT: Sending START_TASK (0-{total_sum_limit}) to Leader at Port {leader_port}...\n")
    sock.send_json({
        "type": "START_TASK", 
        "from": "CLIENT", 
        "limit": total_sum_limit
    })
    sock.close()
    ctx.term()

# ----------------- Main ----------------- #
if __name__ == "__main__":
    log_queue = multiprocessing.Queue()
    threading.Thread(target=log_listener, args=(log_queue, "Distributed Summation Log"), daemon=True).start()

    print("\n=== DISTRIBUTED SUMMATION SYSTEM (Automated Trigger) ===")
    
    # Start Broker
    b_proc = multiprocessing.Process(target=run_broker, args=(log_queue,))
    b_proc.start()
    time.sleep(1)

    procs = []

    # Start Agents
    for aid, port in [(1, 6001), (2, 6002), (3, 6003), (4, 6004), (5, 6005), (6,6006), (7,6007)]:
        p = multiprocessing.Process(target=run_agent, args=(aid, port, log_queue))
        p.start()
        procs.append(p)
        time.sleep(1)

    #procs[6].terminate()
    #time.sleep(10)
    #procs[5].terminate()
    # Wait for elections to stabilize after terminations
    
    time.sleep(10) 
    
    # Remove the extra time.sleep(20) before the trigger
    # time.sleep(20) 

    if True:
          #hardcoded for now 
          trigger_leader_task(6007, 1000)
    else:
          print("!!! FAILED TO DISCOVER LEADER AFTER MULTIPLE ATTEMPTS. CANNOT START TASK. !!!")# ----------------- Interrupt Handling ----------------- #
    
    try:
        # Wait for computation or user interrupt
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n>>> KeyboardInterrupt detected. Shutting down processes...")

        for p in procs:
            if p.is_alive():
                p.terminate()
                p.join()
        if b_proc.is_alive():
            b_proc.terminate()
            b_proc.join()

        print(">>> All agents and broker terminated. Exiting.")
        sys.exit(0)
