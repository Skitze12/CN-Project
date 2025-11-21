from datetime import datetime

# Simple logging function to log messages with timestamps
def log(agent_id, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [Agent {agent_id}] {message}")
