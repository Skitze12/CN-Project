import zmq
import json
import sys
import time
import re

CONTROL_PORT = 9000
SERVER_HOST = "localhost"


def send_command(command, args):
    """Sends a command to the System Controller Handler via ZMQ DEALER/ROUTER."""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt_string(zmq.IDENTITY, "CLIENT_CLI")
    sock.setsockopt(zmq.RCVTIMEO, 5000) # 5s timeout
    
    try:
        sock.connect(f"tcp://{SERVER_HOST}:{CONTROL_PORT}")
        
        message = {
            "command": command,
            "args": args
        }
        
        # Send the command
        sock.send_json(message)
        
        # Wait for the response
        if sock.poll(5000):
            response = sock.recv_json()
            return response
        else:
            return {"status": "ERROR", "message": "Server timeout (5s) or failed to connect."}
            
    except zmq.error.ZMQError as e:
        return {"status": "ERROR", "message": f"ZMQ Error: Could not connect to system main. Is system_main.py running? ({e})"}
    except Exception as e:
        return {"status": "ERROR", "message": f"Unexpected error: {e}"}
    finally:
        sock.close()
        ctx.term()

def display_help():
    """Prints the help message for the CLI."""
    print("\n--- Distributed Summation Controller ---")
    print("Commands:")
    print("  list            : Show all active agents in the system.")
    print("  add <ID>        : Start a new agent with a unique ID (e.g., add 10).")
    print("  remove <ID>     : Terminate the process for an agent ID (e.g., remove 5).")
    print("  sum <LIMIT>     : Find the current leader and assign a summation task (e.g., sum 10000).")
    print("  quit/exit       : Shut down the entire background system.")
    print("  help            : Show this help message.")
    print("------------------------------------------")

def process_response(response):
    """Formats and prints the server response."""
    status = response.get("status")
    message = response.get("message")
    
    if status == "OK":
        if "agents" in response:
            print("✅ Active Agents:")
            if not response["agents"]:
                print("  (No agents currently active)")
            for agent in response["agents"]:
                print(f"  ID: {agent['id']:<4} Port: {agent['port']} Status: {agent['status']}")
        elif message:
            print(f"✅ Success: {message}")
        else:
            print("✅ Success: Command processed.")
    else:
        print(f"❌ ERROR: {message}")

def run_cli():
    """Main loop for the command line interface."""
    display_help()
    
    while True:
        try:
            user_input = input("CTRL > ").strip()
            if not user_input:
                continue

            parts = user_input.lower().split()
            command = parts[0]
            args = parts[1:]

            if command in ('quit', 'exit'):
                print("Initiating system shutdown...")
                response = send_command('quit', [])
                process_response(response)
                # Give time for the message to propagate and shutdown to initiate
                time.sleep(1) 
                print("Controller exiting.")
                break

            elif command == 'help':
                display_help()
                continue
                
            elif command == 'list':
                response = send_command('list', [])
            
            elif command == 'add' and args and args[0].isdigit():
                response = send_command('add', [args[0]])
                
            elif command == 'remove' and args and args[0].isdigit():
                response = send_command('remove', [args[0]])

            elif command == 'sum' and args and args[0].isdigit():
                response = send_command('sum', [args[0]])
            
            else:
                print(f"❓ Unknown command or invalid arguments for '{command}'. Type 'help' for commands.")
                continue

            process_response(response)

        except EOFError:
            print("\nEOF received. Exiting controller.")
            break
        except KeyboardInterrupt:
            # Note: This only exits the CLI, not the background system_main process
            print("\nInterrupt received. Use 'quit' to shut down the system or Ctrl+C again to force exit CLI.")
            break 

if __name__ == "__main__":
    run_cli()