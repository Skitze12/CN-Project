import time
import sys
import multiprocessing

# Function to push messages onto the queue
def log(queue, source, msg):
    """Pushes a formatted message onto the multiprocessing queue."""
    timestamp = time.strftime("%H:%M:%S")
    queue.put(f"[{timestamp}] [{source}] {msg}")

# Function to listen to the queue and print messages to the console
def log_listener(queue, title):
    """Listens to the queue and prints messages to stdout with a header."""
    # Print the terminal header once
    sys.stdout.write(f"\n{'='*50}\n === {title} ===\n{'='*50}\n")
    sys.stdout.flush()
    
    # Listen loop
    while True:
        try:
            # Blocks until an item is available
            message = queue.get()
            if message is None: # Exit signal
                break
            sys.stdout.write(message + '\n')
            sys.stdout.flush()
        except:
            # Handle termination
            break