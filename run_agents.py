import multiprocessing
import time
from agent import Agent

def start_agent(agent_id):
    # Initializing the agent with his ID
    Agent(agent_id).run()


if __name__ == "__main__":
    # Temporarily starting with 5 agents
    num_agents = 5

    # Similiar to what we did in OS processes, create a process for each agent and start it
    processes = []

    for i in range(num_agents):
        p = multiprocessing.Process(target=start_agent, args=(i,))
        processes.append(p)
        p.start()
        time.sleep(1)

    # Wait for all processes to finish
    for p in processes:
        p.join()