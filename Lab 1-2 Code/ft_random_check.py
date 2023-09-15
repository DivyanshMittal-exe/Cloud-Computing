import psutil
from constants import LOGFILE, N_WORKERS, DATA_PATH,COUNT


def get_number_of_tester():
    processes = []
    for process in psutil.process_iter(['pid', 'cmdline','status']):
        try:
            process_info = process.info
            pid = process_info['pid']
            cmdline = process_info['cmdline']
            
            # Process cmdline might be a list, convert it to a string
            cmdline_str = " ".join(cmdline)
            
            if "python3 tester.py" in cmdline_str or "python3 client.py" in cmdline_str:
                processes.append(process)
            
    
            # print(f"PID: {pid}, Command: {cmdline_str}")
            
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    
    return processes

import random
import time

def main():
    
    cleanup = get_number_of_tester()
    print(f"Killing all the tester processes {cleanup}")
    for tester in cleanup:
        tester.kill()
    

    am_i_done = False
    while True:
        processes = get_number_of_tester()

        if(len(processes) > 1):
            am_i_done = True
            relevant = processes[1:]
            process_to_tinker = random.choice(relevant)
            # print(process_to_tinker)
            print(process_to_tinker.status())
            if process_to_tinker.status() != psutil.STATUS_STOPPED:
                print(f"Killing process {process_to_tinker.info['pid']}")
                process_to_tinker.suspend()
            else:
                print(f"Resuming process {process_to_tinker.info['pid']}")
                process_to_tinker.resume()    
            
            time.sleep(10*random.random())
        elif am_i_done:
            print("Done with tinkering")
            for process in processes:
                if process.status() != psutil.STATUS_RUNNING:
                    process.resume()
            break
            
            
        
if __name__ == "__main__":
    main()
