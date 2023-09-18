import glob
import logging
import os
import signal
import sys
from threading import current_thread
from typing import List


from constants import LOGFILE, N_WORKERS, DATA_PATH,COUNT
from worker import WcWorker
from mrds import MyRedis

workers = []
# workers: list[WcWorker] = []
def sigterm_handler(signum, frame):
  logging.info('Killing main process!')
  for w in workers:
    print("Inside sigterm")
    w.kill()
  sys.exit()




def main_runner(rds, no_of_workers = None, files_to_copy_list = None):
  # Clear the log file
  # open(LOGFILE, 'w').close()
  logging.basicConfig(filename=LOGFILE,
                      level=logging.DEBUG,
                      force=True,
                      format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s')
  
  thread = current_thread()
  thread.name = "client"
  logging.debug('Done setting up loggers.')

  if files_to_copy_list == None:
    no_of_files = 0
    for file in glob.glob(DATA_PATH):
      no_of_files += 1
      rds.add_file(file)
  else:
    no_of_files = 0
    for file in files_to_copy_list:
        no_of_files += 1
        rds.add_file(file)

  signal.signal(signal.SIGTERM, sigterm_handler)

  if no_of_workers is not None:
    workers = []
    N_WORKERS = no_of_workers


  for i in range(N_WORKERS):
    workers.append(WcWorker(rds=rds))

  print(f"Working with {len(workers)} workers and {len(files_to_copy_list)} files")

  for w in workers:
    w.create_and_run(rds=rds)

  # for w in workers:
  #   w.kill()

  logging.debug('Created all the workers')
  while True:
    try:
      pid_killed, status = os.wait()
      logging.info(f"Worker-{pid_killed} died with status {status}!")
    except:
      break

  
  # pids = rds.rds.hkeys("PID")
  
  # rds.rds.zunionstore(COUNT,pids)
  
  
    

if __name__ == "__main__":
  rds = MyRedis()
  main_runner(rds)

  for word, c in rds.top(3):
    logging.info(f"{word.decode()}: {c}")