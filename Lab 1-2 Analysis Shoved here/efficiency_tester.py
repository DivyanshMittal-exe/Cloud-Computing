import random
import os
import shutil
from mrds import MyRedis
from client import main_runner
import time

def is_file_empty(file_path):
    try:
      return os.path.getsize(file_path) == 0
    except:
      return True


import pandas as pd

base = "/home/higgsboson/Codes/Sem7/COL733"
base = "/home/baadalvm/COL733"
stress_test_at = base + "/StressTest/"
copy_from = base + "/Test/"

def get_constant(no_of_workers):
  name =f"""from typing import Final

LOGFILE: Final[str] = "/tmp/wc.log"
N_WORKERS: Final[int] = {no_of_workers}
DATA_PATH: Final[str] = "{base}/StressTest/*.csv"
# DATA_PATH: Final[str] = "/home/higgsboson/Codes/Sem7/COL733/Test/*.csv"
# DATA_PATH: Final[str] = "/home/baadalvm/COL733/Test/*.csv"
IN: Final[bytes] = b"files"
FNAME: Final[bytes] = b"fname"
COUNT: Final[bytes] = b"count"
"""
  return name

def count_files(folder_path):
  return len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])


if __name__ == "__main__":
  workers = [i for i in range(1,33)]
  # no_of_files = [2*i for i in range(1,40)]
  no_of_files =[50*i for i in range(1,11)]

  pairs = [(x, y) for x in workers for y in no_of_files]
  random.shuffle(pairs)

  logfile = "logs.csv"

  if is_file_empty(logfile):
    with open(logfile, "a") as log:
      log.write("worker, file, time \n")
  else:
      df = pd.read_csv(logfile, usecols=[0, 1])
      csv_pairs = set(map(tuple, df.values))
      pairs = [pair for pair in pairs if tuple(pair) not in csv_pairs]

  print(f"Running for {len(pairs)} cases")
  for pair in pairs:
    w, num = pair

    print(f"Running for {w} workers and {num} files of load")
    with open("constants.py","w") as f:
      f.write(get_constant(w))

    files_are =  os.listdir(copy_from)
    # files_to_copy_list = random.sample(files_are, k=num)
    files_to_copy_list = [random.choice(files_are) for _ in range(num)]

    
    files_to_copy_list = [os.path.join(copy_from,file) for file in files_to_copy_list]

    rds = MyRedis()
    start = time.time()
    main_runner(rds, w, files_to_copy_list)
    end = time.time()


    print(f"Done with {w}, {num}, {end - start} \n")


    with open(logfile, "a") as log:
      log.write(f"{w}, {num}, {end - start} \n")




