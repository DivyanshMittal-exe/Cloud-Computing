import random
import os
import shutil
from mrds import MyRedis
from client import main_runner
import time
import pandas as pd

def is_file_empty(file_path):
    try:
      return os.path.getsize(file_path) == 0
    except:
      return True

base = "/home/higgsboson/Codes/Sem7/COL733"
base = "/home/baadalvm/COL733"
stress_test_at = base + "/StressTest/"
copy_from = base + "/Test/"



def count_files(folder_path):
  return len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])



if __name__ == "__main__":
  no_of_files = [2*i for i in range(1,40)]

  logfile = "serial_logs.csv"
  with open(logfile, "w") as log: 
    log.write("file,serial_time\n")

#   if is_file_empty(logfile):
#     with open(logfile, "a") as log:
#       log.write("file,serial_time\n")
#   else:
#       df = pd.read_csv(logfile, usecols=[0])
#       files_done = df['file'].values.tolist()
#       no_of_files = [file for file in no_of_files if file not in files_done]

  print(f"Running for {len(no_of_files)} cases")
  
  
  for file_count in no_of_files:

    print(f"Ran for {file_count} files", end=", ")

    files_are =  os.listdir(copy_from)
    files_to_copy_list = random.sample(files_are, file_count)

    files_to_copy_list = [os.path.join(copy_from,file) for file in files_to_copy_list]

    st = time.time()
    
    local_count = {}

    for file in files_to_copy_list:
        df = pd.read_csv(file,lineterminator='\n', usecols=['text'], dtype={'text': str})
        df["text"] = df["text"].astype(str)
        for text in df.loc[:,"text"]:
            if text == '\n':
                continue

            for word in text.split(" "):
                if word not in local_count:
                    local_count[word] = 0
                local_count[word] = local_count[word] + 1

    et = time.time()
    print(f"It took {et-st} seconds")
    
    with open(logfile, "a") as log:
      log.write(f"{file_count},{et-st}\n")




