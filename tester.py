import glob
import logging
import os
import signal
import sys
from threading import current_thread
import csv
from client import main_runner

import pandas as pd

from constants import LOGFILE, N_WORKERS, DATA_PATH,COUNT

from worker import WcWorker
from mrds import MyRedis

import time


if __name__ == "__main__":
    
    
    rds = MyRedis()
    start = time.time()
    # main_runner(rds)
    end = time.time()
    print(end - start)
    
    print("Done with counting with redis")
    
    word_count = {}
    
    for file in glob.glob(DATA_PATH):
      print(file)
      df = pd.read_csv(file)
      
      for index, row in df.iterrows():
        text_is = row['text']
        try:
          for word in text_is.split():
            if word in word_count:
                word_count[word] += 1
            else:
              word_count[word] = 1
        except:
          print(text_is)
          
      
    # for file in glob.glob(DATA_PATH):
    #     with open(file) as f:
          
    #       csvreader = csv.reader(f)
    #       next(csvreader, None)
    #       for row in csvreader:
    #         # print(row)
    #         try:
    #           txt = row[4]
    #         except:
    #           txt = ""
    #         for word in txt.split():
    #           if word in word_count:
    #               word_count[word] += 1
    #           else:
    #             word_count[word] = 1
            
    word_count = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
    
    print("Done with counting serially")
    
    for word, count in word_count:
        if rds.rds.zscore(COUNT,word) != count:
          print(word)
        # assert rds.rds.zscore(COUNT,word) == count
        # v = rds.rds.hget("WORD",word)
        # v = v.decode()
        # v = int(v)
        # assert v == count
    
    print("Works ✅")
    
    
        