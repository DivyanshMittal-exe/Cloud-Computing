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
    main_runner(rds)
    end = time.time()
    
    print(f"Done with counting with redis in {end-start} seconds")
    
    word_count = {}
    
    
    wc = {}

    # for filename in glob.glob(DATA_PATH):

    # filename = "twcs.csv"

    #     df = pd.read_csv(filename)
    #     df["text"] = df["text"].astype(str)
    #     for text in df.loc[:,"text"]:
    #         if text == '\n':
    #             continue

    #         for word in text.split(" "):
    #             if word not in wc:
    #                 wc[word] = 0
    #             wc[word] = wc[word] + 1

    
    # word_count = wc
    for file in glob.glob(DATA_PATH):
        df = pd.read_csv(file,lineterminator='\n', usecols=['text'], dtype={'text': str})
        df["text"] = df["text"].astype(str)
        for text in df.loc[:,"text"]:
            if text == '\n':
                continue

            for word in text.split(" "):
                if word not in word_count:
                    word_count[word] = 0
                word_count[word] = word_count[word] + 1
            
    word_count = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
    
    print("Done with counting serially")
    
    for word, count in word_count:
        if rds.rds.zscore(COUNT,word) != count:
          # pass
          print(f"Word {word} has {rds.rds.zscore(COUNT,word)} in redis and {count} in serial")
          
          # Append the above line to ft_logs.txt
          with open("ft_logs.txt", 'a') as file:
            file.write(f"Word {word} has {rds.rds.zscore(COUNT,word)} in redis and {count} in serial\n")
            break
    else:
        # assert rds.rds.zscore(COUNT,word) == count
        with open("ft_logs.txt", 'a') as file:
            file.write(f"Works ✅ in {end-start} seconds\n")
    
        print(f"Works ✅ in {end-start} seconds")
    
    print("Top 3 words are:")
    for word, c in rds.top(3):
      logging.info(f"{word.decode()}: {c}")
    
    
        