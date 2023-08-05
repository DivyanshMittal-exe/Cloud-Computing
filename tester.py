import glob
import logging
import os
import signal
import sys
from threading import current_thread

from client import main_runner

from constants import LOGFILE, N_WORKERS, DATA_PATH,COUNT

from worker import WcWorker
from mrds import MyRedis

import time


if __name__ == "__main__":
    
    
    rds = MyRedis()
    start = time.time()
    main_runner(rds)
    end = time.time()
    print(end - start)
    
    print("Done with counting with redis")
    
    word_count = {}
    for file in glob.glob(DATA_PATH):
        with open(file) as f:
            for line in f:
                for word in line.split():
                    if word in word_count:
                        word_count[word] += 1
                    else:
                        word_count[word] = 1
            
    word_count = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
    
    print("Done with counting serially")
    
    for word, count in word_count:
        assert rds.rds.zscore(COUNT,word) == count
        # v = rds.rds.hget("WORD",word)
        # v = v.decode()
        # v = int(v)
        # assert v == count
    
    print("Works ✅")
    
    
        