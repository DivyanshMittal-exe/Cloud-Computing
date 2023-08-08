import logging
from typing import Any

import csv

from base import Worker
from constants import FNAME
from mrds import MyRedis
from constants import IN, COUNT, FNAME


class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    
    rds.rds.hincrby("PID",self.pid) 
    # Write the code for the worker thread here.
    
    while True:
      data = rds.rds.xreadgroup(WcWorker.GROUP, self.name, {IN: ">"}, count=1)
      if not data:
        break
      else:
        data = data[0][1][0]
        id,data = data
        file = data[FNAME].decode()
        logging.debug(f"Processing {file}")
        
        local_count = {}

        with open(file) as f:
          
          csvreader = csv.reader(f)
          next(csvreader, None)
          for row in csvreader:
            # print(row)
            try:
              txt = row[4]
            except:
              txt = ""
            for word in txt.split():
              if word in local_count:
                  local_count[word] += 1
              else:
                local_count[word] = 1
            
            # for line in f:
            #   for word in line.split():
            #     if word in local_count:
            #       local_count[word] += 1
            #     else:
            #       local_count[word] = 1
        
        for word, count in local_count.items():
          # rds.rds.hincrby(COUNT,word,count)
          # rds.rds.zincrby(self.pid,count,word)
          rds.rds.zincrby(COUNT,count,word)
          
        rds.rds.xack(IN, WcWorker.GROUP, id)
        logging.info(f"Done processing {file}")
          
    # send ack after processing the file
        
    logging.info("Exiting")
