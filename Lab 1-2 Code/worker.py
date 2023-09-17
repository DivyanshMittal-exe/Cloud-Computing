import logging
from typing import Any
import pandas as pd
import time

import os
import re
import json

from base import Worker
from constants import FNAME
from mrds import MyRedis
from constants import IN, COUNT, FNAME


class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    
    self.pid = os.getpid()
    self.name = f"worker-{self.pid}"
    logging.info(f"Created {self.name}")
    
    # rds.rds.hincrby("PID",self.pid)     
    while True:
      try:
        data = rds.rds.xreadgroup(WcWorker.GROUP, self.name, {IN: ">"}, count=1)
      except:
        logging.debug(f"|{self.name}| Encountered an error while Reading. Redis is probably busy")
        continue
      # xreadgroup(groupname, consumername, streams, count=None, block=None, noack=False)
      if not data:
        time_in_ms = 1000
        try:
          data = rds.rds.xautoclaim(IN, WcWorker.GROUP, self.name, time_in_ms) 
        except:
          logging.debug(f"|{self.name}| Encountered an error while AutoClaiming. Redis is probably busy")
          continue
        # if not data[1]:
        
        # print(data)
        
        if not data[1]:
          try:
            p_inf = rds.rds.xpending(IN, Worker.GROUP)
            if p_inf.get("pending") == 0:
              break
            else:
              logging.debug(f"|{self.name}| Someone is pending")
              time.sleep(1)
              continue
              
          except:
            continue
          
        # print(data)
        data_temp = data[1][0]
        id,data_temp = data_temp
        file_claimed = data_temp[FNAME].decode()
        logging.debug(f"|{self.name}| Auto Claimed {file_claimed}")
        data = [data]
        # xautoclaim(name, groupname, consumername, min_idle_time, start_id='0-0', count=None, justid=False)
     
      
      data = data[0][1][0]
      id,data = data
      file = data[FNAME].decode()
      logging.debug(f"|{self.name}| Processing {file}")
      
      local_count = {}
      
      
      df = pd.read_csv(file,lineterminator='\n', usecols=['text'], dtype={'text': str})
      df["text"] = df["text"].astype(str)
      for text in df.loc[:,"text"]:
          if text == '\n':
              continue

          for word in text.split(" "):
              if word not in local_count:
                  local_count[word] = 0
              local_count[word] = local_count[word] + 1
      
      
      
      keys_and_args = [IN,COUNT,WcWorker.GROUP, id, json.dumps(local_count)]   # consumer_group, id, localCountJSON 
      success = None 
      
      while success is None:
        try:
          success = rds.rds.fcall("push_wc", 2 , *keys_and_args)
          if success:
            logging.debug(f"|{self.name}| Done processing {file}")
          else:
            logging.debug(f"|{self.name}| Tried processing {file}, already acked by someone else")
        except:
          logging.debug(f"|{self.name}| Encountered an error while processing {file}. Redis is probably busy")
      


    # send ack after processing the file
        
    # logging.info(f"Killing {self.name}")
    # print(f"Calling kill on {self.name}")
    self.kill()
