import logging
from typing import Any
import pandas as pd

import sys

import re

from base import Worker
from constants import FNAME
from mrds import MyRedis
from constants import IN, COUNT, FNAME


class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    
    # rds.rds.hincrby("PID",self.pid)     
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
        
        try:
          df = pd.read_csv(file, usecols=['text'], dtype={'text': str})  
          text_column = df['text'].tolist()        
          for words in text_column:
            try:
              for word in words.split():
                if word in local_count:
                    local_count[word] += 1
                else:
                  local_count[word] = 1
            except:
              pass

        except pd.errors.ParserError as e:
          print("ParserError:", e)
            # print(words)
            # sys.exit(1)
          
            
              
      
        

        
        

        # with open(file) as f:
          
        #   data = f.read()
          
        #   parsed_data = re.findall(r'(?:,|\n|^)("(?:(?:"")*[^"]*)*"|[^",\n]*|(?:\n|$))', data)

        # data_lines = parsed_data[5:]
        # data_lines = data_lines[6::7]
        
        
        # for txt in data_lines:
        #   for word in txt.split():
        #     if word in local_count:
        #         local_count[word] += 1
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
