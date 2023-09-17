import logging
from typing import Any
import pandas as pd

import os
import re

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
      data = rds.rds.xreadgroup(WcWorker.GROUP, self.name, {IN: ">"}, count=1)
      if not data:
        break
      else:
        data = data[0][1][0]
        id,data = data
        file = data[FNAME].decode()
        logging.debug(f"|{self.name}| Processing {file}")
        
        local_count = {}
        
        # with open(file, mode='r', newline='\r') as f:
        #   for text in f:
        #     if text == '\n':
        #         continue
        #     sp = text.split(',')[4:-2]
        #     tweet = " ".join(sp)
        #     for word in tweet.split(" "):
        #         if word not in local_count:
        #           local_count[word] = 1
        #         else:
        #           local_count[word] += 1
        
        
        df = pd.read_csv(file,lineterminator='\n', usecols=['text'], dtype={'text': str})
        df["text"] = df["text"].astype(str)
        for text in df.loc[:,"text"]:
            if text == '\n':
                continue

            for word in text.split(" "):
                if word not in local_count:
                    local_count[word] = 0
                local_count[word] = local_count[word] + 1
        
        
        
        # try:
        #   df = pd.read_csv(file, usecols=['text'], dtype={'text': str},lineterminator='\n')  
        #   text_column = df['text'].tolist()        
        #   for words in text_column:
        #     try:
        #       for word in words.split(" "):
        #         if word in local_count:
        #             local_count[word] += 1
        #         else:
        #           local_count[word] = 1
        #     except:
        #       logging.critical(f"Couldn't split {words}")

        # except pd.errors.ParserError as e:
        #   logging.critical(f"{file} ParserError:", e)
        
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
          rds.rds.zincrby(COUNT,count,word)
          
        rds.rds.xack(IN, WcWorker.GROUP, id)
        logging.debug(f"|{self.name}| Done processing {file}")
          
    # send ack after processing the file
        
    # logging.info(f"Killing {self.name}")
    # self.kill()