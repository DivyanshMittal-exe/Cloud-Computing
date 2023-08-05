import logging
from typing import Any

from base import Worker
from constants import FNAME
from mrds import MyRedis
from constants import IN, COUNT, FNAME


class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    
    rds.rds.hincrby("PID",self.pid) 
    # Write the code for the worker thread here.
    
    # Read the FILENAME from the redis server xstream, and count the frequency of number of words in the file. I have an xgroup stream for redis variable rds as rds.xgroup_create(IN, Worker.GROUP, id="0", mkstream=True) and the filenames in the stream are appended as self.rds.xadd(IN, {FNAME: fname})
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
          for line in f:
            for word in line.split():
              if word in local_count:
                local_count[word] += 1
              else:
                local_count[word] = 1
        
        for word, count in local_count.items():
          # rds.rds.hincrby(COUNT,word,count)
          # rds.rds.zincrby(self.pid,count,word)
          rds.rds.zincrby(COUNT,count,word)
          
        rds.rds.xack(IN, WcWorker.GROUP, id)
        logging.info(f"Done processing {file}")
          
    # send ack after processing the file
        
    logging.info("Exiting")
