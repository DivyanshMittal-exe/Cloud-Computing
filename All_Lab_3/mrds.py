from __future__ import annotations

import logging
from typing import Optional, Final

from redis.client import Redis

from base import Worker
from constants import IN, COUNT, FNAME

import time


class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379, password='',
                       db=0, decode_responses=False)
    self.rds.flushall()
    self.rds.xgroup_create(IN, Worker.GROUP, id="0", mkstream=True)

  def get_timestamp(self) -> float:
    timestamp = self.rds.time()
    return float(f'{timestamp[0]}.{timestamp[1]}')

  def add_file(self, fname: str) -> None:
    self.rds.xadd(IN, {FNAME: fname})

  def top(self, n: int) -> list[tuple[bytes, float]]:
    return self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n,
                                     withscores=True)

  def get_latency(self) -> list[float]:
    lat = []
    lat_data = self.rds.hgetall("latency")
    for k in sorted(lat_data.keys()):
      v = lat_data[k]
      lat.append(float(v.decode()))
    return lat

  def read(self, worker: Worker) -> Optional[tuple[bytes, dict[bytes, bytes]]]:


    while True:
      try:
        data = self.rds.xreadgroup(worker.GROUP, worker.name, {IN: ">"}, count=1)
      except:
        logging.debug(f"|{worker.name}| Encountered an error while Reading. Redis is probably busy")
        continue

      if not data:
        time_in_ms = 1000
        try:
          data = self.rds.xautoclaim(IN, worker.GROUP, worker.name, time_in_ms)
        except:
          logging.debug(f"|{worker.name}| Encountered an error while AutoClaiming. Redis is probably busy")
          continue

        if not data[1]:
          try:
            p_inf = self.rds.xpending(IN, Worker.GROUP)
            if p_inf.get("pending") == 0:
              return None, None
            else:
              logging.debug(f"|{worker.name}| Someone is pending")
              time.sleep(1)
              continue

          except:
            continue

        # print(data)
        data_temp = data[1][0]
        id, data_temp = data_temp
        file_claimed = data_temp[FNAME].decode()
        logging.debug(f"|{worker.name}| Auto Claimed {file_claimed}")
        data = [data]


      return data[0][1][0]

  def write(self, id: bytes, wc: dict[str, int]) -> None:
      return -1
