from __future__ import annotations

import logging
import os
import signal
import sys
from abc import abstractmethod, ABC
from threading import current_thread
from typing import Any, Final
from multiprocessing import Process


class Worker(ABC):
  GROUP: Final = "worker"

  def __init__(self, **kwargs: Any):
    self.name = "worker-?"
    self.pid = -1
    self.child_process = None

  def create_and_run(self, **kwargs: Any) -> None:
    # Create a process using the multiprocessing library
    
    # self.child_process = Process(target=self.run, kwargs=kwargs)
    # self.child_process.start()

    # self.kill()

    pid = os.fork()
    assert pid >= 0
    if pid == 0:
      # Child worker process
      self.pid = os.getpid()
      self.name = f"worker-{self.pid}"
      # print(f"Running {self.name}")
      thread = current_thread()
      thread.name = self.name
      logging.info(f"Starting")
      self.run(**kwargs)
      # print(f"Done with {self.name}")
      # self.kill()
      os._exit(0)
      sys.exit()
    else:
      self.pid = pid
      self.name = f"worker-{pid}"



  @abstractmethod
  def run(self, **kwargs: Any) -> None:
    raise NotImplementedError

  def kill(self) -> None:
    logging.info(f"Killing {self.name}, {self.pid}")
    print("Called Kill")
    # print(f"Trying to kill {self.child_process}")
    os.kill(self.pid, signal.SIGKILL)

    # # os.kill(self.pid, signal.SIGKILL)
    # if self.child_process is not None and self.child_process.is_alive():
    #   logging.info(f"Killing {self.name}")
    #   # self.child_process.terminate()
    #   self.child_process.join()  # Wait for the child process to terminate
    #
