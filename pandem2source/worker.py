from abc import ABC, abstractmethod

class Worker:
  _conf = None
  _logging = None
  _parameters = None
  
  def __init__ (self, name, logging,):
    self.name = name 
  
  @property
  @abstractmethod
  def name(self):
    pass
  
  @property
  @abstractmethod
  def initparams(self):
    pass
  
  @property
  @abstractmethod
  def workparams(self):
    pass

  @property
  @abstractmethod
  def workparams(self):
    pass


  @abstractmethod
  def init(self, conf, log):
    pass

  @abstractlethod
  def work(self, args, conf, log):
    pass
