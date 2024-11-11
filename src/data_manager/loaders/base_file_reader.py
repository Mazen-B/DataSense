from abc import ABC, abstractmethod

class BaseFileReader(ABC):
    """
  This is an abstract file reader class, that acts as a blueprint for all file readers.
  """
    def __init__(self, file_path):
        self.file_path = file_path

    @abstractmethod
    def read_file(self, columns=None):
        pass