# Base class for storage backends
from abc import abstractmethod, ABC
from typing import List, Dict, Any

class Storage(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def read(self, path: str, **kwargs) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def write(self, data: List[Dict[str, Any]], path: str, **kwargs):
        pass

    @abstractmethod
    def delete(self, path: str, **kwargs):
        pass

    @abstractmethod
    def update(self, data: List[Dict[str, Any]], path: str, **kwargs):
        pass
