from abc import ABC, abstractmethod

# pylint disable=too-few-public-methods
class Store(ABC):
    @classmethod
    @abstractmethod
    def save_schemas(cls, explorer):
        pass
