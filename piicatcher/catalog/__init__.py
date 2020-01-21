from abc import ABC, abstractmethod


class Store(ABC):
    @classmethod
    @abstractmethod
    def save_schemas(cls, explorer):
        pass
