from abc import ABC, abstractmethod

class AbstractTask(ABC):

    @abstractmethod
    def start(self) -> None:
        pass