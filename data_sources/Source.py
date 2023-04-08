from abc import ABC, abstractmethod
from logger import log

from classes.FileMetadata import FileMetadata

class Source(ABC):
    
    source_name: str = None

    @abstractmethod
    def download_file(self, file_metadata: FileMetadata) -> None:
        pass
