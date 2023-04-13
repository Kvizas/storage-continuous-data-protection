import os
import traceback

from tasks.AbstractTask import AbstractTask
from classes.S3Storage import S3Storage
from database import connect_to_db

from logger import log

from data_sources.Source import Source
from classes.FileMetadata import FileMetadata

class DownloadTask(AbstractTask):

    def __init__(self, source: Source, file_metadata: FileMetadata) -> None:

        self.file_metadata: FileMetadata = file_metadata
        self.source: Source = source
        self.status: str = "prepairing"

    def start(self) -> None:
        try:
            if self.file_metadata.is_captured():
                self.status = "skipped"
                return

            self.status = "downloading"
            self.file_metadata = self.source.download_file(self.file_metadata)

            self.status = "uploading"
            storage = S3Storage()
            storage.upload_file(self.file_metadata.file_path, self.file_metadata)

            self.status = "registering"
            self.file_metadata.register_as_captured()

            try:
                self.status = "cleaning_up"
                os.remove("temp/" + self.file_metadata.file_path)
            except:
                pass

            self.status = "done"

        except:
            self.status = "failed"
            log(f"Error while downloading: \n" + traceback.format_exc(), "warn", self.source.source_name, self.file_metadata.file_path)