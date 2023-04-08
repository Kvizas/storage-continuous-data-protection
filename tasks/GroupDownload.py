from tasks.AbstractTask import AbstractTask
from tasks.Download import DownloadTask

from logger import log
import traceback

from data_sources.Source import Source
from classes.FileMetadata import FileMetadata

class GroupDownloadTask(AbstractTask):

    def __init__(self, source: Source) -> None:
        self.download_list: list[FileMetadata] = []
        self.skipped: int = 0
        self.failed: int = 0
        self.successful: int = 0
        self.source: Source = source
        self.status: str = "preparing"

    def start(self) -> None:
        try:
            self.status = "starting"
            log(f"GroupDownload task started with {len(self.download_list)} files in queue", "info", self.source.source_name)

            for file_metadata in self.download_list:
                self.download_file(file_metadata) 

            self.status = f"done ({self.successful}/{len(self.download_list)}, skipped {self.skipped}, failed {self.failed})"
            log(f"GroupDownload done ({self.successful}/{len(self.download_list)}, skipped {self.skipped}, failed {self.failed})", "debug", self.source.source_name)

        except:
            self.status = "failed"
            log(traceback.format_exc(), "error")

    def download_file(self, file_metadata: FileMetadata) -> None:
        self.status = f"({self.successful}/{len(self.download_list)}, skipped {self.skipped}, failed {self.failed})"

        download_task = DownloadTask(self.source, file_metadata)
        download_task.start()

        if download_task.status == "done":
            self.successful += 1
        if download_task.status == "skipped":
            self.skipped += 1
        if download_task.status == "failed":
            self.failed += 1

    def add_file(self, file_metadata: FileMetadata) -> None:
        self.download_list.append(file_metadata)