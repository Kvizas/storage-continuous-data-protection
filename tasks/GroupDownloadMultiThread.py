from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from multiprocessing import Manager, Lock

from tasks.AbstractTask import AbstractTask
from tasks.Download import DownloadTask

import os
from logger import log
import traceback

from data_sources.Source import Source
from classes.FileMetadata import FileMetadata

#
################################################################
#
#  THIS CLASS IS NOT COMPATIBLE WITH GOOGLE API CLIENT
#
################################################################
#
# This class was coded without knowing that
# httplib2 is not threadsafe and therefore neither is google-api-python-client.
# more info https://github.com/googleapis/google-api-python-client/issues/253
# 

class GroupDownloadMultiThreadTask(AbstractTask):

    def __init__(self, source: Source):
        self.download_list: list[FileMetadata] = []
        self.skipped: int = 0
        self.failed: int = 0
        self.successful: int = 0
        self.source: Source = source
        self.status: str = "preparing"

    def start(self):
        try:
            self.status = "starting"
            log(f"GroupDownload task started with {len(self.download_list)} files in queue", "info", self.source.source_name)

            executor = ThreadPoolExecutor(max_workers=int(os.getenv('MAX_DOWNLOAD_WORKERS')))
            lock = Manager().Lock()

            futures = [
                executor.submit(
                    self.download_file,
                    file_metadata,
                    lock
                ) for file_metadata in self.download_list
            ]

            wait(futures)
            self.status = f"done ({self.successful}/{len(self.download_list)}, skipped {self.skipped}, failed {self.failed})"
            log(f"GroupDownload done ({self.successful}/{len(self.download_list)}, skipped {self.skipped}, failed {self.failed})", "debug", self.source.source_name)

        except:
            self.status = "failed"
            log(traceback.format_exc(), "error")

    def download_file(self, file_metadata: FileMetadata, lock: Lock):
        with lock:
            self.status = f"({self.successful}/{len(self.download_list)}, skipped {self.skipped}, failed {self.failed})"

        download_task = DownloadTask(self.source, file_metadata)
        download_task.start()

        with lock:
            if download_task.status == "done":
                self.successful += 1
            if download_task.status == "skipped":
                self.skipped += 1
            if download_task.status == "failed":
                self.failed += 1

    def add_file(self, file_metadata: FileMetadata):
        self.download_list.append(file_metadata)