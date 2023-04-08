import os

from tasks.AbstractTask import AbstractTask
from classes.S3Storage import S3Storage
from logger import log
import traceback
from database import connect_to_db

class GroupRestoreTask(AbstractTask):

    def __init__(self, source, restore_path: str) -> None:

        self.source = source
        self.restore_path = restore_path
        self.status = "prepairing"

    def start(self) -> None:
        try:
            self.status = "getting_filenames"
            stored_filenames = self.get_stored_filenames()

            # self.status = "uploading"
            # storage = S3Storage()
            # storage.upload_file(self.file_metadata.file_path, self.file_metadata)

            # self.status = "registering"
            # self.file_metadata.register_as_captured()

            # self.status = "done"
        except:
            self.status = "failed"
            log(f"Error while restoring {self.file_metadata.file_path} \n" + traceback.format_exc(), "error", self.source.source_name)


    def get_stored_filenames(self) -> list[tuple]:
        """
        Returns array of tuples (file_path, modified_time) of the newest versions of all stored files.
        """

        db = connect_to_db()
        cursor = db.cursor()
        cursor.reset()

        cursor.execute(
            """
            SELECT file_path, MAX(modified_time) FROM changes GROUP BY file_path
            """
        , (self.file_path,))

        return cursor.fetchall()