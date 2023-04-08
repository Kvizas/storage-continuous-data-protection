from database import connect_to_db

class FileMetadata:

    file_path: str = None
    mime_type: str = None
    source: str = None
    modified_by: str = None
    modified_time: int = None
    size: int = None
    md5_checksum: str = None
        
    def register_as_captured(self) -> None:
        """ Inserts FileMetadata into SQL as captured change. """

        db = connect_to_db()
        cursor = db.cursor()

        cursor.execute(
            """
            INSERT INTO changes 
            (file_path, mime_type, source, modified_by, modified_time, size, md5_checksum)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s)
            """
        , (
            self.file_path,
            self.mime_type,
            self.source,
            self.modified_by,
            self.modified_time,
            self.size,
            self.md5_checksum,
        ))
        
        db.commit()

    def is_captured(self) -> bool:
        """ Returns True if the file in backup storage is newer or the same as this one. """
        
        db = connect_to_db()
        cursor = db.cursor()
        cursor.reset()

        cursor.execute(
            """
            SELECT t1.modified_time, md5_checksum FROM changes t1 JOIN (
                SELECT MAX(modified_time) as modified_time FROM changes WHERE file_path=%s
            ) t2 ON t1.modified_time = t2.modified_time ORDER BY id desc LIMIT 1
            """
        , (self.file_path,))

        row = cursor.fetchone()

        if row == None:
            return False

        max_modified_time = int(row[0])
        md5_checksum = row[1]

        if not md5_checksum:
            return max_modified_time >= self.modified_time

        return (
                max_modified_time >= self.modified_time
            or
                md5_checksum == self.md5_checksum
            )

    def set_id(self, value: str) -> None:
        self.id: str = value

    def set_file_path(self, value: str) -> None:
        self.file_path: str = value

    def set_mime_type(self, value: str) -> None:
        self.mime_type: str = value

    def set_source(self, value: str) -> None:
        self.source: str = value

    def set_modified_time(self, value: str) -> None:
        """
        Parses modified date into %y%m%d%H%M format.

        Parameters:
        value (string): Datetime in ISO8601 format
        """

        from datetime import datetime
        modified_time = datetime.fromisoformat(value[:-1])
        self.modified_time: int = int(modified_time.strftime("%y%m%d%H%M"))

    def set_modified_by(self, value: str) -> None:
        self.modified_by: str = value

    def set_size(self, value: int) -> None:
        self.size: int = value

    def set_md5_checksum(self, value: str) -> None:
        self.md5_checksum: str = value