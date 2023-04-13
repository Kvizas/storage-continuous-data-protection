import mysql.connector
import mysql.connector.connection_cext
import os

from datetime import datetime

def connect_to_db() -> mysql.connector.connection_cext.CMySQLConnection:
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE")
    )

def setup_tables() -> None:
    db = connect_to_db()
    cursor = db.cursor()

    # Logs table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS logs
        (
            `id` int(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
            `type` varchar(8) NOT NULL,
            `message` varchar(3072) NOT NULL,
            `source` varchar(64) NOT NULL,
            `datetime` datetime NOT NULL DEFAULT current_timestamp(),
            `file_path` varchar(255)
        ) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    )

    # Changes table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS changes
        (
            `id` int(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
            `file_path` varchar(255) NOT NULL,
            `mime_type` varchar(255) NOT NULL,
            `source` varchar(64) NOT NULL,
            `modified_by` varchar(64),
            `modified_time` int(11) UNSIGNED NOT NULL,
            `size` int(11) NOT NULL,
            `backup_datetime` datetime NOT NULL DEFAULT current_timestamp(),
            `md5_checksum` varchar(32)
        ) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        CREATE INDEX file_path ON changes (file_path);
        CREATE INDEX modified_by ON changes (modified_by);
        """
    )