from database import connect_to_db
from datetime import datetime


def log(message, log_type="DEBUG", source="INTERNAL", file_path=None):
    """
    Logs message into SQL logs table
    """
    log_type = log_type.upper()
    source = source.upper()

    now = datetime.now()

    db = connect_to_db()
    cursor = db.cursor()

    cursor.execute("""
    INSERT INTO logs (type, message, source, datetime, file_path) VALUES (%s, %s, %s, %s, %s)
    """, (log_type, message, source, now.isoformat(), file_path))

    db.commit()