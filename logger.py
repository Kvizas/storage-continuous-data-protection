from database import connect_to_db
from datetime import datetime


def log(message, log_type="DEBUG", source="INTERNAL"):
    """
    Logs message into SQL logs table
    """
    log_type = log_type.upper()
    source = source.upper()

    now = datetime.now()

    print(f"[{now}] {log_type} / {source} - " + message)

    db = connect_to_db()
    cursor = db.cursor()

    cursor.execute("""
    INSERT INTO logs (type, message, source, datetime) VALUES (%s, %s, %s, %s)
    """, (log_type, message, source, now.isoformat()))