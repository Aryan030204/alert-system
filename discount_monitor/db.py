# =============================================================
# db.py — DB connection + ensure alerts table exists
# =============================================================

import mysql.connector
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD


def get_connection(db_name: str):
    """Return a MySQL connection for a given brand DB."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=db_name,
    )


def ensure_alerts_table(conn):
    """Create discount_alerts table if it doesn't already exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS discount_alerts (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            alert_time      DATETIME,
            brand           VARCHAR(50),
            alert_type      VARCHAR(50),
            current_value   DECIMAL(10,2),
            baseline_value  DECIMAL(10,2),
            delta_pct       DECIMAL(10,2),
            message         TEXT
        )
    """)
    conn.commit()
    cursor.close()
