import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/error_logs.db")
RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))

logger = logging.getLogger(__name__)

print(DATABASE_PATH)


class LiteDb:
    def __init__(self, db_path=DATABASE_PATH) -> None:
        self.db_path = db_path
        self.local = threading.local()
        self.init_db()

    @contextmanager
    def get_connection(self):
        """Thread-safe connection pooling using thread-local storage"""
        if not hasattr(self.local, "conn") or self.local.conn is None:
            try:
                self.local.conn = sqlite3.connect(
                    self.db_path,
                    timeout=10.0,  # Wait up to 10 seconds for locks
                    check_same_thread=False,
                )
                # Enable WAL mode for better concurrency
                self.local.conn.execute("PRAGMA journal_mode=WAL")
                # Optimize for performance
                self.local.conn.execute("PRAGMA synchronous=NORMAL")
            except sqlite3.Error as e:
                logger.error(f"Failed to connect to database: {e}")
                raise

        try:
            yield self.local.conn
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            self.local.conn.rollback()
            raise
        else:
            self.local.conn.commit()

    def close_connection(self):
        """Close thread-local connection (call on thread cleanup)"""
        if hasattr(self.local, "conn") and self.local.conn:
            self.local.conn.close()
            self.local.conn = None

    def init_db(self):
        """Initialize SQLite database with proper indexes"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Create main error logs table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    container_name TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    error_hash TEXT,
                    occurrence_count INTEGER DEFAULT 1,
                    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Composite index for common queries
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_container_timestamp 
                ON error_logs(container_name, timestamp DESC)
            """
            )

            # Index for cleanup operations
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON error_logs(timestamp)
            """
            )

            # Index for search operations
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_error_message 
                ON error_logs(error_message)
            """
            )

            # Index for deduplication
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_error_hash 
                ON error_logs(container_name, error_hash)
            """
            )

            logger.info("Database initialized successfully")

    def save_error(self, container_name: str, error_message: str):
        """Save single error to database with deduplication"""
        if not error_message or not container_name:
            return

        error_hash = str(hash(error_message))

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Check if same error occurred in last 60 seconds (deduplication)
            cursor.execute(
                """
                SELECT id, occurrence_count FROM error_logs 
                WHERE container_name = ? 
                AND error_hash = ? 
                AND datetime(last_seen) > datetime('now', '-60 seconds')
                ORDER BY last_seen DESC LIMIT 1
            """,
                (container_name, error_hash),
            )

            existing = cursor.fetchone()

            if existing:
                # Update existing error count and timestamp
                cursor.execute(
                    """
                    UPDATE error_logs 
                    SET occurrence_count = occurrence_count + 1,
                        last_seen = CURRENT_TIMESTAMP
                    WHERE id = ?
                """,
                    (existing[0],),
                )
                logger.debug(
                    f"Updated error count for {container_name}: {existing[1] + 1}"
                )
            else:
                # Insert new error
                cursor.execute(
                    """
                    INSERT INTO error_logs (container_name, error_message, error_hash)
                    VALUES (?, ?, ?)
                """,
                    (container_name, error_message, error_hash),
                )
                logger.debug(f"Inserted new error for {container_name}")

    def save_errors_batch(self, errors: list[tuple[str, str]]):
        """Batch insert errors for better performance"""
        if not errors:
            return

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO error_logs (container_name, error_message, error_hash)
                VALUES (?, ?, ?)
            """,
                [(name, msg, str(hash(msg))) for name, msg in errors],
            )
            logger.info(f"Batch inserted {len(errors)} errors")

    def get_errors(
        self,
        container_name: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ):
        """Get errors with filtering, search and pagination"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            query = """
                SELECT id, container_name, error_message, occurrence_count, 
                       first_seen, last_seen, timestamp 
                FROM error_logs
            """
            count_query = "SELECT COUNT(*) FROM error_logs"
            params = []

            conditions = []
            if container_name:
                conditions.append("container_name = ?")
                params.append(container_name)

            if search:
                conditions.append("error_message LIKE ?")
                params.append(f"%{search}%")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
                query += where_clause
                count_query += where_clause

            query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
            count_params = params.copy()
            params.extend([limit, offset])

            # Get total count
            cursor.execute(count_query, count_params)
            total_count = cursor.fetchone()[0]

            # Get paginated results
            cursor.execute(query, params)
            results = cursor.fetchall()

            return {
                "errors": [
                    {
                        "id": row[0],
                        "container_name": row[1],
                        "error_message": row[2],
                        "occurrence_count": row[3],
                        "first_seen": row[4],
                        "last_seen": row[5],
                        "timestamp": row[6],
                    }
                    for row in results
                ],
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "has_next": (offset + limit) < total_count,
                    "has_prev": offset > 0,
                },
            }

    def cleanup_old_logs(self, days: int = RETENTION_DAYS):
        """Remove logs older than specified days"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cutoff_date = datetime.now() - timedelta(days=days)

                cursor.execute(
                    """
                    DELETE FROM error_logs 
                    WHERE datetime(timestamp) < datetime(?)
                """,
                    (cutoff_date.isoformat(),),
                )

                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} old log entries")

                # Vacuum to reclaim space (optional, can be expensive)
                cursor.execute("VACUUM")

                return deleted_count
        except sqlite3.Error as e:
            logger.error(f"Error during cleanup: {e}")
            return 0

    def get_error_summary(self, container_name: str | None = None, hours: int = 24):
        """Get error summary statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            query = """
                SELECT 
                    container_name,
                    COUNT(*) as error_count,
                    COUNT(DISTINCT error_hash) as unique_errors,
                    SUM(occurrence_count) as total_occurrences,
                    MAX(timestamp) as last_error
                FROM error_logs
                WHERE datetime(timestamp) > datetime('now', '-' || ? || ' hours')
            """
            params = [hours]

            if container_name:
                query += " AND container_name = ?"
                params.append(container_name)

            query += " GROUP BY container_name ORDER BY error_count DESC"

            cursor.execute(query, params)
            results = cursor.fetchall()

            return [
                {
                    "container_name": row[0],
                    "error_count": row[1],
                    "unique_errors": row[2],
                    "total_occurrences": row[3],
                    "last_error": row[4],
                }
                for row in results
            ]

    def get_database_stats(self):
        """Get database size and row count"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("SELECT COUNT(*) FROM error_logs")
                total_rows = cursor.fetchone()[0]

                # Get database file size
                db_size = (
                    os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                )

                return {
                    "total_rows": total_rows,
                    "db_size_mb": round(db_size / (1024 * 1024), 2),
                    "db_path": self.db_path,
                }
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {"error": str(e)}
