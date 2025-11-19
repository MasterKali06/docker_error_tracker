import sqlite3


class LiteDb:
    def __init__(self, db_path="error_logs.db") -> None:
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """Initialize SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_name TEXT NOT NULL,
                error_message TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_container_name 
            ON error_logs(container_name)
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_timestamp 
            ON error_logs(timestamp)
        """
        )
        conn.commit()
        conn.close()

    def save_error(self, container_name, error_message):
        """Save single error to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO error_logs (container_name, error_message)
            VALUES (?, ?)
        """,
            (container_name, error_message),
        )
        conn.commit()
        conn.close()

    def get_errors(self, container_name=None, search=None, limit=50, offset=0):
        """Get errors with filtering, search and pagination"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = "SELECT * FROM error_logs"
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
        params.extend([limit, offset])

        # Get total count
        cursor.execute(count_query, params[:-2])  # Exclude limit/offset for count
        total_count = cursor.fetchone()[0]

        # Get paginated results
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()

        return {
            "errors": [
                {
                    "id": row[0],
                    "container_name": row[1],
                    "error_message": row[2],
                    "timestamp": row[3],
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
