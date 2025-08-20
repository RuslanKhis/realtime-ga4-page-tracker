import logging
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class PostgreSQLHandler:
    def __init__(
        self,
        host: str = "host.docker.internal",  # matches your Metabase connection
        port: int = 5432,
        database: str = "postgres",
        username: str = "postgres",
        password: str = "postgres",
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.connection_string)
        self.tables_to_manage = [
            "active_users_by_page",
            "events_by_page",
            "conversions",
            "traffic_sources",
            "overview",
        ]
        self.init_tables()

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password,
        )

    def init_tables(self):
        """Initialize PostgreSQL tables (snake_case columns)."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Active users by page
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS active_users_by_page (
                    id SERIAL PRIMARY KEY,
                    unified_screen_name VARCHAR(500),
                    country VARCHAR(100),
                    active_users DOUBLE PRECISION,
                    screen_page_views DOUBLE PRECISION,
                    report_type VARCHAR(100),
                    extracted_at TIMESTAMP,
                    created_date DATE GENERATED ALWAYS AS (DATE(extracted_at)) STORED
                )
                """
            )
            
            # NEW: Events by page
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS events_by_page (
                    id SERIAL PRIMARY KEY,
                    unified_screen_name VARCHAR(500),
                    event_name VARCHAR(500),
                    event_count DOUBLE PRECISION,
                    report_type VARCHAR(100),
                    extracted_at TIMESTAMP,
                    created_date DATE GENERATED ALWAYS AS (DATE(extracted_at)) STORED
                )
                """
            )

            # Conversions (event_name + key_events for realtime)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS conversions (
                    id SERIAL PRIMARY KEY,
                    event_name VARCHAR(500),
                    key_events DOUBLE PRECISION,
                    report_type VARCHAR(100),
                    extracted_at TIMESTAMP,
                    created_date DATE GENERATED ALWAYS AS (DATE(extracted_at)) STORED
                )
                """
            )

            # Traffic sources (realtime-supported fields)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS traffic_sources (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(500),
                    medium VARCHAR(500),
                    campaign VARCHAR(500),
                    active_users DOUBLE PRECISION,
                    report_type VARCHAR(100),
                    extracted_at TIMESTAMP,
                    created_date DATE GENERATED ALWAYS AS (DATE(extracted_at)) STORED
                )
                """
            )

            # Overview metrics (store both)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS overview (
                    id SERIAL PRIMARY KEY,
                    active_users DOUBLE PRECISION,
                    screen_page_views DOUBLE PRECISION,
                    key_events DOUBLE PRECISION,
                    event_count DOUBLE PRECISION,
                    report_type VARCHAR(100),
                    extracted_at TIMESTAMP,
                    created_date DATE GENERATED ALWAYS AS (DATE(extracted_at)) STORED
                )
                """
            )

            # Indexes
            for t in self.tables_to_manage:
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{t}_extracted_at ON {t}(extracted_at);"
                )

            conn.commit()
            logger.info("All tables initialized successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error initializing tables: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        """Insert DataFrame into table. Assumes snake_case columns that match DDL."""
        if df.empty:
            logger.info(f"No data to insert for {table_name}")
            return
        try:
            df.to_sql(
                table_name, self.engine, if_exists="append", index=False, method="multi"
            )
            logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {str(e)}")
            raise

    def get_latest_data(self, table_name: str, limit: int = 100) -> pd.DataFrame:
        try:
            query = f"""
                SELECT * FROM {table_name}
                ORDER BY extracted_at DESC
                LIMIT {limit}
            """
            result = pd.read_sql(query, self.engine)
            logger.info(f"Retrieved {len(result)} rows from {table_name}")
            return result
        except Exception as e:
            logger.error(f"Error querying {table_name}: {str(e)}")
            return pd.DataFrame()

    def check_database_status(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
            tables = cursor.fetchall()
            logger.info(f"Available tables: {tables}")

            stats = {}
            for (table_name,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                stats[table_name] = count

            logger.info(f"Table statistics: {stats}")
            return {"status": "ok", "tables": stats}
        except Exception as e:
            logger.error(f"Error checking database: {str(e)}")
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()
            conn.close()

    def get_summary_stats(self) -> dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        stats = {}
        try:
            for table in self.tables_to_manage:
                try:
                    cursor.execute(
                        f"""
                        SELECT
                            COUNT(*) as total_records,
                            MAX(extracted_at) as latest_update,
                            MIN(extracted_at) as earliest_record
                        FROM {table}
                        """
                    )
                    result = cursor.fetchone()
                    stats[table] = {
                        "total_records": result[0],
                        "latest_update": result[1],
                        "earliest_record": result[2],
                    }
                except Exception as table_error:
                    logger.warning(
                        f"Could not get stats for {table}: {str(table_error)}"
                    )
                    stats[table] = {"error": str(table_error)}
            return stats
        except Exception as e:
            logger.error(f"Error getting summary stats: {str(e)}")
            return {}
        finally:
            cursor.close()
            conn.close()

    def cleanup_old_data(self, hours_to_keep: int = 24):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            for table in self.tables_to_manage:
                try:
                    cursor.execute(
                        f"""
                        DELETE FROM {table}
                        WHERE extracted_at < NOW() - INTERVAL '{hours_to_keep} hours'
                        """
                    )
                    logger.info(f"Cleaned up old data from {table}")
                except Exception as table_error:
                    logger.error(f"Error cleaning up {table}: {str(table_error)}")
            conn.commit()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def create_aggregated_views(self):
        conn = self.get_connection(); cursor = conn.cursor()
        try:
            cursor.execute("DROP VIEW IF EXISTS hourly_metrics;")
            cursor.execute("DROP VIEW IF EXISTS realtime_dashboard;")

            # Hourly metrics across sources
            cursor.execute("""
                CREATE VIEW hourly_metrics AS
                SELECT
                DATE_TRUNC('hour', extracted_at) AS hour,
                report_type,
                SUM(active_users)      AS total_active_users,
                SUM(screen_page_views) AS total_page_views,
                SUM(key_events)        AS total_conversions
                FROM (
                -- pages: has active_users, screen_page_views
                SELECT extracted_at, report_type,
                        active_users,
                        screen_page_views,
                        0::double precision AS key_events
                FROM active_users_by_page
                UNION ALL
                -- conversions: has key_events
                SELECT extracted_at, report_type,
                        0::double precision AS active_users,
                        0::double precision AS screen_page_views,
                        COALESCE(key_events,0) AS key_events
                FROM conversions
                UNION ALL
                -- traffic: has active_users
                SELECT extracted_at, report_type,
                        active_users,
                        0::double precision AS screen_page_views,
                        0::double precision AS key_events
                FROM traffic_sources
                ) x
                GROUP BY 1,2
                ORDER BY hour DESC
            """)

            # Realtime page view
            cursor.execute("""
                CREATE VIEW realtime_dashboard AS
                SELECT
                unified_screen_name AS page,
                country,
                SUM(active_users)      AS active_users,
                SUM(screen_page_views) AS page_views,
                MAX(extracted_at)      AS extracted_at
                FROM active_users_by_page
                WHERE extracted_at >= NOW() - INTERVAL '30 minutes'
                GROUP BY unified_screen_name, country
                ORDER BY extracted_at DESC, active_users DESC
            """)

            conn.commit()
            logger.info("Aggregated views created successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating aggregated views: {str(e)}")
        finally:
            cursor.close(); conn.close()