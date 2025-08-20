from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os
import logging
from airflow.utils.dates import days_ago



from scripts.ga4_client import GA4RealtimeClient
from scripts.postgres_handler import PostgreSQLHandler

# ========================
# Configuration
# ========================
GA4_PROPERTY_ID = "NNNNNNNNNNN"  # Replace with your actual property ID
CREDENTIALS_PATH = "secrets/ga4SAK.json"  # Path to your service account key file

# Default arguments for all tasks
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False
}

# ========================
# DAG Definition
# ========================
dag = DAG(
    'ga4_realtime_pipeline',
    default_args=default_args,
    description='GA4 Realtime Data Pipeline - Extract and Store Analytics Data',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['ga4', 'realtime', 'analytics', 'postgres'],
)

# ========================
# Task Functions
# ========================

def check_credentials(**context):
    """Check if GA4 credentials file exists and is accessible"""
    try:
        if not os.path.exists(CREDENTIALS_PATH):
            raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_PATH}")
        
        # Test GA4 client initialization
        client = GA4RealtimeClient(GA4_PROPERTY_ID, CREDENTIALS_PATH)
        logging.info("GA4 credentials verified successfully")
        
        return {"status": "success", "message": "Credentials verified"}
        
    except Exception as e:
        logging.error(f"Credentials check failed: {str(e)}")
        raise

def extract_active_users_data(**context):
    """Extract active users by page data from GA4"""
    try:
        client = GA4RealtimeClient(GA4_PROPERTY_ID, CREDENTIALS_PATH)
        df = client.get_realtime_active_users_by_page()
        
        if not df.empty:
            db_handler = PostgreSQLHandler()  
            db_handler.insert_dataframe(df, 'active_users_by_page')
            
            logging.info(f"Successfully processed {len(df)} active users records")
            return {"status": "success", "records": len(df), "table": "active_users_by_page"}
        else:
            logging.info("No active users data found")
            return {"status": "no_data", "records": 0, "table": "active_users_by_page"}
            
    except Exception as e:
        logging.error(f"Error extracting active users data: {str(e)}")
        raise

def extract_events_by_page_data(**context):
    """Extract event counts by page from GA4"""
    try:
        client = GA4RealtimeClient(GA4_PROPERTY_ID, CREDENTIALS_PATH)
        df = client.get_realtime_events_by_page()
        
        if not df.empty:
            db_handler = PostgreSQLHandler()
            db_handler.insert_dataframe(df, 'events_by_page')
            
            logging.info(f"Successfully processed {len(df)} event records")
            return {"status": "success", "records": len(df), "table": "events_by_page"}
        else:
            logging.info("No event data found")
            return {"status": "no_data", "records": 0, "table": "events_by_page"}
            
    except Exception as e:
        logging.error(f"Error extracting events by page data: {str(e)}")
        raise

def extract_conversions_data(**context):
    """Extract conversions data from GA4"""
    try:
        client = GA4RealtimeClient(GA4_PROPERTY_ID, CREDENTIALS_PATH)
        df = client.get_realtime_conversions()
        
        if not df.empty:
            db_handler = PostgreSQLHandler()
            db_handler.insert_dataframe(df, 'conversions')
            
            logging.info(f"Successfully processed {len(df)} conversion records")
            return {"status": "success", "records": len(df), "table": "conversions"}
        else:
            logging.info("No conversions data found")
            return {"status": "no_data", "records": 0, "table": "conversions"}
            
    except Exception as e:
        logging.error(f"Error extracting conversions data: {str(e)}")
        raise

def extract_traffic_sources_data(**context):
    """Extract traffic sources data from GA4"""
    try:
        client = GA4RealtimeClient(GA4_PROPERTY_ID, CREDENTIALS_PATH)
        df = client.get_realtime_traffic_sources()
        
        if not df.empty:
            db_handler = PostgreSQLHandler()
            db_handler.insert_dataframe(df, 'traffic_sources')
            
            logging.info(f"Successfully processed {len(df)} traffic source records")
            return {"status": "success", "records": len(df), "table": "traffic_sources"}
        else:
            logging.info("No traffic sources data found")
            return {"status": "no_data", "records": 0, "table": "traffic_sources"}
            
    except Exception as e:
        logging.error(f"Error extracting traffic sources data: {str(e)}")
        raise

def extract_overview_data(**context):
    """Extract overview metrics from GA4"""
    try:
        client = GA4RealtimeClient(GA4_PROPERTY_ID, CREDENTIALS_PATH)
        df = client.get_realtime_overview()
        
        if not df.empty:
            db_handler = PostgreSQLHandler()
            db_handler.insert_dataframe(df, 'overview')
            
            logging.info(f"Successfully processed {len(df)} overview records")
            return {"status": "success", "records": len(df), "table": "overview"}
        else:
            logging.info("No overview data found")
            return {"status": "no_data", "records": 0, "table": "overview"}
            
    except Exception as e:
        logging.error(f"Error extracting overview data: {str(e)}")
        raise

def create_aggregated_views(**context):
    """Create aggregated views for Metabase dashboards"""
    try:
        db_handler = PostgreSQLHandler()
        db_handler.create_aggregated_views()
        
        logging.info("Aggregated views created successfully")
        return {"status": "success", "message": "Views created"}
        
    except Exception as e:
        logging.error(f"Error creating aggregated views: {str(e)}")
        raise

def cleanup_old_data(**context):
    """Clean up data older than 24 hours"""
    try:
        db_handler = PostgreSQLHandler()
        db_handler.cleanup_old_data(hours_to_keep=24)
        
        logging.info("Old data cleanup completed")
        return {"status": "success", "message": "Cleanup completed"}
        
    except Exception as e:
        logging.error(f"Error during cleanup: {str(e)}")
        raise

def generate_pipeline_summary(**context):
    """Generate summary of pipeline execution"""
    try:
        db_handler = PostgreSQLHandler()
        stats = db_handler.get_summary_stats()
        
        logging.info("Pipeline Summary:")
        for table, table_stats in stats.items():
            if 'error' not in table_stats:
                logging.info(f"  {table}: {table_stats['total_records']} records, "
                           f"latest: {table_stats['latest_update']}")
            else:
                logging.warning(f"  {table}: {table_stats['error']}")
        
        return {"status": "success", "stats": stats}
        
    except Exception as e:
        logging.error(f"Error generating summary: {str(e)}")
        raise

def check_database_status(**context):
    """Check database status for debugging"""
    try:
        db_handler = PostgreSQLHandler()
        status = db_handler.check_database_status()
        
        logging.info(f"Database status: {status}")
        return status
        
    except Exception as e:
        logging.error(f"Error checking database status: {str(e)}")
        raise

# ========================
# Task Definitions
# ========================

start_task = DummyOperator(task_id='start_pipeline', dag=dag)

check_creds_task = PythonOperator(
    task_id='check_credentials', python_callable=check_credentials, dag=dag
)

extract_active_users_task = PythonOperator(
    task_id='extract_active_users', python_callable=extract_active_users_data, dag=dag
)

extract_events_by_page_task = PythonOperator(
    task_id='extract_events_by_page', python_callable=extract_events_by_page_data, dag=dag
)

extract_conversions_task = PythonOperator(
    task_id='extract_conversions', python_callable=extract_conversions_data, dag=dag
)

extract_traffic_sources_task = PythonOperator(
    task_id='extract_traffic_sources', python_callable=extract_traffic_sources_data, dag=dag
)

extract_overview_task = PythonOperator(
    task_id='extract_overview', python_callable=extract_overview_data, dag=dag
)

create_views_task = PythonOperator(
    task_id='create_aggregated_views',
    python_callable=create_aggregated_views,
    dag=dag,
    trigger_rule='none_failed_min_one_success',
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag,
    trigger_rule='all_done',
)

summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_pipeline_summary,
    dag=dag,
    trigger_rule='all_done',
)

end_task = DummyOperator(
    task_id='end_pipeline', dag=dag, trigger_rule='all_done'
)

check_db_status_task = PythonOperator(
    task_id='check_database_status', python_callable=check_database_status, dag=dag
)

# ========================
# Task Dependencies
# ========================

start_task >> check_creds_task

check_creds_task >> [
    extract_active_users_task,
    extract_events_by_page_task, 
    extract_conversions_task, 
    extract_traffic_sources_task,
    extract_overview_task
]

[
    extract_active_users_task,
    extract_events_by_page_task, 
    extract_conversions_task,
    extract_traffic_sources_task,
    extract_overview_task
] >> create_views_task

create_views_task >> cleanup_task >> check_db_status_task >> summary_task >> end_task