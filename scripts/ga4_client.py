import os
import json
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    Dimension,
    Metric,
    RunRealtimeReportRequest,
    MinuteRange,
)
from google.oauth2 import service_account
import pandas as pd
from typing import Dict, List, Any
import logging
import re 

logger = logging.getLogger(__name__)

def to_snake(name: str) -> str:
    s = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.replace('.', '_').lower()

class GA4RealtimeClient:
    def __init__(self, property_id: str, credentials_path: str):
        self.property_id = property_id
        self.credentials_path = credentials_path
        self.client = self._initialize_client()
    
    def _initialize_client(self):
        """Initialize GA4 client with service account credentials"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/analytics.readonly']
            )
            return BetaAnalyticsDataClient(credentials=credentials)
        except Exception as e:
            logger.error(f"Failed to initialize GA4 client: {str(e)}")
            raise
    
    def get_realtime_active_users_by_page(self) -> pd.DataFrame:
        """Get active users by page (last 5 minutes)"""
        try:
            request = RunRealtimeReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[
                    Dimension(name="unifiedScreenName"),
                    Dimension(name="country"),
                ],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="screenPageViews"),
                ],
                minute_ranges=[
                    MinuteRange(
                        name="last_5_minutes",
                        start_minutes_ago=5,
                        end_minutes_ago=0
                    )
                ]
            )
            
            response = self.client.run_realtime_report(request)
            return self._parse_response_to_df(response, 'active_users_by_page')
            
        except Exception as e:
            logger.error(f"Error getting active users by page: {str(e)}")
            return pd.DataFrame()

    def get_realtime_events_by_page(self) -> pd.DataFrame:
        """Get event counts by page and event name (last 5 minutes)"""
        try:
            request = RunRealtimeReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[
                    Dimension(name="unifiedScreenName"),
                    Dimension(name="eventName"),
                ],
                metrics=[Metric(name="eventCount")],
                minute_ranges=[
                    MinuteRange(name="last_5_minutes", start_minutes_ago=5, end_minutes_ago=0)
                ],
            )
            response = self.client.run_realtime_report(request)
            return self._parse_response_to_df(response, "events_by_page")
        except Exception as e:
            logger.error(f"Error getting events by page: {str(e)}")
            return pd.DataFrame()
    
    def get_realtime_conversions(self) -> pd.DataFrame:
        try:
            request = RunRealtimeReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[Dimension(name="eventName")],
                metrics=[Metric(name="keyEvents")],  # eventCount + eventName causes 400 in realtime
                minute_ranges=[MinuteRange(name="last_5_minutes", start_minutes_ago=5, end_minutes_ago=0)]
            )
            response = self.client.run_realtime_report(request)
            return self._parse_response_to_df(response, 'conversions')
        except Exception as e:
            logger.error(f"Error getting conversions: {str(e)}")
            return pd.DataFrame()
    
    def get_realtime_traffic_sources(self) -> pd.DataFrame:
        """
        Try several realtime-supported dims; map whatever we get into
        columns: source, medium, campaign, active_users, report_type, extracted_at
        """
        candidates = [
            # Preferred dimensions, often unavailable in realtime API
            ["sessionSource", "sessionMedium", "sessionCampaign"],
            ["sessionSource", "sessionMedium"],
            # single-dimension fallbacks that usually work in realtime
            ["deviceCategory"],
            ["country"],
        ]
        for dims in candidates:
            try:
                req = RunRealtimeReportRequest(
                    property=f"properties/{self.property_id}",
                    dimensions=[Dimension(name=d) for d in dims],
                    metrics=[Metric(name="activeUsers")],
                    minute_ranges=[MinuteRange(name="last_5_minutes", start_minutes_ago=5, end_minutes_ago=0)]
                )
                resp = self.client.run_realtime_report(req)
                df = self._parse_response_to_df(resp, "traffic_sources")
                if df.empty:
                    continue

                # Normalize into expected table columns
                df = df.rename(columns={
                    "session_source": "source",
                    "session_medium": "medium",
                    "session_campaign": "campaign",
                    # Handle fallback cases
                    "device_category": "source",
                    "country": "source"
                })

                # Ensure required columns exist
                for col in ["source", "medium", "campaign"]:
                    if col not in df.columns:
                        df[col] = "(not set)"

                # Retain only expected columns
                keep = ["source", "medium", "campaign", "active_users", "report_type", "extracted_at"]
                return df[keep]
            except Exception as e:
                logger.warning(f"Traffic sources candidate {dims} failed: {e}")
                continue

        logger.info("No traffic source candidate returned data")
        return pd.DataFrame()
    
    def get_realtime_overview(self) -> pd.DataFrame:
        """Get overall realtime overview metrics"""
        try:
            request = RunRealtimeReportRequest(
                property=f"properties/{self.property_id}",
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="screenPageViews"),
                    Metric(name="keyEvents"),
                    Metric(name="eventCount"),
                ],
                minute_ranges=[
                    MinuteRange(
                        name="last_5_minutes",
                        start_minutes_ago=5,
                        end_minutes_ago=0
                    )
                ]
            )
            
            response = self.client.run_realtime_report(request)
            return self._parse_response_to_df(response, 'overview')
            
        except Exception as e:
            logger.error(f"Error getting overview: {str(e)}")
            return pd.DataFrame()
    
    def _parse_response_to_df(self, response, report_type: str) -> pd.DataFrame:
        try:
            if not response.rows:
                logger.info(f"No data returned for {report_type}")
                return pd.DataFrame()

            dim_headers = [to_snake(h.name) for h in response.dimension_headers]
            metric_headers = [to_snake(h.name) for h in response.metric_headers]

            rows = []
            for row in response.rows:
                d = {}
                for i, dim_value in enumerate(row.dimension_values):
                    d[dim_headers[i]] = dim_value.value or '(not set)'
                for i, metric_value in enumerate(row.metric_values):
                    d[metric_headers[i]] = float(metric_value.value) if metric_value.value else 0.0
                d['report_type'] = report_type
                d['extracted_at'] = pd.Timestamp.now()
                rows.append(d)

            df = pd.DataFrame(rows)
            logger.info(f"Successfully parsed {len(df)} rows for {report_type}")
            return df
        except Exception as e:
            logger.error(f"Error parsing response for {report_type}: {str(e)}")
            return pd.DataFrame()