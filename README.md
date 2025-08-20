# realtime-ga4-page-tracker
## Core-problem
While Google Analytics 4 has quite a powerful UI, it unfortunately has some limitations. One such limitation occurs when marketers want to track the performance of a specific page in real time. Take, for example, yet another iPhone launch. Usually, mobile providers have dedicated pages where you can pre-order an iPhone. Marketers often want to know in real time how many people are visiting the site, how many page views they have, and how many conversions they have. This type of information is not easy to get via the GA4 UI because, while you can get the number of active users in the last 30 or 5 minutes, it will show you the number for the entire site and not a specific page. You can get the number of page views; however, there is no easy way to filter the page views related to the page that interests you the most.

Of course, one could argue that we can use GA4 Explorations or create a dashboard in Looker Studio. This, however, is not a solution, since the data there will inevitably be either aggregated for the whole day or processed for the past day, which is not what we want. Thus, to overcome these issues, I have created this application that allows users to get real-time data specific to their needs.

## Solution
The solution leverages the GA4 Realtime Data API to extract page-level analytics data that the GA4 UI doesn't provide. We built an automated ETL pipeline that runs every five minutes, collecting active users, page views, events, and conversions, broken down by specific pages and countries. The data is stored in PostgreSQL and visualized through Metabase dashboards, giving marketers real-time insights into individual page performance during critical campaigns.

## Tech stack
I have created this solution primarily utilizing the following tools:  
1. Google Analytics Data API to retrieve data  
2. Apache Airflow via Astro to create a pipeline that fetches data  
3. PostgreSQL to store data  
4. Metabase to visualize the data  
5. Docker to run Apache Airflow

## Architecture
This solution creates a real-time analytics data pipeline architecture using a containerized microservices approach. We use Apache Airflow to orchestrate data collection from the GA4 Data API every five minutes, storing results in PostgreSQL for analysis in a Metabase dashboard.

Our Airflow DAG has the following structure:
![Dag flow](images/DAG%20Flow.png)

My architecture follows the ETL pattern: we extract data from the GA4 Data API, transform it using a Pandas DataFrame with snake_case formatting and metadata enrichment, and then load the normalized data into PostgreSQL tables.

The pipeline runs in a Docker container, with Airflow handling orchestration. PostgreSQL provides data persistence, and Metabase offers real-time data visualization. The solution captures active users, page views, events, conversions, and traffic sources at the page level—which the GA4 UI cannot currently provide.

The most important components are the GA4 client for API interactions, a PostgreSQL handler for database operations, and an Airflow DAG that coordinates the entire workflow. I have introduced data freshness control via automated cleanup. The solution also creates aggregated views for dashboard consumption, enabling marketers to monitor specific page performance during critical events like product launches (e.g., yet another iPhone launch).

## How to start using it



You need to run the command below from the main folder of this repo to make the main scripts executable.
```bash
chmod +x scripts/*.sh
```

Then, you can run the following command to start the application:
```bash
./scripts/start-project.sh
```

