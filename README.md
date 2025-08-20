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

## Setup instructions
### Part I. Cloning Repo


First, I recommend that you clone this repository to your local machine:

```bash
git clone https://github.com/RuslanKhis/realtime-ga4-page-tracker.git
cd realtime-ga4-page-tracker
```

Please also create secrets folder where we expect to store our service account keys:

```bash
touch secrets
```

### Part II. Getting keys to GA4 

#### Check prerequisites
Before we can even start getting data from GA4 programmatically, we need to, so to speak, build a funnel to use (enable the API) and get keys (API Keys). To make sure that you can perform the necessary actions as outlined below, please check that you have the following:

- **Google Cloud Project (GCP):** A GCP project provides the framework to access Google’s cloud services. You will need an active GCP project to create the necessary resources for connecting to the GA4 API. Users with basic IAM permissions (like the default “Basic Editor” role) can create service accounts.
- **Access to a Google Analytics 4 Property (Viewer or Editor permissions):** You must be able to add a service account’s email address to the GA4 Property settings and grant it the necessary permissions (“Viewer” or “Editor”).

#### GCP Setup
Ok, with the prerequisites set up, please proceed to do the following:

Enable **Google Analytics Data API**:

1. **Project Dashboard:** Go to your Google Cloud Project dashboard.
2. **APIs & Services:** Navigate to the "APIs & Services" section.
3. **Search and Enable:** Search for "Google Analytics Data API" and click "Enable".

Create **Service Account**:

1. **APIs & Services -> Credentials:** Go back to the "Credentials" subsection within "APIs & Services" in your GCP project dashboard.

2. **+ Create Credentials:** Click on "+ Create Credentials" and choose "Service Account".

3. **Service Account Details:** Fill in a descriptive name for your service account (e.g., "ga4-reporting-api"). You can also add a brief description if desired. Click on "Create and Continue."

4. **Other Details:** You can leave "Grant this Service Account Access to Project" and "Grant Users Access to this Service Account" empty and just proced to button "Done."

5. **Download JSON Key:** In your GCP Project, navigate to the "IAM & Admin" section and then select "Service Accounts". Locate the service account you just created (e.g., "ga4-reporting-api"). Click on the "Keys" tab within your service account's details. Click on the "Add Key" button, choose "Create New Key", and select the "JSON" format. The JSON key file will be downloaded to your local computer. Name your file `ga4SAK.json` and put it in the `secrets` folder of our solution.


#### Google Analytics Setup

Ok, so now we have built the funnel to get data from GA4, we need to give it keys to access our GA4 Property:

Grant **Service Account** Access to **GA4 Property**:

1. **Find Your Service Account Email:** Open the JSON key file you downloaded earlier when creating your service account in GCP.  Locate the "client_email" field and copy the email address. This is the unique identifier for your service account.
2. **Navigate to Your GA4 Property:** Go to the Google Analytics platform and access the specific GA4 property where you want to extract data.
3. **Admin Settings:** Within your GA4 property, navigate to the "Admin" settings.
4. **Property Access Management:** Look for a section labeled "Property Access Management". This is where you'll add new users.
5. **+ Add Member:** Click the button labeled "+ Add Member" to add a new member (your service account).
6. **Paste Email and Set Permissions:** Paste the service account's email address you copied earlier.  Since you just need to read data, grant the "Viewer" permission level. 
7. **Save:** Click "Save" to finalize granting the necessary permissions to your service account.

In addition, please make sure to go to `Admin` -> `Property` -> `Property Details` and save the `PROPERTY ID` provided in the top right corner. We will need it later to specify to the GA4 Data API for which property we want to get data.



You need to run the command below from the main folder of this repo to make the main scripts executable.
```bash
chmod +x scripts/*.sh
```

Then, you can run the following command to start the application:
```bash
./scripts/start-project.sh
```

