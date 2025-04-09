## Reddit ELI5 Data Analytics Platform: Automated Daily Insights from Community Discussion
### Overview
This project builds a complete end-to-end data pipeline to extract, transform, and visualize data from the [r/explainlikeimfive (ELI5)](https://www.reddit.com/r/explainlikeimfive/) subreddit. 
The objective is to generate meaningful KPIs that capture community engagement, topic trends, and interaction quality — all automated and visualized in a dashboard.

### Project Goals
________________________________________
•	Ingest Reddit data (posts & comments) from the ELI5 subreddit.<br>
•	Transform and normalize data for analytics. <br>
•	Generate daily KPIs around subreddit activity. <br>
•	Visualize insights in an interactive dashboard. <br>

### Architecture 
![image](https://github.com/user-attachments/assets/75260fe0-1fce-40c8-822c-09e5518937b0)

### Components
________________________________________
#### 1. Orchestration Layer (Apache Airflow) <br>
The ETL pipeline is scheduled to run daily via Airflow and includes the following tasks.<br>
•	Web Scraping Task: Extracts new posts & comments daily using PRAW.<br>
•	Data Staging Task: Uploads raw data to object storage (MinIO).<br>
•	Landing → Production Load: Processes raw files to structured tables.<br>
•	Aggregation Task: Builds daily KPIs for visualization.<br>
________________________________________
#### 2. Ingestion layer
New posts on ELI5 Subreddit are extracted daily using PRAW (Reddit API wrapper for scraping content) and uploaded as CSV files into MinIO Object Storage
________________________________________
#### 3. Transformation Layer (PostgreSQL)
Structured storage and processing:<br>
•	`raw_reddit_posts`: Raw text data.<br>
•	`posts`: Cleaned Reddit posts.<br>
•	`comments`: Flattened comment records.<br>
•	`authors`: Unique contributor data.<br>
• `kpi_aggregates`: Final daily summarized table for dashboard.<br>
________________________________________
#### 4. Visualization Layer (Metabase)
An interactive dashboard that includes:<br>
_Total Posts_	:  How many questions were asked during the selected timeline?<br>
_Post Frequency over time_ : 	Questions asked each day.<br>
_Upvotes per Post_ :	How engaging are the questions?<br>
_Most Common Topics_: 	What’s trending in ELI5?<br>
_Comment-to-Post Ratio_	: How much discussion happens?<br>
_Top Answerers_	: Most helpful users.<br>
_Time to First Comment_	: Community responsiveness.<br>
________________________________________
### Setup Instructions
#### Prerequisites
•	Docker + Docker Compose<br>
•	Python 3.10+<br>
##### 1. Clone the Repo
`git clone <<TO BE ADDED>>>`
##### 2. Environment Variables
Update .env or use Airflow Connections for:<br>
•	Reddit API credentials<br>
•	PostgreSQL DB credentials<br>
•	MinIO Access Keys<br>
##### 3. Start Docker Containers
`docker compose up -d`
##### 4. Airflow DAG
The DAG is located at:
`/dags/data_pipeline.py`


