Reddit ELI5 Data Analytics Platform: Automated Daily Insights from Community Discussions
________________________________________
Overview
This project builds a complete end-to-end data pipeline to extract, transform, and visualize data from the r/explainlikeimfive (ELI5) subreddit. The objective is to generate meaningful KPIs that capture community engagement, topic trends, and interaction quality — all automated and visualized in a dashboard.
________________________________________
 Project Goals
•	Ingest Reddit data (posts & comments) from the ELI5 subreddit.
•	Transform and normalize data for analytics.
•	Generate daily KPIs around subreddit activity.
•	Visualize insights in an interactive dashboard.
________________________________________
Architecture Summary
The pipeline is modular and broken down into four layers:
![alt text](image.png)
________________________________________
Components
 1. Orchestration Layer (Apache Airflow)
The ETL pipeline is scheduled to run daily via Airflow and includes the following tasks.
•	Web Scraping Task: Extracts new posts & comments daily using PRAW
•	Data Staging Task: Uploads raw data to object storage (MinIO)
•	Landing → Production Load: Processes raw files to structured tables
•	Aggregation Task: Builds daily KPIs for visualization
________________________________________
2. Ingestion layer
New posts on ELI5 Subreddit are extracted daily using PRAW (Reddit API wrapper for scraping content) and uploaded as CSV files into MinIO Object Storage
________________________________________
3. Transformation Layer (PostgreSQL)
Structured storage and processing:
•	raw_reddit_posts: Raw text data.
•	posts: Cleaned Reddit posts.
•	comments: Flattened comment records.
•	authors: Unique contributor data.
•	kpi_aggregates: Final daily summarized table for dashboard.
________________________________________
4. Visualization Layer (Metabase)
An interactive dashboard that includes:
Total Posts	How many questions were asked during the selected timeline?
Post Frequency over time	Questions asked each day.
Upvotes per Post	How engaging are the questions?
Most Common Topics	What’s trending in ELI5?
Comment-to-Post Ratio	How much discussion happens?
Top Answerers	Most helpful users.
Time to First Comment	Community responsiveness.
________________________________________
Setup Instructions
Prerequisites
•	Docker + Docker Compose
•	Python 3.10+
1. Clone the Repo
bash
git clone <<TO BE ADDED>>>
cd reddit-etl-dashboard
2. Environment Variables
Update .env or use Airflow Connections for:
•	Reddit API credentials
•	PostgreSQL DB credentials
•	MinIO Access Keys
3. Start Docker Containers
Bash
docker-compose up -d
4. Airflow DAG
The DAG is located at:
bash
/dags/data_pipeline.py


