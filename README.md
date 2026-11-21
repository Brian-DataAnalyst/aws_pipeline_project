# AWS data pipeline project showing daily trending youtube stats

This project combines static Kaggle files from 10 regions with a daily Youtube API ingestion pull to show trending video data. The pipeline combines Glue ETL jobs with lambda functions to process the data through the medallion architecture to produce analytics ready datasets. These functions are all orchestrated with AWS step functions and have a data quality gate prior to publishing to the Gold layer.

Overall project architecture

<img width="1690" height="931" alt="YouTube Trending Data Pipeline Diagram" src="https://github.com/user-attachments/assets/701c5230-d4f2-4ef4-abbe-f7377783c31b" />


Features - 

This pipeline automates the full end to end process to produce 3 analytics ready tables from the Youtube API. 

Trending Analytics — daily trending metrics per region (total videos, views, engagement rates)
Channel Analytics — channel-level performance and ranking across regions
Category Analytics — category-level breakdowns with view share percentages
The pipeline supports 10 regions and runs on a configurable schedule via AWS EventBridge.


Tech stack used - 
| Component       | Technology                     |
|-----------------|---------------------------------|
| Compute         | AWS Lambda, AWS Glue (PySpark)  |
| Storage         | Amazon S3 (Parquet, json, csv)  |
| Orchestration   | AWS Step Functions               |
| Scheduling      | Amazon EventBridge               |
| Metadata        | AWS Glue Data Catalog            |
| Query Engine    | Amazon Athena                    |
| Alerting        | Amazon SNS                       |
| Monitoring      | Amazon CloudWatch                |
| Security        | AWS IAM                          |
| Languages       | Python 3, PySpark, SQL            |
| Libraries       | Pandas, AWS Wrangler, Boto3       |
| Data Format     | Parquet (Snappy compression)      |


Data Flow

Bronze Layer (Raw data)
Ingestion of Youtube API trending video data via the ingestion lambda (youtube_api_ingestion).
This lambda pulls 2 json files into the raw layer per region 

Trending videos — top 50 trending videos per region
Category mappings — video category ID-to-name reference data

Data is stored as raw JSON in S3, partitioned by region, date, and hour:

s3://bronze-bucket/youtube/raw_statistics/region=US/date=2026-04-01/hour=12/
s3://bronze-bucket/youtube/raw_statistics_reference_data/region=US/

Silver Layer (Cleaned data)
Two parallel jobs run on the raw data from the bronze layer to produce the silver layer. 

Statistics (Glue job: bronze_to_silver_statistics)

Schema enforcement across both API JSON and Kaggle CSV formats
Type casting (views, likes, dislikes → Long; dates parsed)
Null handling and region standardization
Deduplication (latest record per video/region/date)
Derived metrics: like_ratio, engagement_rate
Output: Parquet with Snappy compression, partitioned by region


Reference data (lambda: json_to_parquet)

Normalizes JSON category mappings to tabular format
Deduplicates category entries
Output: Parquet, partitioned by region

Data Quality
Before data moves to the gold, the Data quality lambda function(data_quality) validates Silver data:

| Check             | Threshold                    |
|-------------------|-------------------------------|
| Row count         | >= 10 rows                    |
| Null percentage   | <= 5% on critical columns     |
| Schema validation | Required columns present     |
| Value ranges      | Views sanity check            |
| Data freshness    | < 48 hours since last data    |

If any of the above checks fail, the job stops and sends an sns notification of the failure and details from what failed. Additionally, the silver_gold transform does not execute.

Gold layer aggregation
### `trending_analytics`

Daily trending metrics aggregated per region.

| Column                | Description                       |
|-----------------------|-----------------------------------|
| `region`              | Country code (US, GB, IN, etc.)   |
| `trending_date_parsed`| Date of trending snapshot         |
| `total_videos`        | Number of trending videos         |
| `total_views`         | Sum of all views                  |
| `total_likes`         | Sum of all likes                  |
| `avg_views_per_video` | Average views per trending video  |
| `avg_like_ratio`      | Average like-to-view ratio        |
| `avg_engagement_rate` | Average engagement rate           |
| `unique_channels`     | Count of distinct channels        |
| `unique_categories`   | Count of distinct categories      |

### `channel_analytics`

Channel-level performance and ranking.

| Column                | Description                           |
|-----------------------|---------------------------------------|
| `channel_title`       | YouTube channel name                  |
| `region`              | Country code                          |
| `total_videos`        | Videos that trended                   |
| `total_views`         | Total views across trending videos    |
| `avg_engagement_rate` | Average engagement rate               |
| `times_trending`      | Number of times appeared in trending  |
| `rank_in_region`      | Performance rank within the region    |
| `categories`          | Categories the channel appears in     |

### `category_analytics`

Category-level breakdowns with view share.

| Column                | Description                    |
|-----------------------|--------------------------------|
| `category`            | Video category name            |
| `region`              | Country code                   |
| `trending_date_parsed`| Date of trending snapshot      |
| `video_count`         | Number of videos in category   |
| `total_views`         | Total views for the category   |
| `avg_engagement_rate` | Average engagement rate        |
| `view_share_pct`      | Percentage of total views      |

### Storage and Querying

All Gold tables are stored in **Parquet format with Snappy compression**, partitioned by `region`, and registered in the **AWS Glue Data Catalog** for querying through **Amazon Athena**.

### Prior to set up

AWS Account with permissions to create Lambda, Glue, S3, Step Functions, SNS, IAM, Athena, EventBridge, and CloudWatch resources
YouTube Data API v3 key — obtain from the Google Cloud Console
AWS CLI configured with appropriate credentials
Python 3.9+


## Initial AWS Setup

### 1. Create S3 Buckets

Create the following S3 buckets for the Bronze, Silver, Gold, and Script layers:

```bash
aws s3 mb s3://yt-data-pipeline-bronze-<region>-<env>
aws s3 mb s3://yt-data-pipeline-silver-<region>-<env>
aws s3 mb s3://yt-data-pipeline-gold-<region>-<env>
aws s3 mb s3://yt-data-pipeline-script-<region>-<env>
```

### 2. Create Glue Databases

Create the Glue databases for each data layer:

```bash
aws glue create-database --database-input '{"Name": "yt_pipeline_bronze_<env>"}'

aws glue create-database --database-input '{"Name": "yt_pipeline_silver_<env>"}'

aws glue create-database --database-input '{"Name": "yt_pipeline_gold_<env>"}'
```

### 3. Create an SNS Topic for Alerts

Create an SNS topic and subscribe your email address to receive pipeline alerts:

```bash
aws sns create-topic --name yt-data-pipeline-alerts-<env>

aws sns subscribe --topic-arn <topic-arn> --protocol email --notification-endpoint <your-email>
```

---

## Configuration

### Environment Variables

#### Ingestion Lambda

| Variable           | Description                  | Example                                      |
|--------------------|------------------------------|----------------------------------------------|
| `YOUTUBE_API_KEY`  | YouTube Data API v3 key       | `AIzaSy...`                                  |
| `S3_BUCKET_BRONZE` | Bronze S3 bucket name         | `yt-data-pipeline-bronze-us-west-1-dev`     |
| `YOUTUBE_REGIONS`  | Comma-separated region codes  | `US,GB,CA,DE,FR,IN,JP,KR,MX,RU`             |

#### Data Quality Lambda

| Variable              | Description                    | Default                  |
|-----------------------|--------------------------------|--------------------------|
| `S3_BUCKET_SILVER`    | Silver S3 bucket name          | —                        |
| `GLUE_DB_SILVER`      | Silver Glue database name      | `yt_pipeline_silver_dev` |
| `SNS_ALERT_TOPIC_ARN` | SNS topic ARN for alerts       | —                        |
| `DQ_MIN_ROW_COUNT`    |


