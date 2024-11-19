# chicago-things

## Table of Contents
* [Pipeline Overview](#Pipeline-Overview)
* [Project Background](#Project-Background)
* [Project Overview](#Project-Overview)
* [Tools and Services Used](#Tools-and-Services-Used)


## Pipeline Overview
![Untitled-2024-10-15-1453](https://github.com/user-attachments/assets/bdaee42c-bc56-41ce-be1e-ae20c0e75601)

## Project Background
This project delves into the connection between weather conditions and crime in Chicago. By combining publicly available crime data from BigQuery with real-time weather information from [the Openweather API](https://openweathermap.org/api), it seeks to uncover patterns and insights that could guide law enforcement strategies and urban planning. The idea is that factors like temperature, humidity, and visibility may influence human behavior, potentially affecting crime rates.

## Project Overview
The project leverages [Apache Airflow](https://airflow.apache.org/) to construct a reliable and scalable ETL (Extract, Transform, Load) pipeline. This pipeline processes weather data and integrates it with existing crime datasets, ensuring a comprehensive foundation for analysis. Key features of the pipeline include:
 - Data Extraction: Retrieves live weather data for Chicago from the OpenWeather API.
 - Data Transformation:
    - Converts JSON responses into a structured tabular format.
    - Normalizes data fields and converts temperature values from Kelvin to Celsius.
    - Adds time-zone-aware timestamps for precise tracking.
 - Data Loading:
    - Stores the processed data in multiple locations for analysis and backup:
      - Local CSV files for archival.
      - [Google Cloud Storage (GCS)](https://cloud.google.com/storage?hl=en) in CSV and Parquet formats.
      - [BigQuery](https://cloud.google.com/bigquery?hl=en) for advanced analysis and integration with crime data.
      - [Amazon S3](https://aws.amazon.com/s3/) for distributed storage and redundancy.
      - [PostgreSQL](https://www.postgresql.org/) for relational data storage. 
 - Notifications:
      - Success and failure notifications are sent via [LINE Notify API](https://notify-bot.line.me/doc/en/) and [Slack API](https://api.slack.com/tutorials/tracks/actionable-notifications) to ensure smooth operations.
      - Notifications include details like task execution status, timestamps, and error messages (if any), aiding quick issue resolution.
 
## Tools and Services Used

 
