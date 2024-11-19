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

  Noted: The LINE Notify service will be discontinued as of March 31, 2025. [End of service for LINE Notify](https://notify-bot.line.me/closing-announce)

 
## Tools and Services Used
This project leverages a variety of tools and services to create a robust and efficient ETL pipeline:
 1. Data Orchestration:
    - Apache Airflow: Used for scheduling and orchestrating the ETL pipeline. The environment is hosted in [Docker](https://www.docker.com/), ensuring portability and easy management of dependencies.
 2. Programming and Data Processing:
    - [Python](https://www.python.org/): Enables flexibility in building custom ETL tasks and transformations.
    - [Pandas](https://pandas.pydata.org/): Facilitates efficient data manipulation and transformation.
    - [PyArrow](https://arrow.apache.org/docs/python/index.html): Converts data into Parquet format for optimized storage and querying.
 3. Data Storage and Integration:
    - Google BigQuery: Hosts the crime dataset and integrates weather data for comprehensive analysis
    - Google Cloud Storage (GCS): Stores processed data in CSV and Parquet formats for scalability and accessibility.
    - Amazon S3: Provides additional storage for redundancy and distributed access.
    - PostgreSQL: Manages structured data in a relational database format.
 4. Data Profile and Quality:
    - [Dataplex](https://cloud.google.com/dataplex?hl=en): Used to ensure high data quality and maintain a comprehensive data profile. Dataplex automatically generates metadata, schema validation, and profiling reports, enabling users to monitor data consistency and identify anomalies.
 5. APIs and External Services:
    - OpenWeather API: Supplies real-time weather data for Chicago.
    - LINE Notify: Sends notifications about pipeline execution (success or failure). Note: LINE Notify will be discontinued on March 31, 2025.
    - Slack Webhooks: Provides task status updates directly to Slack channels for seamless communication.
 6. Task Monitoring and Notifications:
    - Notifications via LINE Notify and Slack API ensure visibility into pipeline operations, alerting stakeholders to task statuses, timestamps, and errors.
 7. Containerized Environment:
    - Docker: Ensures consistent deployment of Apache Airflow.

 
