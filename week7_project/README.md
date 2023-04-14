# Data Engineering Zoomcamp Project

In this capstone project, I will showcase my data engineering skills in the following areas:

- Cloud
- Infrastructure as Code
- Workflow Orchestration
- Data Warehouse
- Batch Processing

I will apply these skills in developing an end to end data pipeline.

## Dataset

I will use the [Fantasy Premier League]("https://fantasy.premierleague.com/api/bootstrap-static/") data being scraped from an API.

## Workflow Orchestration

On inspecting the data, I realised that json output from 2 keys could not be saved to a dataframe:

1. game_settings - did not have a flat structure. Not needed for downstream analysis either,
2. total_players - is an integer representing total active players in the season,

hence I skipped these two in the ETL pipeline to upload into GCS.


