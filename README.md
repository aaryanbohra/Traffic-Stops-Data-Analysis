# 🚔 Police Traffic Stops Analysis & Dashboard 📊

A **PySpark-based data pipeline and interactive dashboard** that analyzes racial and temporal patterns in U.S. police traffic stop data.
Answers Real World question of “Do racial demographics and geographic location affect police stop patterns, such as the time of day when stops occur?
It cleans and standardizes messy multi-state datasets, aggregates key metrics, and visualizes patterns by race, region, and time of day.  

The dashboard helps uncover potential disparities and trends in policing practices.

---

## 🚀 Features

- ✅ Scalable **PySpark ETL pipeline** for cleaning and normalizing raw data
- ✅ Uses **PySpark SQL queries** for efficient distributed aggregations and feature engineering   
- ✅ Handles inconsistent formats, mislabeled fields, and invalid values  
- ✅ Feature engineering: infers `stop_hour`, adds `region` and `time_of_day` buckets  
- ✅ Aggregates stops by race, region, and time category  
- ✅ Interactive web dashboard with **Dash + Plotly**  
- ✅ Supports large datasets and produces clean **Parquet and CSV outputs**

---
## 🧰 Tech Stack

- **PySpark SQL & DataFrame API** — Distributed data cleaning & aggregation  
- **Dash & Plotly** — Interactive dashboard visualizations  
- **Parquet & CSV** — Cleaned output formats  
- **Python** — Glue code & orchestration  

---
## Results
A Interactive Dash UI with all the graphs for analysis
![image](https://github.com/user-attachments/assets/842ac3ca-1bc4-4ac8-b41e-195409f2554f)

