# Check alert 

This project implements an alerting system for our application using Airflow DAG. 
Every 15 minutes, it monitors key metrics such as active users in the feed and messenger, views, likes,
CTR, and the number of messages sent.  

Based on an analysis of metric behavior, the interquartile range (IQR) method was 
selected for anomaly detection. When an abnormal value is detected, the system sends an alert
to a Telegram chat containing the metric name, its current value, and the magnitude of the deviation.  

**Tech stack: Airflow, Python, SQL, Telegram API**
