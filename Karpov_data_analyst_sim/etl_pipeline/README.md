# Task description
The expected output of ETL pipeline is a DAG in Airflow, which will be calculated every day for the previous day. 

- We are processing two tables in parallel. In feed_actions, we count the number of views and likes for each user. In message_actions, we count how many messages each user receives and sends, how many people they write to, and how many people write to them. Each upload is in a separate task.  

- Next, we combine the two tables into one.  

- For this table, we calculate all these metrics broken down by gender, age, and OS.   

- And we write the final data with all the metrics to a separate table in ClickHouse.  

- Every day, new data are added to the same table   
