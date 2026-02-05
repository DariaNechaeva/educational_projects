# Project description
In this project, we set up automatic sending of an analytical report to Telegram every morning using Airflow. 
The feed report consists of two parts:  

- text with information about key metrics for the previous day  
- a graph with metrics for the previous 7 days
  
The report contains **key metrics:** DAU, Views, Likes, CTR
