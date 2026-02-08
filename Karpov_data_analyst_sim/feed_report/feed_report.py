from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task

from datetime import datetime, timedelta
from datetime import date
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import pandahouse as ph

import requests
import telegram
import io
import logging
from io import StringIO


# get ClickHouse
def ch_get_df(query, host='',
              user='', password=''):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


# Default arguments
default_args = {
    'owner': 'd.nechaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 24),
}

#DAG every day at 11 am
schedule_interval = '0 11 * * *'

my_token = '' 
group_chat_id = 

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, description="Daily DAG")
def dag_feed_report():

    @task()
    def extract_feed():
        """
        Extracts data on feed from database
        """
        query_feed = """SELECT
                        toDate(time) as event_date
                        , count(distinct user_id) as DAU
                        , SUM (if(action = 'view', 1, 0)) as views
                        , SUM (if(action = 'like', 1, 0)) as likes
                        , likes/views as ctr
                        FROM simulator_20251220.feed_actions
                        WHERE toDate(time) BETWEEN (today()-7) AND (today()-1) 
                        GROUP BY toDate(time)
                        ORDER BY toDate(time) ASC
                    format TSVWithNames"""
        df_feed = ch_get_df(query=query_feed)
        # df_feed['event_date'] = pd.to_datetime(df_feed['event_date'])
        return df_feed
    
    
    @task()
    def generate_message(df_feed):
        """
        Sends a message with metrics to a chat
        """
        yesterday = (datetime.today() - timedelta(days=1)).strftime('%d-%m-%Y')

        message = (
            f"Key metrics for: {yesterday}\n"
            f"DAU: {df_feed['DAU'].values[6]:,}" + "\n"
            f"Views: {df_feed['views'].values[6]:,}" + "\n"
            f"Likes: {df_feed['likes'].values[6]:,}" + "\n"
            f"CTR: {df_feed['ctr'].values[6]:.1%}"
        )

        return message

        
    @task()
    def generate_plots(df_feed):
        # Generate the Plots
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        plt.suptitle("The key metrics for the last week", fontsize=18, y=0.95)

        metrics = [('DAU', 'DAU'), ('views', 'Views'), ('likes', 'Likes'), ('ctr', 'CTR')]
        dark_blue = '#003366' 

        for i, (col, title) in enumerate(metrics):
            ax = axes.flatten()[i]
            ax.plot(df_feed['event_date'], df_feed[col], color=dark_blue, linewidth=2, marker='o', markersize=4)
            ax.set_title(title, fontsize=14)
            ax.set_xlabel("") 
            ax.grid(True, linestyle='--', alpha=0.5)
            ax.tick_params(axis='x', rotation=45)

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        
        graph = io.BytesIO()
        plt.savefig(graph)
        graph.seek(0)
        plt.close()

        return graph


    @task()
    def send_info(message, graph, chat_id):
        bot = telegram.Bot(token = my_token)
        bot.sendMessage(chat_id = chat_id, text=message)
        bot.sendPhoto(chat_id = chat_id, photo=graph)
        return None
      
    #pipeline    
    df_feed = extract_feed()
    message = generate_message(df_feed)
    graph = generate_plots(df_feed)
    send_info(message, graph, group_chat_id)


dag_feed_report = dag_feed_report()
