# libraries import
import requests
import io
from io import StringIO

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import datetime, date, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

import sys 
import os


# ClickHouse connection
connection = {'host': '',
              'database':'',
              'user':'',
              'password':''
             }


def check_anomaly(df, metric, a=5, n=5):
    # Anomalies are detected with Interquartile range
    # Сalculate Q1
    df["q25"] = df[metric].shift(1).rolling(n).quantile(0.25)
    # Calculate Q3
    df["q75"] = df[metric].shift(1).rolling(n).quantile(0.75)
    # IQR
    df["iqr"] = df["q75"] - df["q25"]
    # calculate upper range
    df["up"] = df["q75"] + a * df["iqr"]
    # calculate lower range
    df["low"] = df["q25"] - a * df["iqr"]

    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df


# Default parameters
default_args = {
    'owner': 'd.nechaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 2, 2),
}

# DAG every 15 mins
schedule_interval = "*/15 * * * *"

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, description="Alert DAG")
def dag_alert_ndv():
    
    @task()
    def run_alerts(chat=None):
    
    #alert system, takes a query from db, metrics and apply check_anomaly function for anomaly detection
    
        chat_id = chat or  #alerts chat id
        bot = telegram.Bot(token="")  # bot token
        
        #Feed
        query = '''
            SELECT 
            toStartOfFifteenMinutes(time) AS ts, 
            toDate(time) AS date, 
            formatDateTime(ts, '%R') AS hm,
            uniqExact(user_id) AS users_feed, 
            countIf(action='like') AS likes, 
            countIf(action='view') AS views, 
            ROUND(countIf(action='like')/countIf(action='view'), 2) AS ctr
            FROM simulator_20251220.feed_actions
            WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts
        '''
        data = ph.read_clickhouse(query, connection=connection)

        metrics_list = ['views', 'likes', 'ctr', 'users_feed']
        
        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric)

            if is_alert == 1:
                dashboard_link = "https://superset.lab.karpov.courses/superset/dashboard/7933/"
                message = '''Time: {time} \n Metric: {metric}\n Current value {current_val:.2f}.\n Difference is more than {val_diff:.2%}.\n Check dashboard for details {dashboard_link}'''.format(
                    time=df['ts'].iloc[-1].strftime('%Y-%m-%d %H:%M'),
                    metric=metric, 
                    current_val=df[metric].iloc[-1], 
                    val_diff = abs(1 - df[metric].iloc[-1] / df[metric].iloc[-2]),
                    dashboard_link=dashboard_link)
                # plot a graph
                sns.set(rc={'figure.figsize': (16,10)})
                plt.tight_layout()

                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
                sns.lineplot(x=df['ts'], y=df['up'], label='up', ax=ax)
                sns.lineplot(x=df['ts'], y=df['low'], label='low', ax=ax)

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                ax.set_title(metric)
                ax.set(ylim=(0, None))

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object, format='png', bbox_inches='tight')
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=message)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
                
                #Messender
                
        query_msg = '''
            SELECT 
            toStartOfFifteenMinutes(time) AS ts, 
            toDate(time) AS date, 
            formatDateTime(ts, '%R') AS hm,
            uniqExact(user_id) AS senders, 
            count() AS messages
            FROM simulator_20251220.message_actions
            WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts
        '''
        data_msg = ph.read_clickhouse(query_msg, connection=connection)

        metrics_list_msg = ['senders', 'messages']
        
        for metric in metrics_list_msg:
            print(metric)
            df_msg = data_msg[['ts', 'date', 'hm', metric]].copy()
            is_alert, df_msg = check_anomaly(df_msg, metric)

            if is_alert == 1:
                dashboard_link = "https://superset.lab.karpov.courses/superset/dashboard/7933/"
                message = '''Time: {time} \n Metric: {metric}\n Current value {current_val:.2f}.\n Difference is more than {val_diff:.2%}.\n Check dashboard for details {dashboard_link}'''.format(
                    time=df_msg['ts'].iloc[-1].strftime('%Y-%m-%d %H:%M'),
                    metric=metric, 
                    current_val=df_msg[metric].iloc[-1], 
                    val_diff = abs(1 - df_msg[metric].iloc[-1] / df_msg[metric].iloc[-2]),
                    dashboard_link=dashboard_link)

                sns.set(rc={'figure.figsize': (16,10)})
                plt.tight_layout()

                ax = sns.lineplot(x=df_msg['ts'], y=df_msg[metric], label='metric')
                sns.lineplot(x=df_msg['ts'], y=df_msg['up'], label='up', ax=ax)
                sns.lineplot(x=df_msg['ts'], y=df_msg['low'], label='low', ax=ax)

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                #graph
                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                ax.set_title(metric)
                ax.set(ylim=(0, None))

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object, format='png', bbox_inches='tight')
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=message)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)


        return 

    run_alerts()


dag_alert_ndv = dag_alert_ndv()