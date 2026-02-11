# Импортируем необходимые библиотеки
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


# connect to ClickHouse
connection = {'host': '',
              'database':'',
              'user':'',
              'password':''
             }


def check_anomaly(df, metric, a=3, n=96):
    """
   Proposed algorithm defines outlier based on 3 sigma deviation from historical data (3-sigma).
    a - sigma multiplier
    n - window size (15 mins intervals in 24 hours)
    """
    if df.empty:
        return []

    alerts = []
    # Sort data by time
    df = df.sort_values('ts')

        # 1. Calculate moving average and standard deviation
        # shift(1) excludes the current value from the calculation of "norm"
    df["mean"] = df[metric].shift(1).rolling(n).mean()
    df["std"] = df[metric].shift(1).rolling(n).std()

    # 2. Form boundaries (mean +/- a * sigma)
    df["up"] = df["mean"] + a * df["std"]
    df["low"] = df["mean"] - a * df["std"]

    # 3. Smooth boundaries
    df['up_smooth'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low_smooth'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    # 4. Check the last point
    last_row = df.iloc[-1]
    current_val = last_row[metric]
    upper_bound = last_row['up_smooth']
    lower_bound = last_row['low_smooth']

    z_score = abs(current_val - last_row["mean"]) / (last_row["std"] + 1e-9)

    # Alert condition (use z_score and smoothed boundaries)
    if current_val > last_row['up_smooth'] or current_val < last_row['low_smooth']:
        is_alert = 1
    else:
        is_alert = 0
            
    return is_alert, z_score, df


# Default parameters that are passed to the tasks
default_args = {
    'owner': 'd.nechaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 2, 2),
}

# DAG interval: every 15 mins
schedule_interval = "*/15 * * * *"

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, description="Alert DAG based on 3 sigma method")
def dag_alert_ndv_3sigma():
    
    @task()
    def run_alerts(chat=None):
    
    #alert system
    
        chat_id = chat or  #bot chat_id
        bot = telegram.Bot(token="")  
        
        #Feed actions
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
            WHERE time >= today() - 14 AND time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts
        '''
        data = ph.read_clickhouse(query, connection=connection)

        metrics_list = ['views', 'likes', 'ctr', 'users_feed']
        
        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, z_score, df = check_anomaly(df, metric)

            if is_alert == 1 or True:
                dashboard_link = "https://superset.lab.karpov.courses/superset/dashboard/7933/"
                message = '''3sigma. Time: {time} \n Metric: {metric}\n Current value {current_val:.2f}.\n Mean {mean:.2f},\n std {std:.4f},\n z_score {z_score:.1f}.\n Check dashboard for details {dashboard_link}'''.format(
                    time=df['ts'].iloc[-1].strftime('%Y-%m-%d %H:%M'),
                    metric=metric, 
                    current_val=df[metric].iloc[-1], 
                    mean = df.iloc[-1]['mean'],
                    std = df.iloc[-1]['std'],
                    z_score = z_score,
                    dashboard_link=dashboard_link)
                

                sns.set(rc={'figure.figsize': (16,10)})
                plt.tight_layout()

                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
                sns.lineplot(x=df['ts'], y=df['up_smooth'], label='up', ax=ax)
                sns.lineplot(x=df['ts'], y=df['low_smooth'], label='low', ax=ax)

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
                
                
                #Messenger actions
                
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
                message = '''Metric: {metric}\n Current value {current_val:.2f}. Difference is more than {val_diff:.2%}.\n Check dashboard for details {dashboard_link}'''.format(
                    metric=metric, 
                    current_val=df[metric].iloc[-1], 
                    val_diff = abs(1 - df[metric].iloc[-1] / df[metric].iloc[-2]),
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


dag_alert_ndv_3sigma = dag_alert_ndv_3sigma()