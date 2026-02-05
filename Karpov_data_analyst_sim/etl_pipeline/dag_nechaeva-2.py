from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишем таски в питоне
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
import numpy as np

# ClickHouse connection
def ch_get_df(query, host='',
              user='', password=''):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


# default parameters
default_args = {
    'owner': 'd.nechaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 18),
}

# DAG every day at 8 am
schedule_interval = '0 8 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, description="Daily DAG")
def dag_daily_app_info():

    @task()
    def extract_feed():
        """
        get data on feed from db
        """
        query_feed = """SELECT
                    user_id
                    , toDate(time) as event_date
                    , gender
                    , age
                    , os 
                    , SUM (if(action = 'view', 1, 0)) as views
                    , SUM (if(action = 'like', 1, 0)) as likes
                    FROM simulator_20251220.feed_actions
                    WHERE toDate(time) = today() - 1
                    GROUP BY user_id, toDate(time), gender, age, os
                    format TSVWithNames"""
        df_feed = ch_get_df(query=query_feed)
        return df_feed
    
    @task()
    def extract_messages():
        """
        get data on messages from db
        """
        query_messages = """SELECT 
                    t1.event_date as event_date
                    , t1.user_id as user_id
                    , t1.messages_sent
                    , t1.users_sent
                    , t2.messages_received
                    , t2.users_received
                    , t1.gender
                    , t1.age
                    , t1.os
                    FROM 
                    (SELECT 
                    user_id
                    , toDate(time) as event_date
                    , count(receiver_id) as messages_sent
                    , count(distinct receiver_id) as users_sent
                    , gender
                    , age
                    , os 
                    FROM simulator_20251220.message_actions
                    WHERE toDate(time) = today() - 1
                    GROUP BY user_id, toDate(time), gender, age, os) t1
                    LEFT JOIN
                    (SELECT 
                    receiver_id
                    , toDate(time) as event_date
                    , count(user_id) as messages_received
                    , count(distinct user_id) as users_received
                    FROM simulator_20251220.message_actions
                    WHERE toDate(time) = today() - 1
                    GROUP BY receiver_id, toDate(time)) t2
                    ON t1.user_id = t2.receiver_id
                    format TSVWithNames"""
        df_message = ch_get_df(query=query_messages)
        return df_message

    @task
    def merge_tables(df_feed, df_message):
        
        """
        Task to merge df with messages and feed
        """
        df = pd.merge(df_feed, df_message, how='outer', on=['user_id', 'event_date', 'gender', 'age', 'os'])
        #filling NaN with 0
        df['views'].fillna(0, inplace=True)
        df['likes'].fillna(0, inplace=True)
        df['messages_sent'].fillna(0, inplace=True)
        df['users_sent'].fillna(0, inplace=True)
        df['messages_received'].fillna(0, inplace=True)
        df['users_received'].fillna(0, inplace=True)
        
        return df
    
    @task
    def os(df):
        """
        Task to group on os
        """
        os = (
            df
            .groupby(['event_date', 'os'], as_index=False)
            .agg(
            views=('views', 'sum'),
            likes=('likes', 'sum'),
            messages_sent=('messages_sent', 'sum'),
            messages_received=('messages_received', 'sum'),
            users_sent=('users_sent', 'sum'),
            users_received=('users_received', 'sum'))
            .rename(columns={'os': 'dimension_value'}))
        
        os['dimension'] = 'os'
        
        return os
    
    @task
    def gender(df):
        """
        Task to group on gender
        """
        gender = (
            df
            .groupby(['event_date', 'gender'], as_index=False)
            .agg(
            views=('views', 'sum'),
            likes=('likes', 'sum'),
            messages_sent=('messages_sent', 'sum'),
            messages_received=('messages_received', 'sum'),
            users_sent=('users_sent', 'sum'),
            users_received=('users_received', 'sum'))
            .rename(columns={'gender': 'dimension_value'}))
        
        gender['dimension'] = 'gender'
        
        return gender
    
    @task
    def age(df):
        """
        Task to group on age
        """
        age = (
            df
            .groupby(['event_date', 'age'], as_index=False)
            .agg(
            views=('views', 'sum'),
            likes=('likes', 'sum'),
            messages_sent=('messages_sent', 'sum'),
            messages_received=('messages_received', 'sum'),
            users_sent=('users_sent', 'sum'),
            users_received=('users_received', 'sum'))
            .rename(columns={'age': 'dimension_value'}))
        
        age['dimension'] = 'age'
        
        return age
    
    @task
    def combine_tables(gender_df, os_df, age_df):
        """
        Task to merge all the dataframes (age, gender, os) together
        """
        
        df_full = pd.concat([gender_df, os_df, age_df], ignore_index=True)
        
        df_full['dimension_value'] = df_full['dimension_value'].astype(str)
        
        numeric_cols = [
        'views', 'likes', 'messages_received', 'messages_sent', 
        'users_received', 'users_sent']
        
        # Fill NaNs with 0 to prevent conversion errors, then cast to int
        df_full[numeric_cols] = df_full[numeric_cols].fillna(0).astype('int64')
        
        return df_full[['event_date', 'dimension', 'dimension_value', 'views', 'likes',
                         'messages_received', 'messages_sent', 'users_received', 'users_sent']]
    
    @task
    def add_table_to_ch(df_full):
        """
        Task to add data to clickhouse table
        """
        
        #test DB where the table should be created and stored
        connection_test = {'host': '',
                      'database':'',
                      'user':'', 
                      'password':''
                     }

        #creating new table
        query_test = '''
        CREATE TABLE IF NOT EXISTS test.the_app_usage
                    (
                    event_date Date,
                    dimension String,
                    dimension_value String,
                    views UInt64,
                    likes UInt64, 
                    messages_received UInt64,
                    messages_sent UInt64, 
                    users_received UInt64, 
                    users_sent UInt64
                    )
                    ENGINE = MergeTree()
                    ORDER BY (event_date, dimension, dimension_value)
                    '''

        
        ph.execute(query = query_test, connection = connection_test)
        
        ph.to_clickhouse(df=df_full, table="the_app_usage",index=False, connection=connection_test)
    
 
    df_feed = extract_feed()
    df_message = extract_messages()
    df = merge_tables(df_feed, df_message)
    os = os(df)
    gender = gender(df)
    age = age(df)
    df_full = combine_tables(gender, os, age)
    add_table_to_ch(df_full)
   

dag_daily_app_info = dag_daily_app_info()
    

