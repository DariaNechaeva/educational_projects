from airflow.decorators import dag, task
from airflow.models import Variable

from datetime import datetime, timedelta
import io
from io import StringIO

import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns
import telegram


# Get ClickHouse
def ch_get_df(query, host='',
              user='', password=''):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


def _ch_creds():
    host = Variable.get("clickhouse_host", default_var="")
    user = Variable.get("clickhouse_user", default_var="")
    password = Variable.get("clickhouse_password", default_var="")
    return host, user, password


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

"""
Airflow Variables expected:
- telegram_bot_token: Telegram bot token
- telegram_chat_id: chat/group id to send report to (integer)
Optional:
- clickhouse_host, clickhouse_user, clickhouse_password
"""
bot_token = '' 
chat_id =  #report chat


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, description="Daily DAG")
def dag_the_app_report():

    @task()
    def extract_feed_week():
        """
        Extracts data on feed from database
        """
        query_feed = """
            SELECT
                toDate(time) as event_date,
                count(DISTINCT user_id) as dau,
                sum(action = 'view') as views,
                sum(action = 'like') as likes,
                (likes / views) as ctr
            FROM simulator_20251220.feed_actions
            WHERE toDate(time) BETWEEN (today() - 7) AND (today() - 1)
            GROUP BY event_date
            ORDER BY event_date ASC
            FORMAT TSVWithNames
        """
        host, user, password = _ch_creds()
        df = ch_get_df(query=query_feed, host=host, user=user, password=password)
        df["event_date"] = pd.to_datetime(df["event_date"])
        return df

    
    @task()
    def extract_messages_week():
        """
        Extracts data on messages from database
        """
        query_message = """
            SELECT
                toDate(time) as event_date,
                count() as messages,
                count(DISTINCT user_id) as senders,
                (messages / senders) as avg_msg_per_sender
            FROM simulator_20251220.message_actions
            WHERE toDate(time) BETWEEN (today() - 7) AND (today() - 1)
            GROUP BY event_date
            ORDER BY event_date ASC
            FORMAT TSVWithNames
        """
        host, user, password = _ch_creds()
        df = ch_get_df(query=query_message, host=host, user=user, password=password)
        df["event_date"] = pd.to_datetime(df["event_date"])
        return df

    @task()
    def extract_dau_split_week():
        """
        Extracts data on DAU from database
        """
        query_dau = """
            WITH user_day_flags AS (
                SELECT
                    user_id,
                    day,
                    max(used_feed) AS used_feed,
                    max(used_messages) AS used_messages
                FROM (
                    SELECT
                        user_id,
                        toDate(time) AS day,
                        1 AS used_feed,
                        0 AS used_messages
                    FROM simulator_20251220.feed_actions

                    UNION ALL

                    SELECT
                        user_id,
                        toDate(time) AS day,
                        0 AS used_feed,
                        1 AS used_messages
                    FROM simulator_20251220.message_actions
                )
                GROUP BY user_id, day
            )
            SELECT
                day,
                uniqIf(user_id, used_feed = 1 AND used_messages = 0) AS dau_feed_only,
                uniqIf(user_id, used_feed = 0 AND used_messages = 1) AS dau_messages_only,
                uniqIf(user_id, used_feed = 1 AND used_messages = 1) AS dau_both,
                uniq(user_id) AS dau_total
            FROM user_day_flags
            WHERE day BETWEEN (today() - 7) AND (today() - 1)
            GROUP BY day
            ORDER BY day
            FORMAT TSVWithNames
        """
        host, user, password = _ch_creds()
        df = ch_get_df(query=query_dau, host=host, user=user, password=password)
        df["day"] = pd.to_datetime(df["day"])
        return df
    
    
    @task()
    def build_message(df_feed, df_messages, df_dau):
        """
        Builds a message with metrics
        """
        yesterday = (datetime.today() - timedelta(days=1)).strftime('%d.%m.%Y')

        feed_y = df_feed.iloc[-1]
        msg_y = df_messages.iloc[-1]
        dau_y = df_dau.iloc[-1]
        
        feed_two = df_feed.iloc[-2]
        msg_two = df_messages.iloc[-2]
        dau_two = df_dau.iloc[-2]
        
        message = '''Daily app report for {yesterday}\n\n👥 DAU total {dau_total:,} & difference {dau_total_diff:.1%}\n - Feed only: {feed_only:,} & difference {feed_dau_diff:.1%}\n- Messages only: {messages_dau:,} & difference {dau_messages_diff:.1%}\n- Both: {dau_both:,} & difference {both_dau_diff:.1%}\n\n📰 Feed:\n- Views: {feed_views:,} & difference {views_diff:.1%}\n- Likes: {likes:,} & difference {likes_diff:.1%}\n- CTR: {ctr:.2%} & difference {ctr_diff:.2%}\n\nMessages:\n- Messages sent {msg_sent:,} & difference {sent_diff:.1%}\n- Senders: {senders:,} & difference {senders_diff:.1%}\n- Avg msg/sender {msg_per_sender:.2f} & difference {diff_avg_msg:.2f}'''.format(
            yesterday = yesterday,
            dau_total = int(dau_y['dau_total']),
            dau_total_diff = (dau_y['dau_total']/dau_two['dau_total'] - 1),
            feed_only = int(dau_y['dau_feed_only']),
            feed_dau_diff = (dau_y['dau_feed_only']/dau_two['dau_feed_only'] - 1),
            messages_dau = int(dau_y['dau_messages_only']),
            dau_messages_diff = (dau_y['dau_messages_only']/ dau_two['dau_messages_only'] - 1),
            dau_both = int(dau_y['dau_both']),
            both_dau_diff = (dau_y['dau_both']/dau_two['dau_both'] - 1),
            feed_views = int(feed_y['views']),
            views_diff = (feed_y['views']/feed_two['views'] - 1),
            likes = int(feed_y['likes']),
            likes_diff = (feed_y['likes']/feed_two['likes'] - 1),
            ctr = float(feed_y['ctr']),
            ctr_diff = (feed_y['ctr']/feed_two['ctr'] - 1),
            msg_sent = int(msg_y['messages']),
            sent_diff = (msg_y['messages']/msg_two['messages'] - 1),
            senders = int(msg_y['senders']),
            senders_diff = (msg_y['senders']/msg_two['senders'] - 1),
            msg_per_sender = float(msg_y['avg_msg_per_sender']),
            diff_avg_msg = (msg_y['avg_msg_per_sender']/msg_two['avg_msg_per_sender'] - 1)
        )
        return message
            
            

    @task()
    def extract_feed_actions():
        """
        Extracts data on feed actions from database with OS, gender and age group
        """
        query = """
            SELECT 
            toStartOfInterval(time, INTERVAL 30 minute) AS interval_start,
            os,
            gender,
            multiIf(age < 18, 'Under 18', 
                    age BETWEEN 18 AND 30, '18-30', 
                    age BETWEEN 31 AND 50, '30-50', 
                    '50+') AS age_group,
            
            count() AS total_actions,
            uniqCombined(user_id) AS active_users,
            round(total_actions / active_users, 2) AS actions_per_user,
            countIf(action = 'view') AS views,
            countIf(action = 'like') AS likes,
            
            if(views > 0, round(likes / views, 4), 0) AS ctr
            FROM simulator_20251220.feed_actions
            WHERE toDate(time) = yesterday()
            GROUP BY 
            interval_start, 
            os, 
            gender, 
            age_group
            ORDER BY 
            interval_start ASC, 
            os, 
            gender
            FORMAT TSVWithNames
            """
        host, user, password = _ch_creds()
        df = ch_get_df(query=query, host=host, user=user, password=password)
        return df
    
    
    @task()
    def plot_feed_actions(df):
        """
        Plots feed actions with OS, gender and age group
        """
        # Data modifications
        df['interval_start'] = pd.to_datetime(df['interval_start'])
        df['gender'] = df.gender.astype('object')
        df['total_actions'] = df.total_actions.astype('int64')
        df['active_users'] = df.active_users.astype('int64')
        df['views'] = df.views.astype('int64')
        df['likes'] = df.likes.astype('int64')

        # metrics for cycle
        metrics = ['ctr', 'actions_per_user', 'likes', 'views']
        dimensions = ['gender', 'os', 'age_group']
        age_order = ['Under 18', '18-30', '31-50', '50+']

        # Grid
        fig, axes = plt.subplots(len(metrics), len(dimensions), figsize=(22, 24))
        plt.subplots_adjust(hspace=0.4, wspace=0.3)

        for i, metric in enumerate(metrics):
            for j, dim in enumerate(dimensions):
                ax = axes[i, j]

                # aggregation
                agg_df = df.groupby(['interval_start', dim]).agg({
                    'likes': 'sum',
                    'views': 'sum',
                    'total_actions': 'sum',
                    'active_users': 'sum'
                }).reset_index()

                # calculate metrics
                if metric == 'ctr':
                    agg_df['plot_val'] = (agg_df['likes'] / agg_df['views']) * 100
                    title_suffix = "CTR (%)"
                elif metric == 'actions_per_user':
                    agg_df['plot_val'] = agg_df['total_actions'] / agg_df['active_users']
                    title_suffix = "Actions per User"
                else:
                    agg_df['plot_val'] = agg_df[metric]
                    title_suffix = metric.capitalize()

                # plot
                sns.lineplot(
                    data=agg_df, 
                    x='interval_start', 
                    y='plot_val', 
                    hue=dim, 
                    hue_order=age_order if dim == 'age_group' else None,
                    ax=ax, 
                    marker='o', 
                    markersize=3
                )

                # title and labels
                ax.set_title(f"{title_suffix} by {dim.capitalize()}", fontsize=12)
                ax.set_ylabel(title_suffix)
                ax.set_xlabel("")
                ax.tick_params(axis='x', rotation=45)

                # Gender legend
                if dim == 'gender':
                    handles, labels = ax.get_legend_handles_labels()
                    ax.legend(handles, ['Male', 'Female'], title='Gender')

        sns.despine()
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=100) 
        buf.seek(0)
        plt.close(fig)

        return buf

    
    @task()
    def extract_message_actions():
        """
        Extracts data on message actions from database with OS, gender and age group
        """
        query = """
            SELECT 
            toStartOfInterval(time, INTERVAL 30 minute) AS interval_start,
            os,
            gender,
            multiIf(age < 18, 'Under 18', 
                    age BETWEEN 18 AND 30, '18-30', 
                    age BETWEEN 31 AND 50, '30-50', 
                    '50+') AS age_group,
            count() as messages,
            count(DISTINCT user_id) as senders,
            round(messages / senders, 2) as avg_msg_per_sender
            FROM simulator_20251220.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY 
            interval_start, 
            os, 
            gender, 
            age_group
            ORDER BY 
            interval_start ASC, 
            os, 
            gender
            FORMAT TSVWithNames
            """
        host, user, password = _ch_creds()
        df_msg = ch_get_df(query=query, host=host, user=user, password=password)
        return df_msg
    
    
    @task()
    def plot_message_actions(df_msg):
        """
        Plots message actions with OS, gender and age group
        """
        # Data modifications
        df_msg['interval_start'] = pd.to_datetime(df_msg['interval_start'])
        df_msg['gender'] = df_msg.gender.astype('object')
        df_msg['messages'] = df_msg.messages.astype('int64')
        df_msg['senders'] = df_msg.senders.astype('int64')

        # metrics for cycle
        metrics = ['messages', 'senders', 'avg_msg_per_sender']
        dimensions = ['gender', 'os', 'age_group']
        age_order = ['Under 18', '18-30', '31-50', '50+']

        # Grid
        fig, axes = plt.subplots(len(metrics), len(dimensions), figsize=(22, 24))
        plt.subplots_adjust(hspace=0.4, wspace=0.3)

        for i, metric in enumerate(metrics):
            for j, dim in enumerate(dimensions):
                ax = axes[i, j]

                # aggregation
                agg_df = df_msg.groupby(['interval_start', dim]).agg({
                    'messages': 'sum',
                    'senders': 'sum'
                }).reset_index()

                # calculate metrics
                if metric == 'avg_msg_per_sender':
                    agg_df['plot_val'] = (agg_df['messages'] / agg_df['senders'])
                    title_suffix = "Messages per sender"
                else:
                    agg_df['plot_val'] = agg_df[metric]
                    title_suffix = metric.capitalize()

                # plot
                sns.lineplot(
                    data=agg_df, 
                    x='interval_start', 
                    y='plot_val', 
                    hue=dim, 
                    hue_order=age_order if dim == 'age_group' else None,
                    ax=ax, 
                    marker='o', 
                    markersize=3
                )

                # title and labels
                ax.set_title(f"{title_suffix} by {dim.capitalize()}", fontsize=12)
                ax.set_ylabel(title_suffix)
                ax.set_xlabel("")
                ax.tick_params(axis='x', rotation=45)

                # Gender legend
                if dim == 'gender':
                    handles, labels = ax.get_legend_handles_labels()
                    ax.legend(handles, ['Male', 'Female'], title='Gender')

        sns.despine()
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=100) 
        buf.seek(0)
        plt.close(fig)

        return buf
    
    @task()
    def rng_data_extract():
        """
        Extracts data on RNG from database
        """
        query_rng = """
            SELECT toStartOfDay(toDateTime(this_week)) as date
            , status AS status
            , AVG(num_users) AS num_users
            FROM
            (SELECT this_week, 
            previous_week,
            -uniq(user_id) as num_users, 
            status
            FROM
            (SELECT user_id
            , groupUniqArray(toMonday(toDate(time))) as weeks_visited
            , addWeeks(arrayJoin(weeks_visited), +1) AS this_week
            , if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status
            , addWeeks(this_week, -1) as previous_week
            FROM simulator_20251220.feed_actions
            GROUP BY user_id
            )
            WHERE status = 'gone'
            GROUP BY this_week, previous_week, status
            HAVING this_week != addWeeks(toMonday(today()), +1)

            union all

            
            SELECT this_week
            , previous_week
            , toInt64(uniq(user_id)) as num_users
            , status
            FROM
            (SELECT user_id
            , groupUniqArray(toMonday(toDate(time))) as weeks_visited
            , arrayJoin(weeks_visited) AS this_week
            , if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status
            , addWeeks(this_week, -1) as previous_week
            FROM simulator_20251220.feed_actions
            GROUP BY user_id)
            GROUP BY this_week, previous_week, status
            ) AS v_table
            GROUP BY status
            , toStartOfDay(toDateTime(this_week))
            ORDER BY date ASC
            format TSVWithNames;
        """
        host, user, password = _ch_creds()
        df_rng = ch_get_df(query=query_rng, host=host, user=user, password=password)
        
        return df_rng

    @task()
    def rng_plot(df_rng):
        """
        Draws RNG (retained / new / gone) bar plot with seaborn. Gone stays negative.
        """
        df_rng["date"] = pd.to_datetime(df_rng["date"]).dt.strftime("%Y-%m-%d")

        fig, ax = plt.subplots(figsize=(10, 6))
        # fig.suptitle("RNG: Retained, New, Gone users by week", fontsize=14, y=1.02)

        sns.barplot(
            data=df_rng,
            x="date",
            y="num_users",
            hue="status",
            hue_order=["new", "retained", "gone"],
            # order=week_order,
            ax=ax,
        )

        ax.axhline(0, color="black", linewidth=0.5)
        ax.set_xlabel("Week")
        ax.set_ylabel("Users")
        ax.tick_params(axis="x", rotation=45)
        plt.setp(ax.get_xticklabels(), ha="right", rotation_mode="anchor")
        ax.grid(True, linestyle="--", alpha=0.4, axis="y")
        ax.legend(title=None)

        fig.tight_layout(rect=[0, 0.03, 1, 0.96])
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        buf.seek(0)
        plt.close(fig)
        return buf

   
    @task()
    def send_to_telegram(message, rng_png, feed_actions_png, message_actions_png):
        """
        Sends a report message and all graphs as Telegram photos.
        """
        bot = telegram.Bot(token=bot_token)
        bot.sendMessage(chat_id=chat_id, text=message)
        
        # List of graphs to send
        items = [
        ("Retained, new, gone users", rng_png), 
        ("Feed actions (OS / gender / age groups) for yesterday", feed_actions_png),
        ("Message actions (OS / gender / age groups) for yesterday)", message_actions_png)
        ]
        
        for label, photo in items:
            if photo is not None: # Check if any graph was not built
                photo.seek(0)
                bot.sendPhoto(chat_id=chat_id, photo=photo, caption=label)
            
        return None

      
    # Pipeline
    df_feed = extract_feed_week()
    df_messages = extract_messages_week()
    df_dau = extract_dau_split_week()
    message = build_message(df_feed, df_messages, df_dau)

    df_rng = rng_data_extract()
    rng_png = rng_plot(df_rng)
    
    df_feed_actions = extract_feed_actions()
    feed_actions_png = plot_feed_actions(df_feed_actions)
    
    df_message_actions = extract_message_actions()
    message_actions_png = plot_message_actions(df_message_actions)
    
    send_to_telegram(message, rng_png, feed_actions_png, message_actions_png)


dag_the_app_report = dag_the_app_report()