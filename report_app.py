# Бот написан на версии airflow без асинхронности


from datetime import timedelta, datetime

import pandas as pd
import numpy as np

import pandahouse as ph

import matplotlib.pyplot as plt
import seaborn as sns

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import io


# Подключение к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 
    'user': 'student',
    'database': 'simulator'
}
db='simulator_20240320'
default_args = {
    'owner': 'v.grabchuk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 4, 23),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

# Временной горизонт для составления отчёта
report_horizon = 14   # Дни


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def gvs_report_app():
    @task
    def get_dau_parts(report_horizon):
        # Статистика юзеров
        context = get_current_context()
        today = context['ds']
        today = f"'{today}'"
        q = f"""
        SELECT *
        FROM
        (
            SELECT
                toDate(time) AS date,
                COUNT(DISTINCT user_id) AS dau,
                'feed' AS part
            FROM {db}.feed_actions
            GROUP BY date

            UNION ALL

            SELECT
                toDate(time) AS date,
                COUNT(DISTINCT user_id) AS dau,
                'messenger' AS part
            FROM {db}.message_actions
            GROUP BY date
        )
        WHERE 
            date <= {today}
            AND date+{report_horizon} > {today}
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task   
    def get_dau_feed_only(report_horizon):
        # DAU исключительно в ленте
        context = get_current_context()
        today = context['ds']
        today = f"'{today}'"
        q = f"""
        SELECT DISTINCT
            COUNT(DISTINCT user_id) AS dau,
            toDate(time) AS date
        FROM {db}.feed_actions
        WHERE user_id NOT IN
        (
            SELECT DISTINCT user_id
            FROM {db}.message_actions
        )
            AND date <= {today}
            AND date+{report_horizon} > {today}
        GROUP BY date
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task   
    def get_dau_feed_and_messenger(report_horizon):
        # DAU по пользователям, использующим как ленту, так и мессенджер
        context = get_current_context()
        today = context['ds']
        today = f"'{today}'"
        q = f"""
        WITH 
            feed_days AS (
                SELECT
                    user_id,
                    DATE(time) as date
                FROM {db}.feed_actions
                GROUP BY user_id, date
            ),
            messenger_days AS (
                SELECT
                    user_id,
                    DATE(time) as date
                FROM {db}.message_actions
                GROUP BY user_id, date
            ),
            feed_and_messenger_days AS (
                SELECT 
                    l.user_id AS user_id,
                    l.date AS date
                FROM feed_days AS l
                JOIN messenger_days AS r 
                    USING user_id, date
            )

        SELECT
            COUNT(DISTINCT user_id) AS dau,
            date
        FROM feed_and_messenger_days
        WHERE
            date <= {today}
            AND date+{report_horizon} > {today}
        GROUP BY date
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def get_dau_segments_feed():
        # Статистика по сегментам пользователей ленты
        q = f"""
        WITH 
            activity_weeks_t AS (
                -- Недели активности пользователей
                SELECT DISTINCT
                    user_id,
                    toMonday(time) AS week,
                    1 AS action -- Флаг активности
                FROM {db}.feed_actions
            ),
            all_weeks_t AS (
                -- Все недели
                SELECT DISTINCT week
                FROM activity_weeks_t
            ),
            first_activity_t AS (
                SELECT
                    user_id,
                    MIN(week) AS first_activity
                FROM activity_weeks_t
                GROUP BY user_id
            ),
            users_all_weeks AS (
                SELECT *
                FROM first_activity_t
                CROSS JOIN all_weeks_t
            ),
            full_info_t AS (
                -- Полная информация по активности пользователей
                SELECT *
                FROM users_all_weeks
                LEFT JOIN activity_weeks_t
                    USING user_id, week
                WHERE
                NOT (action=0 AND week < first_activity)
            ),
            segmentation_prep_t AS (
                -- Подготовка для сегментации
                SELECT
                    user_id,
                    week,
                    first_activity,
                    action,
                    SUM(action) OVER (
                        PARTITION BY user_id
                        ORDER BY week
                        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                    ) AS segment_prep
                FROM full_info_t
            ),
            segment_t AS (
                SELECT
                    user_id,
                    week,
                    first_activity,
                    action,
                    segment_prep,
                    multiIf(
                        week=first_activity, 'new',
                        segment_prep=2 OR (action=1 AND segment_prep=1), 'retained',
                        'gone'
                    ) AS segment
                FROM segmentation_prep_t
            )

        SELECT
            COUNT(DISTINCT user_id) AS uniq_users,
            week,
            segment
        FROM segment_t
        GROUP BY week, segment
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task 
    def get_dau_segments_messenger():
        # # Статистика по сегментам пользователей мессенджера
        q = f"""
        WITH 
            activity_weeks_t AS (
                -- Недели активности пользователей
                SELECT DISTINCT
                    user_id,
                    toMonday(time) AS week,
                    1 AS action -- Флаг активности
                FROM {db}.message_actions
            ),
            all_weeks_t AS (
                -- Все недели
                SELECT DISTINCT week
                FROM activity_weeks_t
            ),
            first_activity_t AS (
                SELECT
                    user_id,
                    MIN(week) AS first_activity
                FROM activity_weeks_t
                GROUP BY user_id
            ),
            users_all_weeks AS (
                SELECT *
                FROM first_activity_t
                CROSS JOIN all_weeks_t
            ),
            full_info_t AS (
                -- Полная информация по активности пользователей
                SELECT *
                FROM users_all_weeks
                LEFT JOIN activity_weeks_t
                    USING user_id, week
                WHERE
                NOT (action=0 AND week < first_activity)
            ),
            segmentation_prep_t AS (
                -- Подготовка для сегментации
                SELECT
                    user_id,
                    week,
                    first_activity,
                    action,
                    SUM(action) OVER (
                        PARTITION BY user_id
                        ORDER BY week
                        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                    ) AS segment_prep
                FROM full_info_t
            ),
            segment_t AS (
                SELECT
                    user_id,
                    week,
                    first_activity,
                    action,
                    segment_prep,
                    multiIf(
                        week=first_activity, 'new',
                        segment_prep=2 OR (action=1 AND segment_prep=1), 'retained',
                        'gone'
                    ) AS segment
                FROM segmentation_prep_t
            )

        SELECT
            COUNT(DISTINCT user_id) AS uniq_users,
            week,
            segment
        FROM segment_t
        GROUP BY week, segment
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df

    def get_lineplot(title=None, xrotation=20, marks=False, color='b', **kwargs):
        # Возвращает lineplot
        ax = sns.lineplot(color=color, marker='o', **kwargs)
        ax.grid()
        if title is not None:
            ax.set_title(title)
        ax.tick_params(axis='x', labelrotation=xrotation)

        if marks:
            for x, y, m in zip(df[x], df[y], df[y].astype(str)):
                ax.text(x, y, m, color=color)
        return ax
    
    @task
    def get_plots(df_1, df_2, df_3, df_4, df_5):
        # Возвращает все графики на одном полотне
        size = (5, 1)
        figsize = (8, 4*size[0])

        fig, ax = plt.subplots(*size, figsize=figsize)

        ax = plt.subplot(*size, 1);
        get_lineplot(title='DAU', data=df_1, x='date', y='dau', hue='part', ax=ax);
        ax = plt.subplot(*size, 2);
        get_lineplot(title='feed only', data=df_2, x='date', y='dau');
        ax = plt.subplot(*size, 3);
        get_lineplot(title='feed & messenger', data=df_3, x='date', y='dau');
        ax = plt.subplot(*size, 4);
        get_lineplot(title='feed segments', data=df_4, x='week', y='uniq_users', hue='segment');
        ax = plt.subplot(*size, 5);
        get_lineplot(title='messenger segments', data=df_5, x='week', y='uniq_users', hue='segment');

        fig.tight_layout()

        return fig
    
    @task
    def report_image(bot, chat_id, plot):
        # Отправка изображения
        plot;
        plot = io.BytesIO()
        plt.savefig(plot, format='png', bbox_inches='tight')
        plot.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot)
    
    
    # Получаем метрики
    df_1 = get_dau_parts(report_horizon)
    df_2 = get_dau_feed_only(report_horizon)
    df_3 = get_dau_feed_and_messenger(report_horizon)
    df_4 = get_dau_segments_feed()
    df_5 = get_dau_segments_messenger()
    # Подрубаем бота
    my_token = 
    bot = telegram.Bot(token=my_token)
    chat_id = 
    # Графический отчёт
    plot = get_plots(df_1, df_2, df_3, df_4, df_5)
    report_image(bot, chat_id, plot)
    
    
gvs_report_app = gvs_report_app()

