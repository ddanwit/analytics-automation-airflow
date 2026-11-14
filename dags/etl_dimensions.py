# coding=utf-8
"""
ETL-пайплайн: каждый день считаем метрики за ВЧЕРА по feed_actions и message_actions,
режем по gender / age / os и дописываем в таблицу схемы test в ClickHouse.

Граф:
  extract_feed ──┐
                 ├─> merge_tables ─┬─> transform_os     ─┐
  extract_messages ┘               ├─> transform_gender ─┼─> load
                                   └─> transform_age    ─┘
"""
from datetime import datetime, timedelta

import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task

TABLE_NAME = ''
OWNER = ''

DAY_OVERRIDE = None

SRC_DB = ''  

connection_src = {
    'host': '',
    'database': м,
    'user': 'student',
    'password': '',
}
connection_test = {
    'host': '',
    'database': '',
    'user': '',
    'password': '',
}

METRICS = ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']
ATTRS = ['gender', 'age', 'os']

default_args = {
    'owner': OWNER,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 10, 1),
}
schedule_interval = '0 7 * * *'  

# вспомогательные функции
def day_expr():
    """Выражение ClickHouse для нужного дня: вчера или заданная для отладки дата."""
    return f"toDate('{DAY_OVERRIDE}')" if DAY_OVERRIDE else 'yesterday()'

def fetch_user_dict():
    """Последние известные gender/age/os каждого пользователя (по обеим таблицам)."""
    q = f"""
        SELECT user_id,
               argMax(gender, time) AS gender,
               argMax(age, time)    AS age,
               argMax(os, time)     AS os
        FROM (
            SELECT user_id, gender, age, os, time FROM {SRC_DB}.feed_actions
            UNION ALL
            SELECT user_id, gender, age, os, time FROM {SRC_DB}.message_actions
        )
        GROUP BY user_id
    """
    return ph.read_clickhouse(q, connection=connection_src)


def make_slice(df, dim):
    """Агрегируем метрики по одному срезу и приводим к формату финальной таблицы."""
    out = df.groupby(['event_date', dim], as_index=False)[METRICS].sum()
    out.insert(1, 'dimension', dim)
    out = out.rename(columns={dim: 'dimension_value'})
    out['dimension_value'] = out['dimension_value'].astype(str)
    return out

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_radvilovich():  # имя DAG должно быть уникальным: добавьте свой юзернейм

    # 1a. feed_actions: на пользователя — просмотры и лайки
    @task()
    def extract_feed():
        q = f"""
            SELECT toDate(time) AS event_date,
                   user_id,
                   any(gender) AS gender,
                   any(age)    AS age,
                   any(os)     AS os,
                   countIf(action = 'view') AS views,
                   countIf(action = 'like') AS likes
            FROM {SRC_DB}.feed_actions
            WHERE toDate(time) = {day_expr()}
            GROUP BY event_date, user_id
        """
        return ph.read_clickhouse(q, connection=connection_src)

    # 1b. message_actions: на пользователя — сколько отправил/получил и скольким/от скольких
    @task()
    def extract_messages():
        q_sent = f"""
            SELECT user_id,
                   any(gender) AS gender,
                   any(age)    AS age,
                   any(os)     AS os,
                   count()              AS messages_sent,
                   uniqExact(receiver_id) AS users_sent
            FROM {SRC_DB}.message_actions
            WHERE toDate(time) = {day_expr()}
            GROUP BY user_id
        """
        q_recv = f"""
            SELECT receiver_id AS receiver,
                   count()             AS messages_received,
                   uniqExact(user_id)  AS users_received
            FROM {SRC_DB}.message_actions
            WHERE toDate(time) = {day_expr()}
            GROUP BY receiver_id
        """
        sent = ph.read_clickhouse(q_sent, connection=connection_src)
        recv = ph.read_clickhouse(q_recv, connection=connection_src).rename(columns={'receiver': 'user_id'})
        msg = sent.merge(recv, on='user_id', how='outer')
        for c in ['messages_sent', 'users_sent', 'messages_received', 'users_received']:
            msg[c] = msg[c].fillna(0).astype('int64')
        return msg

    # 2. Объединяем две таблицы в одну (по user_id)
    @task()
    def merge_tables(feed, msg):
        if feed.empty and msg.empty:
            raise ValueError('Нет данных за выбранный день ни в feed_actions, ни в message_actions')
        day = feed['event_date'].iloc[0] if not feed.empty else None
        if day is None:  # на случай, если в ленте пусто, а сообщения есть
            q = f"SELECT {day_expr()} AS event_date"
            day = ph.read_clickhouse(q, connection=connection_src)['event_date'].iloc[0]

        cube = feed.drop(columns=['event_date']).merge(msg, on='user_id', how='outer', suffixes=('', '_m'))
        for a in ATTRS:  # атрибуты берём из ленты, а если там нет — из сообщений
            cube[a] = cube[a].combine_first(cube[a + '_m'])
        cube = cube.drop(columns=[a + '_m' for a in ATTRS])

        # пользователи, которые вчера только получали сообщения, не имеют атрибутов в данных дня —
        # достаём их из справочника
        missing = cube[ATTRS].isna().any(axis=1)
        if missing.any():
            ud = fetch_user_dict().set_index('user_id')
            for a in ATTRS:
                cube.loc[missing, a] = cube.loc[missing, 'user_id'].map(ud[a])
        still = cube[ATTRS].isna().any(axis=1)
        if still.any():
            print(f'Не удалось определить атрибуты у {still.sum()} пользователей — исключены из срезов')
            cube = cube[~still]

        for c in METRICS:
            cube[c] = cube[c].fillna(0).astype('int64')
        cube['gender'] = cube['gender'].astype('int64')
        cube['age'] = cube['age'].astype('int64')
        cube['event_date'] = pd.to_datetime(day)
        return cube[['event_date', 'user_id'] + ATTRS + METRICS]

    # 3. Три среза — три таска
    @task()
    def transform_os(cube):
        return make_slice(cube, 'os')

    @task()
    def transform_gender(cube):
        return make_slice(cube, 'gender')

    @task()
    def transform_age(cube):
        return make_slice(cube, 'age')

    # 4-5. Запись в ClickHouse, ежедневное дополнение таблицы
    @task()
    def load(df_os, df_gender, df_age):
        final = pd.concat([df_os, df_gender, df_age], ignore_index=True)
        final['event_date'] = pd.to_datetime(final['event_date']).dt.date  # в CSV уйдёт как 2026-10-24
        for c in METRICS:
            final[c] = final[c].astype('int64')
        final = final[['event_date', 'dimension', 'dimension_value'] + METRICS]

        ph.execute(f"""
            CREATE TABLE IF NOT EXISTS test.{TABLE_NAME}
            (
                event_date        Date,
                dimension         String,
                dimension_value   String,
                views             Int64,
                likes             Int64,
                messages_received Int64,
                messages_sent     Int64,
                users_received    Int64,
                users_sent        Int64
            )
            ENGINE = MergeTree()
            ORDER BY (event_date, dimension, dimension_value)
        """, connection=connection_test)

        # защита от дублей при перезапуске: если за этот день уже есть данные — не дописываем
        day = final['event_date'].iloc[0]
        cnt = ph.read_clickhouse(
            f"SELECT count() AS c FROM test.{TABLE_NAME} WHERE event_date = '{day}'",
            connection=connection_test)['c'].iloc[0]
        if cnt > 0:
            print(f'За {day} данные уже есть ({cnt} строк) — пропускаем загрузку')
            return
        ph.to_clickhouse(df=final, table=TABLE_NAME, index=False, connection=connection_test)
        print(f'Загружено {len(final)} строк за {day} в test.{TABLE_NAME}')

    # ------------------------------------------------------ порядок выполнения
    feed = extract_feed()
    msg = extract_messages()
    cube = merge_tables(feed, msg)
    df_os = transform_os(cube)
    df_gender = transform_gender(cube)
    df_age = transform_age(cube)
    load(df_os, df_gender, df_age)


dag_etl_radvilovich = dag_etl_radvilovich()