# coding=utf-8
"""
Ежедневный отчёт по ленте новостей в Телеграм (каждый день в 11:00):
  - текст: DAU, просмотры, лайки, CTR за вчера (с изменением к предыдущему дню);
  - график: те же 4 метрики за последние 7 дней.
"""
import io
from datetime import datetime, timedelta
 
import pandas as pd
import pandahouse as ph
import requests
from matplotlib.figure import Figure
 
TELEGRAM_TOKEN = ''  
CHAT_ID = -            
SEND_TO_TELEGRAM = True                  
 
OWNER = 'darina-radvilovich'
SRC_DB = ''
connection = {
    'host': '',
    'database': SRC_DB,
    'user': '',
    'password': '',
}

def get_data():
    """Метрики по дням за последние 7 полных дней (до вчера включительно)."""
    q = f"""
        SELECT toDate(time) AS day,
               uniqExact(user_id) AS dau,
               countIf(action = 'view') AS views,
               countIf(action = 'like') AS likes,
               countIf(action = 'like') / countIf(action = 'view') AS ctr
        FROM {SRC_DB}.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 6 AND yesterday()
        GROUP BY day
        ORDER BY day
    """
    df = ph.read_clickhouse(q, connection=connection)
    df['day'] = pd.to_datetime(df['day'])
    return df
 
def _fmt_int(x):
    return f'{int(x):,}'.replace(',', ' ')
 
def _delta(cur, prev):
    if prev in (0, None) or pd.isna(prev):
        return ''
    return f' ({(cur / prev - 1) * 100:+.1f}% к пред. дню)'
 
def build_text(df):
    """Текст отчёта за последний день в df."""
    df = df.sort_values('day').reset_index(drop=True)
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else None
    d = lambda col: _delta(last[col], prev[col]) if prev is not None else ''
    return (
        f"Отчёт по ленте за {last['day']:%d.%m.%Y}\n\n"
        f"DAU: {_fmt_int(last['dau'])}{d('dau')}\n"
        f"Просмотры: {_fmt_int(last['views'])}{d('views')}\n"
        f"Лайки: {_fmt_int(last['likes'])}{d('likes')}\n"
        f"CTR: {last['ctr']:.3f}{d('ctr')}"
    )
 
def build_plot(df):
    """График 4 метрик за 7 дней (PNG в памяти, без сохранения на диск)."""
    df = df.sort_values('day')
    x = df['day'].dt.strftime('%d.%m')
    fig = Figure(figsize=(14, 8))
    axes = fig.subplots(2, 2)
    panels = [('dau', 'DAU'), ('views', 'Просмотры'), ('likes', 'Лайки'), ('ctr', 'CTR')]
    for ax, (col, title) in zip(axes.ravel(), panels):
        ax.plot(x, df[col], marker='o', linewidth=2)
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
    fig.suptitle('Метрики ленты за последние 7 дней')
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    return buf
 
def send_report(text, plot_buf):
    """Отправка в Телеграм одним сообщением: график + текст в подписи."""
    if not SEND_TO_TELEGRAM:
        print(text)
        print('(график построен, отправка в Телеграм выключена)')
        return
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto'
    r = requests.post(url, data={'chat_id': CHAT_ID, 'caption': text},
                      files={'photo': ('report.png', plot_buf.getvalue())}, timeout=60)
    if not r.ok:  
        raise RuntimeError(f'Telegram вернул {r.status_code}: {r.text}')
    print('Отчёт отправлен')
    
from airflow.decorators import dag, task
 
default_args = {
    'owner': OWNER,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 10, 1),
}
schedule_interval = '0 11 * * *'  
 
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_feed_report_radvilovich():  
 
    @task()
    def extract():
        return get_data()
 
    @task()
    def report(df):
        text = build_text(df)
        buf = build_plot(df)
        send_report(text, buf)
 
    report(extract())
 
 
dag_feed_report_radvilovich = dag_feed_report_radvilovich()