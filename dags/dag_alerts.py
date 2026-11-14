# coding=utf-8
"""
Система алертов для приложения (лента + мессенджер): проверка каждые 15 минут.
 
Метрики: активные пользователи ленты, просмотры, лайки, CTR,
         активные пользователи мессенджера, отправленные сообщения.
Срезы:   всё приложение, os, source.
 
Метод (статистический, межквартильный размах):
  для проверяемой 15-минутки берём значения той же 15-минутки (и двух соседних) за предыдущие 14 дней,
  считаем q25, q75, IQR и допустимый диапазон  [q25 - a*IQR ; q75 + a*IQR]  (a = 3).
  Интервал строится именно для этого времени суток, а не «в среднем за день».
  Чтобы не ловить мелкий шум, ширина запаса не меньше 10% от медианы.
  Выход за диапазон -> алерт: метрика, срез, значение, отклонение от нормы и от значения «день назад», график.
 
Граф DAG:  extract -> detect_and_send   (запуск каждые 15 минут)
"""
import io
from datetime import datetime, timedelta
 
import numpy as np
import pandas as pd
import pandahouse as ph
import requests
from matplotlib.dates import DateFormatter
from matplotlib.figure import Figure

TELEGRAM_TOKEN = '9287289855:AAEB6ZuHzvmCVgtX6Sgjbqd-Hyv96-DxCdI'  
ALERT_CHAT_ID = -969316925               
SEND_TO_TELEGRAM = True                  
DASHBOARD_URL = ''                       
 
OWNER = 'darina-radvilovich-fhg4877'
SRC_DB = 'simulator_20260920'
connection = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'database': SRC_DB,
    'user': 'student',
    'password': 'dpo_python_2020',
}
 
HISTORY_DAYS = 14        
IQR_A = 3              
MIN_REL = 0.10          
MIN_POINTS = 20          
MIN_COUNT = 20         
MIN_VIEWS_FOR_CTR = 200  
MAX_ALERTS_PER_RUN = 5   
SLICES = ['all', 'os', 'source']
 
SLOT = pd.Timedelta(minutes=15)
FMT = '%Y-%m-%d %H:%M:%S'
METRICS = [
    ('feed_users', 'Активные пользователи ленты', 'int'),
    ('views', 'Просмотры', 'int'),
    ('likes', 'Лайки', 'int'),
    ('ctr', 'CTR', 'ctr'),
    ('msg_users', 'Активные пользователи мессенджера', 'int'),
    ('messages', 'Отправленные сообщения', 'int'),
]
COUNT_COLS = ['feed_users', 'views', 'likes', 'msg_users', 'messages']
 
def get_data(history_days=HISTORY_DAYS):
    """15-минутные метрики за history_days+1 суток по всем срезам (только завершённые 15-минутки)."""
    now_ts = pd.Timestamp(ph.read_clickhouse('SELECT toStartOfFifteenMinutes(now()) AS t',
                                             connection=connection)['t'].iloc[0])
    start = now_ts - pd.Timedelta(days=history_days + 1)
    cond = "time >= toDateTime('{}') AND time < toDateTime('{}')".format(start.strftime(FMT),
                                                                           now_ts.strftime(FMT))
    frames = []
    for dim in SLICES:
        sel, grp = ('', '') if dim == 'all' else (', {} AS slice_value'.format(dim), ', slice_value')
        q_feed = """
            SELECT toStartOfFifteenMinutes(time) AS ts {sel},
                   uniqExact(user_id) AS feed_users,
                   countIf(action = 'view') AS views,
                   countIf(action = 'like') AS likes
            FROM {db}.feed_actions
            WHERE {cond}
            GROUP BY ts {grp}
            ORDER BY ts
        """.format(sel=sel, grp=grp, db=SRC_DB, cond=cond)
        q_msg = """
            SELECT toStartOfFifteenMinutes(time) AS ts {sel},
                   uniqExact(user_id) AS msg_users,
                   count() AS messages
            FROM {db}.message_actions
            WHERE {cond}
            GROUP BY ts {grp}
            ORDER BY ts
        """.format(sel=sel, grp=grp, db=SRC_DB, cond=cond)
        f = ph.read_clickhouse(q_feed, connection=connection)
        m = ph.read_clickhouse(q_msg, connection=connection)
        keys = ['ts'] if dim == 'all' else ['ts', 'slice_value']
        d = f.merge(m, on=keys, how='outer')
        if dim == 'all':
            d['slice_value'] = 'all'
        d['slice_name'] = dim
        frames.append(d)
    df = pd.concat(frames, ignore_index=True)
    df['ts'] = pd.to_datetime(df['ts'])
    df['last_ts'] = now_ts - SLOT   
    return df.fillna(0)
 
def build_series(df):
    """{(срез, значение): {метрика: pd.Series на сетке 15-минуток}}; пропуски = 0 (падение до нуля — тоже аномалия)."""
    last_ts = pd.Timestamp(df['last_ts'].iloc[0])
    grid = pd.date_range(df['ts'].min(), last_ts, freq='15min')
    out = {}
    for (sn, sv), g in df.groupby(['slice_name', 'slice_value']):
        g = g.drop_duplicates('ts').set_index('ts').reindex(grid)
        for c in COUNT_COLS:
            g[c] = g[c].fillna(0)
        series = {c: g[c] for c in COUNT_COLS}
        series['ctr'] = g['likes'] / g['views'].replace(0, np.nan)
        out[(sn, sv)] = series
    return out

def baseline_values(series, ts, days=HISTORY_DAYS):
    """Значения метрики в ту же 15-минутку (и две соседние) за предыдущие дни."""
    times = [ts - pd.Timedelta(days=d) + k * SLOT for d in range(1, days + 1) for k in (-1, 0, 1)]
    vals = series.reindex(times).dropna().values
    return vals
 
def get_band(series, ts):
    """Допустимый диапазон по правилу межквартильного размаха для этой 15-минутки."""
    vals = baseline_values(series, ts)
    if len(vals) < MIN_POINTS:
        return None
    q25, q75 = np.percentile(vals, [25, 75])
    med = float(np.median(vals))
    half = max(IQR_A * (q75 - q25), MIN_REL * abs(med))
    return {'median': med, 'lower': q25 - half, 'upper': q75 + half}
 
def check_point(series, ts, volume=None, min_volume=MIN_COUNT):
    """Проверка одной 15-минутки. Возвращает dict с деталями аномалии или None."""
    cur = series.get(ts)
    if cur is None or pd.isna(cur):
        return None
    band = get_band(series, ts)
    if band is None or band['median'] == 0:
        return None
    vol_series = series if volume is None else volume
    vol_band = band if volume is None else get_band(vol_series, ts)
    if vol_band is None or vol_band['median'] < min_volume:
        return None
    if band['lower'] <= cur <= band['upper']:
        return None
    prev = series.get(ts - pd.Timedelta(days=1))
    return {
        'ts': ts, 'current': float(cur), 'median': band['median'],
        'lower': band['lower'], 'upper': band['upper'],
        'dev_norm': (cur / band['median'] - 1) * 100,
        'prev': None if prev is None or pd.isna(prev) or prev == 0 else float(prev),
        'dev_prev': None if prev is None or pd.isna(prev) or prev == 0 else (cur / prev - 1) * 100,
    } 
 
def find_anomalies(all_series, ts):
    """Проверяем все метрики и срезы для 15-минутки ts."""
    found = []
    for (sn, sv), series in all_series.items():
        for col, label, kind in METRICS:
            if col == 'ctr':
                r = check_point(series['ctr'], ts, volume=series['views'], min_volume=MIN_VIEWS_FOR_CTR)
            else:
                r = check_point(series[col], ts)
            if r is not None:
                r.update({'col': col, 'label': label, 'kind': kind, 'slice_name': sn, 'slice_value': sv})
                found.append(r)
    found.sort(key=lambda r: -abs(r['dev_norm']))
    return found
 
def backtest(df, hours=48):
    """Какие алерты сработали бы за последние hours часов — для подбора параметров (IQR_A, MIN_REL)."""
    all_series = build_series(df)
    last_ts = pd.Timestamp(df['last_ts'].iloc[0])
    rows = []
    for ts in pd.date_range(end=last_ts, periods=hours * 4, freq='15min'):
        for r in find_anomalies(all_series, ts):
            rows.append({'ts': ts, 'metric': r['label'], 'slice': '{}={}'.format(r['slice_name'], r['slice_value']),
                         'current': r['current'], 'dev_norm_pct': round(r['dev_norm'], 1)})
    return pd.DataFrame(rows)
 

def _fmt(v, kind):
    return '{:.3f}'.format(v) if kind == 'ctr' else '{:,.0f}'.format(v).replace(',', ' ')
 
def build_text(r):
    group = 'всё приложение' if r['slice_name'] == 'all' else '{} = {}'.format(r['slice_name'], r['slice_value'])
    t0, t1 = r['ts'], r['ts'] + SLOT
    direction = 'выше нормы' if r['current'] > r['upper'] else 'ниже нормы'
    lines = [
        '🚨 Метрика «{}» в срезе: {}.'.format(r['label'], group),
        'Текущее значение: {} (за {:%d.%m %H:%M}–{:%H:%M}).'.format(_fmt(r['current'], r['kind']), t0, t1),
        'Отклонение от нормы для этого времени: {:+.1f}% ({}). Норма ≈ {}, допустимо {} – {}.'.format(
            r['dev_norm'], direction, _fmt(r['median'], r['kind']),
            _fmt(max(r['lower'], 0), r['kind']), _fmt(r['upper'], r['kind'])),
    ]
    if r['dev_prev'] is not None:
        lines.append('К значению день назад: {:+.1f}% (было {}).'.format(r['dev_prev'], _fmt(r['prev'], r['kind'])))
    if DASHBOARD_URL:
        lines.append('Дашборд: {}'.format(DASHBOARD_URL))
    return '\n'.join(lines)
 
def build_plot(series, r):
    """Последние 24 часа: метрика сейчас, день назад и допустимый диапазон."""
    ts = r['ts']
    idx = pd.date_range(end=ts, periods=96, freq='15min')
    cur = series.reindex(idx)
    prev = series.reindex(idx - pd.Timedelta(days=1))
    lows, ups = [], []
    for t in idx:
        b = get_band(series, t)
        lows.append(max(b['lower'], 0) if b else np.nan)
        ups.append(b['upper'] if b else np.nan)
    x = [t.to_pydatetime() for t in idx]
 
    fig = Figure(figsize=(12, 5), dpi=100)
    ax = fig.subplots()
    ax.fill_between(x, lows, ups, alpha=0.2, color='tab:green', label='норма (диапазон по IQR)')
    ax.plot(x, prev.values, linestyle='--', color='gray', label='день назад')
    ax.plot(x, cur.values, linewidth=2, color='tab:blue', label='сейчас')
    ax.scatter([x[-1]], [r['current']], color='red', zorder=5, s=60, label='аномалия')
    ax.set_title('{}: {}={} (последние 24 часа)'.format(r['label'], r['slice_name'], r['slice_value']))
    ax.xaxis.set_major_formatter(DateFormatter('%d.%m %H:%M'))
    ax.grid(True, alpha=0.3)
    ax.legend(loc='upper left')
    fig.autofmt_xdate()
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    return buf.getvalue()

def _tg(method, data=None, files=None):
    url = 'https://api.telegram.org/bot{}/{}'.format(TELEGRAM_TOKEN, method)
    resp = requests.post(url, data=data, files=files, timeout=60)
    if not resp.ok: 
        raise RuntimeError('Telegram {}: {} {}'.format(method, resp.status_code, resp.text))

def send_alerts(found, all_series):
    shown, rest = found[:MAX_ALERTS_PER_RUN], found[MAX_ALERTS_PER_RUN:]
    for r in shown:
        text = build_text(r)
        png = build_plot(all_series[(r['slice_name'], r['slice_value'])][r['col']], r)
        if SEND_TO_TELEGRAM:
            _tg('sendPhoto', data={'chat_id': ALERT_CHAT_ID, 'caption': text},
                files={'photo': ('alert.png', png)})
        else:
            print(text)
            print('---')
    if rest:
        extra = 'Ещё аномалии в этой проверке ({}):\n'.format(len(rest)) + '\n'.join(
            '• {} ({}={}): {:+.1f}%'.format(r['label'], r['slice_name'], r['slice_value'], r['dev_norm'])
            for r in rest)
        if SEND_TO_TELEGRAM:
            _tg('sendMessage', data={'chat_id': ALERT_CHAT_ID, 'text': extra})
        else:
            print(extra)

def run_alerts(df):
    all_series = build_series(df)
    ts = pd.Timestamp(df['last_ts'].iloc[0])
    found = find_anomalies(all_series, ts)
    if not found:
        print('Проверка {:%d.%m %H:%M}: аномалий нет'.format(ts))
        return
    print('Проверка {:%d.%m %H:%M}: найдено аномалий — {}'.format(ts, len(found)))
    send_alerts(found, all_series)

from airflow.decorators import dag, task
 
default_args = {
    'owner': OWNER,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2026, 10, 1),
}
schedule_interval = '*/15 * * * *'  
 
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, max_active_runs=1)
def dag_alerts_radvilovich():  
 
    @task()
    def extract():
        return get_data()
 
    @task()
    def detect_and_send(df):
        run_alerts(df)
 
    detect_and_send(extract())
 
 
dag_alerts_radvilovich = dag_alerts_radvilovich()