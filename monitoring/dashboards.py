import sys
import os

# Добавляем корневую директорию проекта в PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dash
from dash import dcc, html
import pandas as pd
import plotly.express as px
from database.db_connection import DatabaseConnection  # Абсолютный импорт
from config import load_db_config  # Абсолютный импорт

# Загрузка конфигурации
config = load_db_config()

# Подключение к базе данных
db_conn = DatabaseConnection(config['monitoring_db'])


# Функция для извлечения данных для первого графика
def fetch_data_first_graph():
    query = """
    SELECT created_at, record_count, check_description
    FROM data_check_results 
    WHERE check_description NOT IN ('Количество ИЛС открытых', 'Количество ИЛС умерших');
    """
    with db_conn:
        with db_conn.get_cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(data, columns=columns)


# Функция для извлечения данных для второго графика
def fetch_data_second_graph():
    query = """
    SELECT created_at, record_count, check_description
    FROM data_check_results 
    WHERE check_description IN ('Количество ИЛС открытых', 'Количество ИЛС умерших');
    """
    with db_conn:
        with db_conn.get_cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(data, columns=columns)


# Получение данных для первого графика
df_first = fetch_data_first_graph()

# Преобразование столбца created_at в формат datetime
df_first['created_at'] = pd.to_datetime(df_first['created_at'])

# Преобразование столбца created_at в формат даты (без времени)
df_first['date'] = df_first['created_at'].dt.date

# Преобразование данных в формат, удобный для первого графика
pivot_df_first = df_first.pivot_table(index='date', columns='check_description', values='record_count',
                                      aggfunc='sum').reset_index()

# Получение данных для второго графика
df_second = fetch_data_second_graph()

# Преобразование столбца created_at в формат datetime
df_second['created_at'] = pd.to_datetime(df_second['created_at'])

# Преобразование столбца created_at в формат даты (без времени)
df_second['date'] = df_second['created_at'].dt.date

# Преобразование данных в формат, удобный для второго графика
pivot_df_second = df_second.pivot_table(index='date', columns='check_description', values='record_count',
                                        aggfunc='sum').reset_index()

# Создание Dash приложения
app = dash.Dash(__name__)

# Определение макета приложения
app.layout = html.Div([
    html.H1("Дашборд проверок данных"),
    dcc.Graph(
        id='data-check-results-second',
        figure=px.line(pivot_df_second, x='date', y=pivot_df_second.columns[1:],
                       title='Количество записей по датам для проверок ИЛС')
    ),
    dcc.Graph(
        id='data-check-results-first',
        figure=px.line(pivot_df_first, x='date', y=pivot_df_first.columns[1:],
                       title='Количество записей по датам для каждой проверки (исключая ИЛС)')
    ),

    dcc.Interval(
        id='interval-component',
        interval=60 * 1000,  # Обновление каждые 60 секунд
        n_intervals=0
    )
])

# Запуск приложения
if __name__ == '__main__':
    app.run_server(debug=True)
