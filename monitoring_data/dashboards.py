import dash
from dash import dcc, html, Input, Output
import pandas as pd
import plotly.graph_objects as go
from database.db_connection import DatabaseConnection
from config import load_db_config

# Загрузка конфигурации
config = load_db_config()

# Подключение к базе данных
db_conn = DatabaseConnection(config['monitoring_db'])

# IP-адреса из config.ini
ip_address = config['ip_for_dashboard']['ip_address']


# Функция для извлечения данных для первого графика
def fetch_data_first_graph():
    query = """
    SELECT created_at, record_count, check_description
    FROM data_check_results 
    WHERE check_description NOT IN ('Количество ИЛС открытых', 'Количество ИЛС умерших', 'Количество ИЛС открытых в исторической системе', 'Количество ИЛС умерших в исторической системе');
    """
    with db_conn:
        with db_conn.get_cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(data, columns=columns)


# Функция для извлечения данных для карточек
def fetch_data_cards():
    query = """
    SELECT created_at, record_count, check_description
    FROM data_check_results 
    WHERE check_description IS NOT NULL;  -- Условие для получения всех записей с ненулевым значением
    """
    with db_conn:
        with db_conn.get_cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(data, columns=columns)


# Функция для форматирования значений с разбиением на разряды
def format_number(value):
    return f"{value:,}".replace(',', ' ')


# Создание Dash приложения
app = dash.Dash(__name__)

# Определение макета приложения
app.layout = html.Div([
    html.H1("Дашборд проверок данных по лицевым счетам", style={
        'fontSize': '36px',  # Размер шрифта
        'color': '#333',  # Цвет текста
        'textAlign': 'center',  # Выравнивание по центру
        'marginBottom': '20px',  # Отступ снизу
        'textShadow': '2px 2px 4px rgba(0, 0, 0, 0.2)',  # Тень текста
    }),

    # Блок для карточек верхнего уровня с градиентом
    html.Div(id='card-container-top', style={
        'display': 'flex',
        'flex-wrap': 'wrap',
        'gap': '20px',
        'margin-bottom': '20px',
        'justify-content': 'center',
        'background': 'linear-gradient(to right, #4facfe, #00f2fe)',  # Градиент
        'padding': '20px',
        'borderRadius': '10px'
    }),

    # Блок для карточек нижнего уровня с другим стилем
    html.Div(id='card-container-bottom', style={
        'display': 'flex',
        'flex-wrap': 'wrap',
        'gap': '20px',
        'margin-bottom': '20px',
        'justify-content': 'center',
        'background': '#f9f9f9',  # Светлый фон
        'padding': '20px',
        'borderRadius': '10px',
        'boxShadow': '0 4px 8px rgba(0, 0, 0, 0.1)',  # Тень блока
    }),

    dcc.Interval(
        id='interval-component',
        interval=60 * 60 * 1000,  # Обновление каждые 60 минут
        n_intervals=0
    ),

    # Блок для графиков первого типа
    dcc.Graph(id='data-check-results-first'),
])


# Обработчик обратного вызова для обновления графиков
@app.callback(
    [Output('data-check-results-first', 'figure'),
     Output('card-container-top', 'children'),
     Output('card-container-bottom', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_graphs(n):
    # Получение данных для первого графика
    df_first = fetch_data_first_graph()
    df_first['created_at'] = pd.to_datetime(df_first['created_at'])
    df_first['date'] = df_first['created_at'].dt.date
    pivot_df_first = df_first.pivot_table(index='date', columns='check_description', values='record_count',
                                          aggfunc='sum').reset_index()

    # Создание графика для первого типа проверок
    figure_first = go.Figure()

    # Добавление линий для каждой проверки
    for column in pivot_df_first.columns[1:]:
        figure_first.add_trace(go.Scatter(
            x=pivot_df_first['date'],
            y=pivot_df_first[column],
            mode='lines+markers',
            name=column,
            line=dict(width=2),  # Установка ширины линии
            marker=dict(size=6),  # Установка размера маркеров
        ))

    # Настройка заголовка и подписей осей
    figure_first.update_layout(
        title='Динамика по каждой из проверок',
        xaxis_title='Дата',
        yaxis_title='Количество записей',
        legend_title='Тип проверки',
        template='plotly_white',  # Использование светлой темы
        hovermode='x unified',  # Объединение всплывающих подсказок
    )

    # Настройка сетки
    figure_first.update_xaxes(showgrid=True, gridcolor='LightGray')
    figure_first.update_yaxes(showgrid=True, gridcolor='LightGray')

    # Получение данных для карточек
    df_cards = fetch_data_cards()
    df_cards['created_at'] = pd.to_datetime(df_cards['created_at'])
    latest_values = df_cards.loc[df_cards.groupby('check_description')['created_at'].idxmax()]

    # Создание карточек для верхнего блока
    card_elements_top = []
    top_checks = ['Количество ИЛС открытых', 'Количество ИЛС открытых в исторической системе',
                  'Количество ИЛС умерших', 'Количество ИЛС умерших в исторической системе']

    for check in top_checks:
        row = latest_values[latest_values['check_description'] == check].iloc[0]
        card = html.Div([
            html.Div(check, style={'flex': '1', 'textAlign': 'left', 'color': '#fff'}),  # Название слева
            html.Div(f"{format_number(row['record_count'])}",
                     style={'flex': '1', 'textAlign': 'right', 'color': '#fff'})  # Значение справа
        ], style={
            'display': 'flex',  # Использование flexbox
            'alignItems': 'center',  # Вертикальное выравнивание по центру
            'border': 'none',
            'borderRadius': '5px',
            'padding': '10px',
            'width': '200px',
            'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
            'background': 'rgba(255, 255, 255, 0.2)',  # Полупрозрачный белый фон
        })
        card_elements_top.append(card)

    # Создание карточек для нижнего блока
    card_elements_bottom = []
    bottom_checks = latest_values[~latest_values['check_description'].isin(top_checks)]

    for check in bottom_checks['check_description']:
        row = latest_values[latest_values['check_description'] == check].iloc[0]
        card = html.Div([
            html.Div(check, style={'flex': '1', 'textAlign': 'left', 'padding': '10px'}),  # Название слева
            html.Div(f"{format_number(row['record_count'])}", style={
                'flex': '1',
                'textAlign': 'right',
                'padding': '10px',
                'background': '#e0f7fa',  # Подсвеченный фон для значения
                'borderLeft': '2px solid #00796b',  # Граница слева
                'borderRadius': '5px',  # Закругление углов
            })  # Значение справа
        ], style={
            'display': 'flex',  # Использование flexbox
            'alignItems': 'center',  # Вертикальное выравнивание по центру
            'border': '1px solid #ccc',
            'borderRadius': '5px',
            'padding': '10px',
            'width': '200px',
            'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
            'background': '#fff',  # Белый фон
        })
        card_elements_bottom.append(card)

    return figure_first, card_elements_top, card_elements_bottom  # Возвращаем график и карточки


# Запуск приложения
if __name__ == '__main__':
    app.run_server(debug=False, host=ip_address)
