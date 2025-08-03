import logging
from database.db_schema import create_netezza_table


def load_csv_to_table(cur, csv_file, table_name):
    """Загрузка данных из CSV файла в таблицу."""
    with open(csv_file, 'r') as f:
        next(f)  # Пропустить заголовок
        cur.copy_expert(f"COPY public.{table_name} FROM STDIN WITH CSV DELIMITER ';'", f)
    logging.info(f"Данные из {csv_file} загружены в таблицу {table_name}.")


def load_data_from_csv(cur, csv_file, table_name):
    """
    Загрузка данных из CSV-файла в таблицу базы данных.
    :param cur: Курсор для выполнения SQL-запросов.
    :param csv_file: Путь к CSV-файлу.
    :param table_name: Имя таблицы, в которую будут загружены данные.
    """
    load_csv_to_table(cur, csv_file, table_name)


def load_data_to_netezza_from_select(conn, table_name, select_query, distribute_column, directory, output_filename):
    """
    Загрузка данных в Netezza из запроса SELECT с использованием внешнего файла.
    :param conn: Соединение с базой данных Netezza.
    :param table_name: Имя таблицы, в которую будут загружены данные.
    :param select_query: SQL-запрос для выборки данных.
    :param distribute_column: Имя столбца для распределения.
    :param directory: Директория для внешнего файла.
    :param output_filename: Имя выходного файла.
    """
    try:
        with conn.cursor() as cur:
            # Создание таблицы в Netezza
            create_netezza_table(cur, table_name, distribute_column)

            # Формирование запроса для вставки данных из внешнего файла
            insert_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM 
            EXTERNAL '{directory}/{output_filename}'
            USING
            (
                Y2BASE 2000
                ENCODING 'internal'
                REMOTESOURCE 'ODBC'
                ESCAPECHAR '\'
            );
            """

            # Выполнение SQL-запроса
            cur.execute(insert_query)
            conn.commit()
            logging.info(f"Данные успешно загружены в таблицу {table_name} из внешнего файла {output_filename}.")
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных в Netezza: {e}")

