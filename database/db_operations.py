import logging
from database.db_schema import create_table_if_not_exists, drop_table_if_exists, create_netezza_table

class DBOperations:
    def __init__(self, connection):
        self.connection = connection

    def execute_query(self, query):
        with self.connection.get_cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def create_table(self, table_name, columns, index_columns=None):
        """
        Создание таблицы, если она не существует.
        :param table_name: Имя создаваемой таблицы.
        :param columns: Словарь с именами столбцов и их типами данных.
        :param index_columns: Список столбцов для создания индекса (по умолчанию None).
        """
        with self.connection.get_cursor() as cur:
            create_table_if_not_exists(cur, table_name, columns, index_columns)
            self.connection.conn.commit()

    def drop_table(self, table_name):
        """
        Удаление таблицы, если она существует.
        :param table_name: Имя таблицы, которую нужно удалить.
        """
        with self.connection.get_cursor() as cur:
            drop_table_if_not_exists(cur, table_name)
            self.connection.conn.commit()

    def clear_table(self, table_name):
        """
        Очистка таблицы.
        :param table_name: Имя таблицы, которую нужно очистить.
        """
        with self.connection.get_cursor() as cur:
            cur.execute(f"TRUNCATE {table_name};")
            self.connection.conn.commit()

    def create_netezza_table(self, table_name, columns, distribute_column):
        """
        Создание таблицы в Netezza.
        :param table_name: Имя создаваемой таблицы.
        :param columns: Словарь с именами столбцов и их типами данных.
        :param distribute_column: Имя столбца для распределения.
        """
        with self.connection.get_cursor() as cur:
            create_netezza_table(cur, table_name, columns, distribute_column)
            self.connection.conn.commit()

    def load_data_to_netezza_from_select(self, table_name, select_query, distribute_column, directory, output_filename):
        """
        Загрузка данных в Netezza из запроса SELECT.
        :param table_name: Имя таблицы, в которую будут загружены данные.
        :param select_query: SQL-запрос для выборки данных.
        :param distribute_column: Имя столбца для распределения.
        :param directory: Директория для внешнего файла.
        :param output_filename: Имя выходного файла.
        """
        with self.connection.get_cursor() as cur:
            insert_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM 
            EXTERNAL '{directory}/{output_filename}'
            USING
            (
                Y2BASE 2000
                ENCODING 'internal'
                REMOTESOURCE 'ODBC'
                ESCAPECHAR '\\'
            );
            """
            cur.execute(insert_query)
            self.connection.conn.commit()
            logging.info(f"Данные успешно загружены в таблицу {table_name} из внешнего файла {output_filename}.")

    def count_total_records(self, table_name):
        """Подсчет общего количества записей в таблице."""
        query = f"SELECT COUNT(*) FROM {table_name};"
        with self.connection.get_cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]

    def count_records(self, query):
        """Подсчет количества записей по заданному запросу."""
        with self.connection.get_cursor() as cur:
            cur.execute(query)
            return cur.rowcount
